import boto3
import csv
import datetime
from io import StringIO

def lambda_handler(event, context):
    # Initialize AWS clients
    s3 = boto3.client('s3')
    ses = boto3.client('ses')

    # Configuration
    bucket_name = "key-expiry-osds"
    csv_key = "keys.csv"
    sender_email = "x"

    try:
        response = s3.get_object(Bucket=bucket_name, Key=csv_key)
        csv_content = response['Body'].read().decode('utf-8-sig')

        # Read CSV using built-in csv module
        csv_reader = csv.DictReader(StringIO(csv_content))
        fieldnames = csv_reader.fieldnames
        print(f"CSV columns found: {fieldnames}")

        today = datetime.date.today()
        print(f"Current date: {today.strftime('%d-%m-%Y')}")

        processed = 0
        recipient_keys = {}

        # Process each key
        for row in csv_reader:
            print(f"Processing row: {row}")
            try:
                row = {k.strip(): v for k, v in row.items()}
                key_name = row.get('GPG_Private_Key') or row.get('GPG_private_key') or 'unknown'
                feed_name = row.get('Feed_Name') or row.get('feed_name') or 'unknown'
                expiry_date_str = (
                    row.get('GPG_Key_Expiry') or
                    row.get('gpg_key_expiry') or
                    row.get('expiry_date')
                )

                if not expiry_date_str or expiry_date_str.strip().upper() == 'N/A':
                    print(f"No valid expiry date found for key {key_name}. Skipping row.")
                    continue

                expiry_date = datetime.datetime.strptime(expiry_date_str, "%d-%m-%Y").date()
                days_until_expiry = (expiry_date - today).days

                print(f"Key: {key_name}, Expiry: {expiry_date}, Days left: {days_until_expiry}")

                if 0 <= days_until_expiry <= 30:
                    email_column = row.get('PIC_Email') or row.get('PIC_email', '')
                    email = email_column.strip()
                    if email:
                        for recipient in email.split(','):
                            recipient = recipient.strip()
                            if recipient:
                                recipient_keys.setdefault(recipient, []).append({
                                    "feed_name": feed_name,
                                    "key_name": key_name,
                                    "expiry_date": expiry_date_str,
                                    "days_until_expiry": days_until_expiry
                                })

                processed += 1
            except Exception as inner_error:
                print(f"Error processing key {row.get('GPG_Private_Key') or 'unknown'}: {inner_error}")

        # Send emails
        try:
            notifications_sent = 0
            for recipient, keys in recipient_keys.items():
                print(f"Sending notification to {recipient} for {len(keys)} keys")
                try:
                    notifications_sent += send_consolidated_email(ses, recipient, keys, sender_email)
                except Exception as email_error:
                    print(f"Error sending email to {recipient}: {email_error}")

            print(f"Summary: Processed {processed} keys, sent {notifications_sent} notifications.")

        except Exception as outer_error:
            print(f"Error in sending emails: {outer_error}")

    except Exception as e:
        print(f"Fatal error: {e}")


def send_consolidated_email(ses, recipient, key_data, sender_email):
    subject = "GPG Key Expiration ALERT-Action Required"

    table_rows_html = ""
    for index, key in enumerate(key_data, start=1):
        table_rows_html += f"""
        <tr>
            <td>{index}</td>
            <td>{key['feed_name']}</td>
            <td>{key['key_name']}</td>
            <td>{key['expiry_date']}</td>
            <td>{key['days_until_expiry']}</td>
        </tr>
        """

    html_body = f"""
    <html>
    <head>
        <style>
            table {{ border-collapse: collapse; width: 100%; }}
            th, td {{ border: 1px solid #ddd; padding: 8px; }}
            th {{ background-color: #f2f2f2; }}
            .footer {{ font-size: 12px; color: #666; margin-top: 20px; }}
        </style>
    </head>
    <body>
        <h1>Key Expiration Alert</h1>
        <p>The following keys are approaching expiration:</p>
        <table>
            <thead>
                <tr>
                    <th>S.No</th>
                    <th>Feed Name</th>
                    <th>GPG Private Key</th>
                    <th>Expiry Date</th>
                    <th>Days Remaining</th>
                </tr>
            </thead>
            <tbody>
                {table_rows_html}
            </tbody>
        </table>
        <br>
        <br>
        <div class="action">
            <strong>Action Required</strong><br>
            Please update these keys before expiration.
        </div>
       
    </body>
    </html>
    """

    text_body = "GPG KEY EXPIRATION ALERT - ACTION REQUIRED\n\n"
    for index, key in enumerate(key_data, start=1):
        text_body += f"{index}. {key['feed_name']} | {key['key_name']} | {key['expiry_date']} | {key['days_until_expiry']} days\n"

    text_body += "\nPlease update these keys before expiration.\nThis is an automated notification sent 30 days before key expiration."

    max_retries = 3
    for attempt in range(max_retries):
        try:
            ses.send_email(
                Source=sender_email,
                Destination={"ToAddresses": [recipient]},
                Message={
                    "Subject": {"Data": subject},
                    "Body": {
                        "Html": {"Data": html_body},
                        "Text": {"Data": text_body}
                    }
                }
            )
            print(f"Email successfully sent to {recipient}")
            return 1
        except Exception as e:
            print(f"Attempt {attempt + 1} failed for {recipient}: {e}")

    print(f"All {max_retries} attempts failed for {recipient}")
    return 0
