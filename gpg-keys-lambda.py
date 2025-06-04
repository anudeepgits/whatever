import boto3
import csv
import datetime
from io import StringIO
import time
import json

def lambda_handler(event, context):
    # Initialize AWS clients
    s3 = boto3.client('s3')
    ses = boto3.client('ses')
    
    # Configuration
    bucket_name = 'key-expiry-osds'
    csv_key = 'keys.csv'
    sender_email = 'avarikoti@gmail.com'
    
    try:
        # Get the CSV file from S3
        response = s3.get_object(Bucket=bucket_name, Key=csv_key)
        csv_content = response['Body'].read().decode('utf-8-sig')

        
        # Parse CSV using built-in csv module
        csv_reader = csv.DictReader(StringIO(csv_content))
        
        # Debug: Print column names
        fieldnames = csv_reader.fieldnames
        print(f"CSV columns found: {fieldnames}")
        
        # Get current date
        today = datetime.datetime.now()
        print(f"Current date: {today.strftime('%d-%m-%Y')}")
        
        # Track statistics
        processed = 0
        notified = 0
        errors = 0
        
        # Process each key in the CSV
        for row in csv_reader:
            processed += 1
            print(f"Processing row: {row}")
            try:
                # Handle different possible column names
                key_name = row.get('GPG_Private_Key') or row.get('GPG_private_key') or 'unknown'
                expiry_date_str = row.get('GPG_Key_Expiry') or row.get('GPG_key_expiry') or row.get('expiry_date')
                
                if not expiry_date_str:
                    print(f"No expiry date found for key {key_name}")
                    continue
                
                # Parse expiry date using dd-mm-yyyy format
                expiry_date = datetime.datetime.strptime(expiry_date_str, '%d-%m-%Y')
                days_until_expiry = (expiry_date - today).days
                
                print(f"Key: {key_name}, Expiry: {expiry_date_str}, Days until expiry: {days_until_expiry}")
                
                # If key expires in exactly 14 days, send notification
                # For testing: also check for keys expiring in 1-3 days
                if days_until_expiry == 14 or (1 <= days_until_expiry <= 3):
                    # Get recipients (comma-separated), stripping whitespace
                    # Handle both PIC_email and PIC_Email column names
                    email_column = row.get('PIC_Email') or row.get('PIC_email', '')
                    recipients = [email.strip() for email in email_column.split(',')]
                    
                    # Send notification via SES
                    send_formatted_email(
                        ses=ses,
                        sender=sender_email,
                        recipients=recipients,
                        key_name=key_name,
                        expiry_date=expiry_date_str,
                        days_remaining=days_until_expiry
                    )
                    
                    notified += 1
                    print(f"Notification sent for key {key_name} to {recipients}")
                    
            except Exception as inner_error:
                errors += 1
                error_msg = f"Error processing key {row.get('GPG_Private_Key', row.get('GPG_private_key', 'unknown'))}: {str(inner_error)}"
                print(error_msg)
        
        print(f"Summary: Processed {processed} keys, sent {notified} notifications, encountered {errors} errors")
        return {
            'statusCode': 200,
            'body': json.dumps(f"Processed {processed} keys, sent {notified} notifications")
        }
        
    except Exception as outer_error:
        error_msg = f"Error in key expiration monitoring: {str(outer_error)}"
        print(error_msg)
        return {
            'statusCode': 500,
            'body': json.dumps(f"Error: {str(outer_error)}")
        }

def send_formatted_email(ses, sender, recipients, key_name, expiry_date, days_remaining):
    """Formatted HTML email notification about key expiration"""
    subject = f"Key Expiration Alert: {key_name} - Action Required"
    
    # HTML email body
    html_body = f"""
    <html>
    <head>
        <style>
            body {{ font-family: Arial, sans-serif; line-height: 1.6; color: #333; }}
            .container {{ max-width: 600px; margin: 0 auto; padding: 20px; }}
            .header {{ background-color: #f8d7da; color: #721c24; padding: 15px; text-align: center; border-radius: 5px; }}
            .content {{ background-color: #f8f9fa; padding: 20px; border-radius: 5px; margin: 15px 0; }}
            .key-details {{ background-color: #f8f9fa; padding: 15px; border-left: 4px solid #ccc; margin: 15px 0; }}
            .action {{ background-color: #d1ecf1; color: #0c5460; padding: 15px; border-radius: 5px; }}
            .footer {{ font-size: 12px; color: #6c757d; margin-top: 30px; text-align: center; }}
        </style>
    </head>
    <body>
        <div class="container">
            <div class="header">
                <h1>Key Expiration Alert !!</h1>
            </div>
            
            <div class="content">
                <p>This is an automated notification that the following key is approaching expiration:</p>
                <div class="key-details">
                    <strong>Key Name:</strong> {key_name}<br>
                    <strong>Expiration Date:</strong> DD-MM-YYYY:{expiry_date}<br>
                    <strong>Days Remaining:</strong> {days_remaining}
                </div>
            </div>
            
            <div class="action">
                <strong>Action Required</strong><br>
                Update the key before expiration.
            </div>
            
            <div class="footer">
                This is an automated notification sent 14 days before key expiration.
            </div>
        </div>
    </body>
    </html>
    """
    
    # Plain text alternative
    text_body = f"""
    KEY EXPIRATION ALERT        ACTION REQUIRED
    
    Key Name: {key_name}
    Expiration Date: {expiry_date}
    Days Remaining: {days_remaining}
    
    Please take immediate action to update/rotate this key before expiration.
    
    This is an automated notification sent 14 days before key expiration.
    """
    
    # Retry logic (up to 3 attempts)
    max_retries = 3
    for attempt in range(max_retries):
        try:
            response = ses.send_email(
                Source=sender,
                Destination={'ToAddresses': recipients},
                Message={
                    'Subject': {'Data': subject},
                    'Body': {
                        'Text': {'Data': text_body},
                        'Html': {'Data': html_body}
                    }
                }
            )
            print(f"Successfully sent notification for key {key_name} to {recipients}")
            return response
            
        except Exception as e:
            if attempt < max_retries-1:
                print(f"Attempt {attempt+1} failed for key {key_name}. Retrying...")
                time.sleep(1)
            else:
                print(f"All {max_retries} attempts failed for key {key_name}")
                raise e
