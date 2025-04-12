import json
import logging
from backup_content import run_content_backup
from backup_metadata import run_metadata_backup

# Configure logging (consider using AWS CloudWatch Logs for production)
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def lambda_handler(event, context):
    """
    AWS Lambda handler function that orchestrates the backup of Tableau Cloud.

    This function calls the functions to backup both the content and metadata
    of a Tableau Cloud site.  It handles any exceptions that occur during the
    backup process and returns an appropriate status code and message.

    Args:
        event (dict):  Event data passed to the Lambda function (not used).
        context (object): Lambda context object (not used).

    Returns:
        dict: A dictionary containing the status code and a message indicating
              the success or failure of the backup.
    """
    try:
        print("🔹 Starting Tableau Cloud backup...")

        print("🔹 Starting content backup...")
        backup_folder = run_content_backup()
        print("✅ Content backup completed.")

        print("🔹 Starting metadata backup...")
        run_metadata_backup(backup_folder)
        print("✅ Metadata backup completed.")

        print("✅ Tableau Cloud backup completed successfully.")
        return {
            'statusCode': 200,
            'body': 'Tableau Cloud backup completed successfully.'
        }

    except Exception as e:
        logging.error(f"❌ Tableau Cloud backup failed: {e}")
        return {
            'statusCode': 500,
            'body': f'Error during Tableau Cloud backup: {e}'
        }