import boto3
import datetime
import logging
from botocore.exceptions import ClientError

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('/var/log/rds_backup.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def create_rds_backup():
    """Create a backup of the RDS database"""
    try:
        client = boto3.client('lightsail', region_name='ap-south-1')  # Added region_name
        
        # Replace with your database name
        database_name = "project-x-db-instance"
        
        # Generate backup name with timestamp
        timestamp = datetime.datetime.now().strftime('%Y-%m-%d-%H-%M')
        backup_name = f"{database_name}-backup-{timestamp}"
        
        # Create the backup
        response = client.create_relational_database_snapshot(
            relationalDatabaseName=database_name,
            relationalDatabaseSnapshotName=backup_name
        )
        
        logger.info(f"Successfully created backup: {backup_name}")
        return backup_name
        
    except ClientError as e:
        logger.error(f"Failed to create backup: {str(e)}")
        raise

def cleanup_old_backups(retention_days=30):
    """Delete backups older than retention period"""
    try:
        client = boto3.client('lightsail', region_name='ap-south-1')  # Added region_name
        
        # Calculate cutoff date
        cutoff_date = datetime.datetime.now() - datetime.timedelta(days=retention_days)
        
        # Get all snapshots
        response = client.get_relational_database_snapshots()
        
        # Track statistics
        deleted_count = 0
        retained_count = 0
        
        # Delete old snapshots
        for snapshot in response['relationalDatabaseSnapshots']:
            creation_date = snapshot['createdAt'].replace(tzinfo=None)
            if creation_date < cutoff_date:
                client.delete_relational_database_snapshot(
                    relationalDatabaseSnapshotName=snapshot['name']
                )
                deleted_count += 1
                logger.info(f"Deleted old snapshot: {snapshot['name']}")
            else:
                retained_count += 1
        
        logger.info(f"Cleanup complete. Deleted: {deleted_count}, Retained: {retained_count}")
                
    except ClientError as e:
        logger.error(f"Failed to cleanup old backups: {str(e)}")
        raise

if __name__ == "__main__":
    try:
        backup_name = create_rds_backup()
        cleanup_old_backups()
        logger.info("Backup process completed successfully")
    except Exception as e:
        logger.error(f"Backup process failed: {str(e)}")
