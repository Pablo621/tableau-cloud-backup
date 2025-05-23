import os

# AWS Settings
SECRET_NAME = os.environ.get("SECRET_NAME", "tableau_backup_secret")
REGION_NAME = os.environ.get("REGION_NAME", "us-east-2")
S3_BUCKET = os.environ.get("S3_BUCKET","tableau-cloud-backups-your_site_name")

# Tableau Settings
TABLEAU_BASE_URL = os.environ.get("TABLEAU_BASE_URL", "https://us-west-2a.online.tableau.com/api/3.24")
SITE_ID = os.environ.get("SITE_ID", "a21g5c14-bf38-4f1c-a8b8-1015fe2db157")  

# Limits for the number of items to back up per object type
MAX_WORKBOOKS = 3
MAX_DATASOURCES = 3
MAX_PREP_FLOWS = 3
