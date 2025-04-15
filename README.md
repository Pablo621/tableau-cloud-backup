# Tableau Cloud Backup with Python and REST API

Automated backup system for Tableau Cloud using Python 3.13, Tableau REST API, and AWS Lambda. This project allows you to extract and store both published content and metadata from your Tableau Cloud Site into Amazon S3, organized by date and object type.

> Built for learning, experimentation, and inspiration—not for production without further testing.

---

## 🔧 What It Backs Up

### Published Content (downloaded as `.twbx`, `.tdsx`, `.tflx`):
- Workbooks
- Published Data Sources
- Prep Flows

### Metadata (stored as `.json`):
- Users and Roles  
- Groups  
- Favorites  
- Subscriptions  
- Custom Views  
- Projects  
- Workbooks, Data Sources, and Prep Flows metadata  
- Virtual Connections  
- Extract Refresh Tasks  

> ❗ **Permissions** and **Personal Access Tokens** are not backed up directly as JSON, since this information is already available via Admin Insights published data sources, which are included in the content backup.

---

## ☁️ Tech Stack

- **Python 3.13**
- **Tableau REST API v3.24**
- **AWS Lambda** – Executes the scripts automatically  
- **Amazon S3** – Stores the backup files  
- **AWS Secrets Manager** – Stores Tableau credentials securely  

---

## 📁 Folder Structure in S3

Backups are created in the following structure:

```
/backup_tableau_cloud_<timestamp>/
│
├── workbooks/
├── published_data_sources/
├── prep_flows/
└── metadata_tableau/
    ├── users.json
    ├── groups.json
    ├── ...
```

Each file is named using the current timestamp and organized by type for easy reference and restore logic.

---

## 🚀 How It Works

1. AWS Lambda triggers the script on schedule or manually.  
2. The script authenticates to Tableau Cloud using a Personal Access Token (PAT).  
3. Content is downloaded using Tableau’s REST API.  
4. Files are uploaded to S3.  
5. Metadata is extracted and stored as JSON.

---

## ⚙️ Prerequisites

### AWS
- An AWS account (free tier is enough to start).
- **S3_BUCKET**: Name of the S3 bucket where backups will be stored.
- **SECRET_NAME**: Name of the secret in AWS Secrets Manager with Tableau credentials.
- **REGION_NAME**: AWS region where the secret is stored.

### Tableau Cloud
- A Tableau Cloud Site with existing content to test.
- **SITE_ID**: The ID of the Tableau Site (can be retrieved from the REST API).
- A valid PAT (Personal Access Token).

These values must be provided as environment variables in the Lambda function.

---

## ⚙️ Configuration Variables

These values can be adjusted in the `config.py` file to control the backup behavior:

```python
MAX_WORKBOOKS = 3         # Maximum number of Workbooks to back up
MAX_DATASOURCES = 3       # Maximum number of Published Data Sources to back up
MAX_PREP_FLOWS = 3        # Maximum number of Prep Flows to back up
```

---

## ⚠️ Disclaimer

This is a learning-focused project. It has been tested and works with the current version of the Tableau REST API, but:
- It is not production-grade.
- You are responsible for reviewing and extending it based on your own backup and restore requirements.

---

## 📄 License

MIT License – see [`LICENSE`](./LICENSE) for details.

---

## ✍️ Author

Pablo F., Solution Engineer @ Tableau  
Presented at Tableau Conference 2025 – San Diego  
Feel free to connect and share ideas!

---

## 📦 Optional: Build the Deployment ZIP from Requirements

Although this project includes all dependencies directly in the `.zip`, you can regenerate it using the `requirements.txt` file if needed.

### 🔧 Steps to create the deployment ZIP

```bash
# 1. Install dependencies in a local folder
pip install -r requirements.txt -t package/

# 2. Move into that folder
cd package

# 3. Zip everything inside the folder
zip -r ../lambda_function.zip .

# 4. Go back and add your Python scripts to the ZIP
cd ..
zip -g lambda_function.zip lambda_handler.py backup_metadata.py backup_content.py config.py
```

You can now upload `lambda_function.zip` to AWS Lambda manually or via the AWS CLI.

> This workflow is optional but useful for sharing, automation, or rebuilding your environment.
