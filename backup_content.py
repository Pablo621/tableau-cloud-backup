import json
import boto3
import requests
import xml.etree.ElementTree as ET
import logging
import os
from datetime import datetime

# Import shared configuration values used across scripts (e.g., Secrets Manager and Tableau settings)
from config import SECRET_NAME, REGION_NAME, TABLEAU_BASE_URL, S3_BUCKET, SITE_ID, MAX_WORKBOOKS, MAX_DATASOURCES, MAX_PREP_FLOWS

# Configure logging (consider using AWS CloudWatch Logs for production)
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


def get_tableau_credentials():
    """
    Retrieve tableau credentials from AWS Secrets Manager.

    Returns:
        dict: A dictionary containing the Tableau Cloud credentials.
              Raises an exception if retrieval fails.
    """
    try:
        client = boto3.client("secretsmanager", region_name=REGION_NAME)
        secret = client.get_secret_value(SecretId=SECRET_NAME)
        credentials = json.loads(secret["SecretString"])
        logging.info("✅ Tableau credentials retrieved from Secrets Manager.")
        return credentials
    except Exception as e:
        logging.error(f"❌ Error retrieving Tableau credentials: {e}")
        raise


def authenticate():
    """
    Authenticate with Tableau Cloud and return the authentication token.

    Returns:
        str: The authentication token.
             Raises an exception if authentication fails.
    """
    credentials = get_tableau_credentials()
        
    url = f"{TABLEAU_BASE_URL}/auth/signin"
    payload = {
        "credentials": {
            "personalAccessTokenName": credentials["PAT_NAME"],
            "personalAccessTokenSecret": credentials["PAT"],
            "site": {
                "contentUrl": credentials["SITE"]
            }
        }
    }

    headers = {"Content-Type": "application/json"}

    try:
        response = requests.post(url, json=payload, headers=headers)
        response.raise_for_status()  # Raise HTTPError for bad responses (4xx or 5xx)

        logging.info(f"🔑 Authentication successful. Status Code: {response.status_code}")

        ns = {"t": "http://tableau.com/api"}
        root = ET.fromstring(response.text)

        credentials_element = root.find("t:credentials", ns)
        if credentials_element is not None:
            auth_token = credentials_element.attrib.get("token")
            logging.info(f"🔑 Authentication token extracted.")
            return auth_token
        else:
            raise Exception("Failed to extract auth token from XML response.")

    except requests.exceptions.RequestException as e:
        logging.error(f"❌ Authentication failed: {e}")
        raise
    except ET.ParseError as e:
        logging.error(f"❌ Error parsing XML response during authentication: {e}. Response text: {response.text}")
        raise
    except Exception as e:
        logging.error(f"❌ Unexpected error during authentication: {e}")
        raise


def list_workbooks(auth_token, SITE_ID):
    """
    List all workbooks in the Tableau Cloud site.

    Args:
        auth_token (str): The authentication token for Tableau Cloud.
        SITE_ID (str): The ID of the Tableau Cloud site.

    Returns:
        list: A list of dictionaries containing workbook information.
              Raises an exception if the API request fails or the response is invalid.
    """
    url = f"{TABLEAU_BASE_URL}/sites/{SITE_ID}/workbooks"

    headers = {
        "X-Tableau-Auth": auth_token
    }

    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()

        logging.info(f"🔍 Workbooks API Response: {response.status_code}")

        workbooks = []  
        ns = {"t": "http://tableau.com/api"}
        root = ET.fromstring(response.text)  

        # Attempting to find workbooks under <tsResponse>/<workbooks>
        workbooks_node = root.find("t:workbooks", ns)
        if workbooks_node is not None:
            for workbook in workbooks_node.findall("t:workbook", ns): 
                workbook_id = workbook.attrib.get("id")
                workbook_name = workbook.attrib.get("name")
                content_url = workbook.attrib.get("contentUrl")

                logging.info(f"📌 Found Workbook: {workbook_name} (ID: {workbook_id})")

                workbooks.append({
                    "id": workbook_id,
                    "name": workbook_name,
                    "contentUrl": content_url
                })
        else:
            logging.warning("⚠️ No se encontró el nodo <workbooks> en la respuesta XML.")

        logging.info(f"✅ {len(workbooks)} Workbooks processed successfully.")
        return workbooks

    except requests.exceptions.RequestException as e:
        logging.error(f"❌ Failed to retrieve workbooks: {e}")
        raise
    except ET.ParseError as e:
        logging.error(f"❌ Error parsing XML response when listing workbooks: {e}. Response text: {response.text}")
        raise
    except Exception as e:
        logging.error(f"❌ Unexpected error retrieving workbooks: {e}")
        raise


def download_workbook(auth_token, SITE_ID, workbook_id, workbook_name):
    """
    Download a specific workbook from Tableau Cloud.

    Args:
        auth_token (str): The authentication token for Tableau Cloud.
        SITE_ID (str): The ID of the Tableau Cloud site.
        workbook_id (str): The ID of the workbook to download.
        workbook_name (str): The name of the workbook.

    Returns:
        str: The path to the downloaded workbook file.
             Raises an exception if the download fails.
    """
    url = f"{TABLEAU_BASE_URL}/sites/{SITE_ID}/workbooks/{workbook_id}/content"

    headers = {
        "X-Tableau-Auth": auth_token
    }

    try:
        response = requests.get(url, headers=headers, stream=True)
        response.raise_for_status()

        logging.info(f"⬇️ Downloading Workbook: {workbook_name} (ID: {workbook_id}). Status Code: {response.status_code}")

        file_path = f"/tmp/{workbook_name}.twbx"  
        with open(file_path, "wb") as file:
            for chunk in response.iter_content(chunk_size=1024):
                file.write(chunk)

        logging.info(f"✅ Workbook {workbook_name} downloaded successfully to {file_path}")
        return file_path

    except requests.exceptions.RequestException as e:
        logging.error(f"❌ Failed to download workbook {workbook_name}: {e}")
        raise
    except Exception as e:
        logging.error(f"❌ Unexpected error downloading workbook {workbook_name}: {e}")
        raise


def upload_workbook_to_s3(file_path, S3_BUCKET, workbook_id, workbook_name, backup_folder):
    """
    Upload a file to Amazon S3.

    Args:
        file_path (str): The path to the file to upload.
        S3_BUCKET (str): The name of the S3 bucket.
        workbook_name (str): The name of the workbook (used in the S3 key).
        backup_folder (str): The folder in the S3 bucket to upload to.

    Returns:
        str: The S3 URL of the uploaded file.
             Raises an exception if the upload fails.
    """
    s3 = boto3.client("s3")
    s3_key = f"{backup_folder}workbooks/{workbook_id}_{workbook_name}.twbx" 

    try:
        s3.upload_file(file_path, S3_BUCKET, s3_key)
        s3_url = f"s3://{S3_BUCKET}/{s3_key}"
        logging.info(f"✅ Workbook uploaded to S3: {s3_url}")
        return s3_url
    except Exception as e:
        logging.error(f"❌ Failed to upload Workbook {workbook_name} to S3: {e}")
        raise


def list_prep_flows(auth_token, SITE_ID):
    """
    List all Prep Flows in the Tableau Cloud site.

    Args:
        auth_token (str): The authentication token for Tableau Cloud.
        SITE_ID (str): The ID of the Tableau Cloud site.

    Returns:
        list: A list of dictionaries containing Prep Flow information.
              Raises an exception if the API request fails or the response is invalid.
    """
    url = f"{TABLEAU_BASE_URL}/sites/{SITE_ID}/flows"

    headers = {
        "X-Tableau-Auth": auth_token
    }

    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()

        logging.info(f"🔍 Checking Prep Flows: {response.status_code}")

        flows = []
        ns = {"t": "http://tableau.com/api"}
        root = ET.fromstring(response.text)
        flows_node = root.find("t:flows", ns)

        if flows_node is not None:
            for flow in flows_node.findall("t:flow", ns):
                flow_id = flow.attrib.get("id")
                flow_name = flow.attrib.get("name")
                content_url = flow.attrib.get("contentUrl")

                logging.info(f"📌 Found Prep Flow: {flow_name} (ID: {flow_id})")

                flows.append({
                    "id": flow_id,
                    "name": flow_name,
                    "contentUrl": content_url
                })

        logging.info(f"✅ {len(flows)} Prep Flows processed successfully.")
        return flows

    except requests.exceptions.RequestException as e:
        logging.error(f"❌ Failed to retrieve Prep Flows: {e}")
        raise
    except ET.ParseError as e:
        logging.error(f"❌ Error parsing XML response when listing Prep Flows: {e}. Response text: {response.text}")
        raise
    except Exception as e:
        logging.error(f"❌ Unexpected error retrieving Prep Flows: {e}")
        raise


def download_prep_flow(auth_token, SITE_ID, flow_id, flow_name):
    """
    Download a specific Prep Flow from Tableau Cloud.

    Args:
        auth_token (str): The authentication token for Tableau Cloud.
        SITE_ID (str): The ID of the Tableau Cloud site.
        flow_id (str): The ID of the Prep Flow to download.
        flow_name (str): The name of the Prep Flow.

    Returns:
        str: The path to the downloaded Prep Flow file.
             Returns None and logs an error if the download fails.
    """
    url = f"{TABLEAU_BASE_URL}/sites/{SITE_ID}/flows/{flow_id}/content"

    headers = {
        "X-Tableau-Auth": auth_token
    }

    try:
        response = requests.get(url, headers=headers, stream=True)
        response.raise_for_status()

        logging.info(f"⬇️ Downloading Prep Flow: {flow_name} (ID: {flow_id}). Status Code: {response.status_code}")

        file_path = f"/tmp/{flow_name}.tflx"
        with open(file_path, "wb") as f:
            for chunk in response.iter_content(chunk_size=1024):
                f.write(chunk)
        
        logging.info(f"✅ Prep Flow {flow_name} downloaded successfully to {file_path}")
        return file_path

    except requests.exceptions.RequestException as e:
        logging.error(f"❌ Failed to download Prep Flow {flow_name}: {e}")
        raise
    except Exception as e:
        logging.error(f"❌ Unexpected error downloading Prep Flow {flow_name}: {e}")
        raise


def upload_prep_flow_to_s3(file_path, S3_BUCKET, flow_id, flow_name, backup_folder):
    """
    Upload a Prep Flow file to Amazon S3.

    Args:
        file_path (str): The path to the file to upload.
        S3_BUCKET (str): The name of the S3 bucket.
        flow_name (str): The name of the Prep Flow (used in the S3 key).
        backup_folder (str): The folder in the S3 bucket to upload to.

    Returns:
        str: The S3 URL of the uploaded file.
             Raises an exception if the upload fails.
    """
    s3 = boto3.client("s3")
    s3_key = f"{backup_folder}prep_flows/{flow_id}_{flow_name}.tflx"

    try:
        s3.upload_file(file_path, S3_BUCKET, s3_key)
        s3_url = f"s3://{S3_BUCKET}/{s3_key}"
        logging.info(f"✅ Prep Flow uploaded to S3: {s3_url}")
        return s3_url
    except Exception as e:
        logging.error(f"❌ Failed to upload Prep Flow {flow_name} to S3: {e}")
        raise


def list_published_data_sources(auth_token, SITE_ID):
    """
    List all published data sources in the Tableau Cloud site.

    Args:
        auth_token (str): The authentication token for Tableau Cloud.
        SITE_ID (str): The ID of the Tableau Cloud site.

    Returns:
        list: A list of dictionaries containing data source information.
              Raises an exception if the API request fails or the response is invalid.
    """
    url = f"{TABLEAU_BASE_URL}/sites/{SITE_ID}/datasources"

    headers = {
        "X-Tableau-Auth": auth_token
    }

    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()

        logging.info(f"🔍 Checking Published Data Sources: {response.status_code}")

        data_sources = []
        ns = {"t": "http://tableau.com/api"}
        root = ET.fromstring(response.text)
        datasources_node = root.find("t:datasources", ns)

        if datasources_node is not None:
            for ds in datasources_node.findall("t:datasource", ns):
                ds_id = ds.attrib.get("id")
                ds_name = ds.attrib.get("name")
                content_url = ds.attrib.get("contentUrl")

                logging.info(f"📌 Found Data Source: {ds_name} (ID: {ds_id})")

                data_sources.append({
                    "id": ds_id,
                    "name": ds_name,
                    "contentUrl": content_url
                })

        logging.info(f"✅ {len(data_sources)} Published Data Sources processed successfully.")
        return data_sources

    except requests.exceptions.RequestException as e:
        logging.error(f"❌ Failed to retrieve Published Data Sources: {e}")
        raise
    except ET.ParseError as e:
        logging.error(f"❌ Error parsing XML response when listing Data Sources: {e}. Response text: {response.text}")
        raise
    except Exception as e:
        logging.error(f"❌ Unexpected error retrieving Published Data Sources: {e}")
        raise


def download_data_source(auth_token, SITE_ID, ds_id, ds_name):
    """
    Download a specific Published Data Source from Tableau Cloud.

    Args:
        auth_token (str): The authentication token for Tableau Cloud.
        SITE_ID (str): The ID of the Tableau Cloud site.
        ds_id (str): The ID of the Data Source to download.
        ds_name (str): The name of the Data Source.

    Returns:
        str: The path to the downloaded Data Source file.
             Raises an exception if the download fails.
    """
    url = f"{TABLEAU_BASE_URL}/sites/{SITE_ID}/datasources/{ds_id}/content"

    headers = {
        "X-Tableau-Auth": auth_token
    }

    try:
        response = requests.get(url, headers=headers, stream=True)
        response.raise_for_status()

        logging.info(f"⬇️ Downloading Data Source: {ds_name} (ID: {ds_id}). Status Code: {response.status_code}")

        file_path = f"/tmp/{ds_name}.tdsx"
        with open(file_path, "wb") as f:
            for chunk in response.iter_content(chunk_size=1024):
                f.write(chunk)
        
        logging.info(f"✅ Data Source {ds_name} downloaded successfully to {file_path}")
        return file_path

    except requests.exceptions.RequestException as e:
        logging.error(f"❌ Failed to download Data Source {ds_name}: {e}")
        raise
    except Exception as e:
        logging.error(f"❌ Unexpected error downloading Data Source {ds_name}: {e}")
        raise


def upload_data_source_to_s3(file_path, S3_BUCKET, ds_id, ds_name, backup_folder):
    """
    Upload a downloaded Tableau Published Data Source to the specified S3 bucket.

    Args:
        file_path (str): The path to the file to upload.
        S3_BUCKET (str): The name of the S3 bucket.
        ds_name (str): The name of the Data Source (used in the S3 key).
        backup_folder (str): The folder in the S3 bucket to upload to.

    Returns:
        str: The S3 URL of the uploaded file.
             Raises an exception if the upload fails.
    """
    s3 = boto3.client("s3")
    s3_key = f"{backup_folder}published_data_sources/{ds_id}_{ds_name}.tdsx"
    
    try:
        s3.upload_file(file_path, S3_BUCKET, s3_key)

        s3_url = f"s3://{S3_BUCKET}/{s3_key}"
        logging.info(f"✅ Data Source uploaded to S3: {s3_url}")
        return s3_url
    except Exception as e:
        logging.error(f"❌ Failed to upload Data Source {ds_name} to S3: {e}")
        raise


def run_content_backup():
    """
    Orchestrates the backup of Tableau Cloud content (Workbooks, Prep Flows, Data Sources).

    This function authenticates with Tableau Cloud, retrieves lists of content,
    downloads each item, and uploads the downloaded files to S3.  It also
    manages the creation of a timestamped backup folder in S3.

    Returns:
        dict:  A dictionary containing the S3 backup folder path on success,
               or an error message with a 500 status code on failure.
    """
    try:
        auth_token = authenticate()
        

        # 🔹 Generate timestamp and define root folder
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        backup_folder = f"backup_tableau_cloud_pablosite_{timestamp}/"
        logging.info(f"📂 Creating backup folder: {backup_folder}")

        # ✅ Backup Workbooks
        workbooks = list_workbooks(auth_token, SITE_ID)
        uploaded_workbooks = []
        count_workbooks = 0

        for workbook in workbooks:
            if count_workbooks >= MAX_WORKBOOKS:
                logging.info(f"🔹 Limit reached: {MAX_WORKBOOKS} workbooks processed. Stopping backup.")
                break

            workbook_id = workbook["id"]
            workbook_name = workbook["name"]

            logging.info(f"📥 Downloading workbook: {workbook_name} (ID: {workbook_id})")

            try:
                file_path = download_workbook(auth_token, SITE_ID, workbook_id, workbook_name)
                s3_url = upload_workbook_to_s3(file_path, S3_BUCKET, workbook_id, workbook_name, backup_folder)

                uploaded_workbooks.append({"name": workbook_name, "s3_url": s3_url})
                count_workbooks += 1

            except Exception as e:
                logging.error(f"⚠️ Error processing {workbook_name}: {e}")

        # ✅ Backup Prep Flows
        prep_flows = list_prep_flows(auth_token, SITE_ID)
        uploaded_flows = []
        count_prep_flows = 0

        for flow in prep_flows:
            if count_prep_flows >= MAX_PREP_FLOWS:
                logging.info(f"🔹 Limit reached: {MAX_PREP_FLOWS} prep flows processed. Stopping backup.")
                break

            flow_id = flow["id"]
            flow_name = flow["name"]

            logging.info(f"📥 Downloading Prep Flow: {flow_name} (ID: {flow_id})")

            try:
                file_path = download_prep_flow(auth_token, SITE_ID, flow_id, flow_name)
                s3_url = upload_prep_flow_to_s3(file_path, S3_BUCKET, flow_id, flow_name, backup_folder)

                uploaded_flows.append({"name": flow_name, "s3_url": s3_url})
                count_prep_flows += 1

            except Exception as e:
                logging.error(f"⚠️ Error processing Prep Flow {flow_name}: {e}")

        # ✅ Back up Published Data Sources
        data_sources = list_published_data_sources(auth_token, SITE_ID)
        uploaded_data_sources = []       
        count_data_sources = 0

        for ds in data_sources:
            if count_data_sources >= MAX_DATASOURCES:
                logging.info(f"🔹 Limit reached: {MAX_DATASOURCES} data sources processed. Stopping backup.")
                break

            ds_id = ds["id"]
            ds_name = ds["name"]

            logging.info(f"📥 Downloading Data Source: {ds_name} (ID: {ds_id})")

            try:
                file_path = download_data_source(auth_token, SITE_ID, ds_id, ds_name)
                s3_url = upload_data_source_to_s3(file_path, S3_BUCKET, ds_id, ds_name, backup_folder)

                uploaded_data_sources.append({"name": ds_name, "s3_url": s3_url})
                count_data_sources += 1

            except Exception as e:
                logging.error(f"⚠️ Error processing Data Source {ds_name}: {e}")

        return backup_folder

    except Exception as e:
        return {
            "statusCode": 500,
            "body": json.dumps(f"❌ Error: {str(e)}")
        }