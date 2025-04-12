import json
import boto3
import requests
import xml.etree.ElementTree as ET
import logging
import os
from datetime import datetime
from backup_content import authenticate

# Import shared configuration values used across scripts (e.g., Secrets Manager and Tableau settings)
from config import SECRET_NAME, REGION_NAME, TABLEAU_BASE_URL, S3_BUCKET, SITE_ID


# Configure logging (consider using AWS CloudWatch Logs for production)
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


def list_users(auth_token, SITE_ID):
    """
    Lists all users in the Tableau Cloud site.

    Args:
        auth_token (str): The authentication token for Tableau Cloud.
        SITE_ID (str): The ID of the Tableau Cloud site.

    Returns:
        dict: A dictionary containing user information.
              Raises an exception if the API request fails or the response is invalid.
    """
    url = f"{TABLEAU_BASE_URL}/sites/{SITE_ID}/users"
    headers = {"X-Tableau-Auth": auth_token}

    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()  # Raise HTTPError for bad responses (4xx or 5xx)
        logging.info(f"🔍 Checking Users: {response.status_code}")

        users_data = {"pagination": {}, "users": {"user": []}}
        ns = {"t": "http://tableau.com/api"}
        root = ET.fromstring(response.text)

        # Extract pagination info
        pagination_node = root.find("t:pagination", ns)
        if pagination_node is not None:
            users_data["pagination"] = {
                "pageNumber": pagination_node.attrib.get("pageNumber", "1"),
                "pageSize": pagination_node.attrib.get("pageSize", "100"),
                "totalAvailable": pagination_node.attrib.get("totalAvailable", "0"),
            }

        # Extract user details
        users_node = root.find("t:users", ns)
        if users_node is not None:
            for user in users_node.findall("t:user", ns):
                user_data = {
                    "domain": {"name": user.attrib.get("domain", "external")},
                    "authSetting": user.attrib.get("authSetting", "ServerDefault"),
                    "email": user.attrib.get("email", "N/A"),
                    "externalAuthUserId": user.attrib.get("externalAuthUserId", "N/A"),
                    "fullName": user.attrib.get("fullName", "N/A"),
                    "id": user.attrib.get("id", "N/A"),
                    "lastLogin": user.attrib.get("lastLogin", "N/A"),
                    "name": user.attrib.get("name", user.attrib.get("email", "N/A")),
                    "siteRole": user.attrib.get("siteRole", "N/A"),
                    "locale": user.attrib.get("locale", ""),
                    "language": user.attrib.get("language", "en"),
                    "idpConfigurationId": user.attrib.get("idpConfigurationId", "N/A")
                }
                logging.info(f"📌 Found User: {user_data['name']} (Role: {user_data['siteRole']})")
                users_data["users"]["user"].append(user_data)

        logging.info(f"✅ {len(users_data.get('users', {}).get('user', []))} Users processed successfully.")
        return users_data

    except requests.exceptions.RequestException as e:
        logging.error(f"❌ Error listing users: {e}")
        raise
    except ET.ParseError as e:
        logging.error(f"❌ Error parsing XML response when listing users: {e}. Response text: {response.text}")
        raise
    except Exception as e:
        logging.error(f"❌ Unexpected error listing users: {e}")
        raise


def save_json(data, file_path, description="data"):
    """
    Saves data to a JSON file.

    Args:
        data (dict or list): The data to save.
        file_path (str): The path to the file.
        description (str, optional): Description of the data being saved. Defaults to "data".

    Raises:
        IOError: If there's an error writing to the file.
    """
    try:
        with open(file_path, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=4, ensure_ascii=False)
        logging.info(f"✅ {description.capitalize()} JSON saved: {file_path}")
    except IOError as e:
        logging.error(f"❌ Error saving {description} to JSON: {e}")
        raise


def list_groups(auth_token, SITE_ID):
    """
    Lists all groups in the Tableau Cloud site.

    Args:
        auth_token (str): The authentication token for Tableau Cloud.
        SITE_ID (str): The ID of the Tableau Cloud site.

    Returns:
        dict: A dictionary containing group information.
              Raises an exception if the API request fails or the response is invalid.
    """
    url = f"{TABLEAU_BASE_URL}/sites/{SITE_ID}/groups"
    headers = {"X-Tableau-Auth": auth_token}

    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        logging.info(f"🔍 Checking Groups: {response.status_code}")

        groups_data = {"pagination": {}, "groups": {"group": []}}
        ns = {"t": "http://tableau.com/api"}
        root = ET.fromstring(response.text)

        # Extract pagination info
        pagination_node = root.find("t:pagination", ns)
        if pagination_node is not None:
            groups_data["pagination"] = {
                "pageNumber": pagination_node.attrib.get("pageNumber", "1"),
                "pageSize": pagination_node.attrib.get("pageSize", "100"),
                "totalAvailable": pagination_node.attrib.get("totalAvailable", "0"),
            }

        # Extract group details
        groups_node = root.find("t:groups", ns)
        if groups_node is not None:
            for group in groups_node.findall("t:group", ns):
                group_id = group.attrib.get("id", "N/A")
                group_name = group.attrib.get("name", "N/A")

                group_data = {
                    "domain": {"name": "local"},
                    "id": group_id,
                    "name": group_name,
                    "users": []
                }

                import_node = group.find("t:import", ns)
                if import_node is not None:
                    group_data["import"] = {
                        "domainName": import_node.attrib.get("domainName", "N/A"),
                        "siteRole": import_node.attrib.get("siteRole", "N/A"),
                        "grantLicenseMode": import_node.attrib.get("grantLicenseMode", "N/A")
                    }

                group_data["users"] = list_group_users(auth_token, SITE_ID, group_id)
                logging.info(f"📌 Found Group: {group_name} (ID: {group_id}) - Users: {len(group_data.get('users', []))}")
                groups_data["groups"]["group"].append(group_data)

        logging.info(f"✅ {len(groups_data.get('groups', {}).get('group', []))} Groups processed successfully.")
        return groups_data

    except requests.exceptions.RequestException as e:
        logging.error(f"❌ Error listing groups: {e}")
        raise
    except ET.ParseError as e:
        logging.error(f"❌ Error parsing XML response when listing groups: {e}. Response text: {response.text}")
        raise
    except Exception as e:
        logging.error(f"❌ Unexpected error listing groups: {e}")
        raise


def list_group_users(auth_token, SITE_ID, group_id):
    """
    Lists all users in a specific group.

    Args:
        auth_token (str): The authentication token for Tableau Cloud.
        SITE_ID (str): The ID of the Tableau Cloud site.
        group_id (str): The ID of the group.

    Returns:
        list: A list of dictionaries containing user IDs and names.
              Raises an exception if the API request fails or the response is invalid.
    """
    url = f"{TABLEAU_BASE_URL}/sites/{SITE_ID}/groups/{group_id}/users"
    headers = {"X-Tableau-Auth": auth_token}

    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        logging.info(f"🔍 Checking Users in Group {group_id}: {response.status_code}")

        users = []
        ns = {"t": "http://tableau.com/api"}
        root = ET.fromstring(response.text)
        users_node = root.find("t:users", ns)

        if users_node is not None:
            for user in users_node.findall("t:user", ns):
                user_id = user.attrib.get("id", "N/A")
                user_name = user.attrib.get("name", "N/A")
                users.append({"id": user_id, "name": user_name})

        logging.info(f"✅ {len(users)} Users in Group {group_id} processed successfully.")
        return users

    except requests.exceptions.RequestException as e:
        logging.error(f"❌ Error listing users in group {group_id}: {e}")
        raise
    except ET.ParseError as e:
        logging.error(f"❌ Error parsing XML response when listing users in group {group_id}: {e}. Response text: {response.text}")
        raise
    except Exception as e:
        logging.error(f"❌ Unexpected error listing users in group {group_id}: {e}")
        raise


def list_projects(auth_token, SITE_ID):
    """
    Lists all projects in the Tableau Cloud site.

    Args:
        auth_token (str): The authentication token for Tableau Cloud.
        SITE_ID (str): The ID of the Tableau Cloud site.

    Returns:
        dict: A dictionary containing project information.
              Raises an exception if the API request fails or the response is invalid.
    """
    url = f"{TABLEAU_BASE_URL}/sites/{SITE_ID}/projects"
    headers = {"X-Tableau-Auth": auth_token}

    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        logging.info(f"🔍 Checking Projects: {response.status_code}")

        projects_data = {"pagination": {}, "projects": {"project": []}}
        ns = {"t": "http://tableau.com/api"}
        root = ET.fromstring(response.text)

        # Extract pagination info
        pagination_node = root.find("t:pagination", ns)
        if pagination_node is not None:
            projects_data["pagination"] = {
                "pageNumber": pagination_node.attrib.get("pageNumber", "1"),
                "pageSize": pagination_node.attrib.get("pageSize", "100"),
                "totalAvailable": pagination_node.attrib.get("totalAvailable", "0")
            }

        # Extract project details
        projects_node = root.find("t:projects", ns)
        if projects_node is not None:
            for project in projects_node.findall("t:project", ns):
                owner_node = project.find("t:owner", ns)
                project_data = {
                    "id": project.attrib.get("id", "N/A"),
                    "name": project.attrib.get("name", "N/A"),
                    "description": project.attrib.get("description", ""),
                    "createdAt": project.attrib.get("createdAt", "N/A"),
                    "updatedAt": project.attrib.get("updatedAt", "N/A"),
                    "contentPermissions": project.attrib.get("contentPermissions", "N/A"),
                    "owner": {
                        "id": owner_node.attrib.get("id", "N/A") if owner_node is not None else "N/A"
                    }
                }

                if "parentProjectId" in project.attrib:
                    project_data["parentProjectId"] = project.attrib["parentProjectId"]

                logging.info(f"📌 Found Project: {project_data['name']} (ID: {project_data['id']})")
                projects_data["projects"]["project"].append(project_data)

        logging.info(f"✅ {len(projects_data.get('projects', {}).get('project', []))} Projects processed successfully.")
        return projects_data

    except requests.exceptions.RequestException as e:
        logging.error(f"❌ Error listing projects: {e}")
        raise
    except ET.ParseError as e:
        logging.error(f"❌ Error parsing XML response when listing projects: {e}. Response text: {response.text}")
        raise
    except Exception as e:
        logging.error(f"❌ Unexpected error listing projects: {e}")
        raise


def list_workbooks(auth_token, SITE_ID):
    """
    Lists all workbooks in the Tableau Cloud site.

    Args:
        auth_token (str): The authentication token for Tableau Cloud.
        SITE_ID (str): The ID of the Tableau Cloud site.

    Returns:
        list: A list of dictionaries containing workbook information.
              Raises an exception if the API request fails or the response is invalid.
    """
    url = f"{TABLEAU_BASE_URL}/sites/{SITE_ID}/workbooks"
    headers = {"X-Tableau-Auth": auth_token}

    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        logging.info(f"🔍 Checking Workbooks: {response.status_code}")

        workbooks = []
        ns = {"t": "http://tableau.com/api"}
        root = ET.fromstring(response.text)
        workbooks_node = root.find("t:workbooks", ns)

        if workbooks_node is not None:
            for workbook in workbooks_node.findall("t:workbook", ns):
                project_node = workbook.find("t:project", ns)
                location_node = workbook.find("t:location", ns)
                owner_node = workbook.find("t:owner", ns)

                workbook_data = {
                    "id": workbook.attrib.get("id", "N/A"),
                    "name": workbook.attrib.get("name", "N/A"),
                    "project": {
                        "id": project_node.attrib["id"] if project_node is not None else "N/A",
                        "name": project_node.attrib["name"] if project_node is not None else "N/A"
                    },
                    "location": {
                        "id": location_node.attrib["id"] if location_node is not None else "N/A",
                        "type": location_node.attrib["type"] if location_node is not None else "N/A",
                        "name": location_node.attrib["name"] if location_node is not None else "N/A"
                    },
                    "owner": {
                        "id": owner_node.attrib["id"] if owner_node is not None else "N/A",
                        "name": owner_node.attrib["name"] if owner_node is not None else "N/A"
                    },
                    "tags": {},
                    "dataAccelerationConfig": {},
                    "description": workbook.attrib.get("description", ""),
                    "contentUrl": workbook.attrib.get("contentUrl", "N/A"),
                    "webpageUrl": workbook.attrib.get("webpageUrl", "N/A"),
                    "showTabs": workbook.attrib.get("showTabs", "false"),
                    "size": workbook.attrib.get("size", "N/A"),
                    "createdAt": workbook.attrib.get("createdAt", "N/A"),
                    "updatedAt": workbook.attrib.get("updatedAt", "N/A"),
                    "encryptExtracts": workbook.attrib.get("encryptExtracts", "false"),
                    "defaultViewId": workbook.attrib.get("defaultViewId", "N/A")
                }

                logging.info(f"📌 Found Workbook: {workbook_data['name']} (ID: {workbook_data['id']})")
                workbooks.append(workbook_data)

        logging.info(f"✅ {len(workbooks)} Workbooks processed successfully.")
        return workbooks

    except requests.exceptions.RequestException as e:
        logging.error(f"❌ Error listing workbooks: {e}")
        raise
    except ET.ParseError as e:
        logging.error(f"❌ Error parsing XML response when listing workbooks: {e}. Response text: {response.text}")
        raise
    except Exception as e:
        logging.error(f"❌ Unexpected error listing workbooks: {e}")
        raise


def list_datasources(auth_token, SITE_ID):
    """
    Lists all datasources in the Tableau Cloud site.

    Args:
        auth_token (str): The authentication token for Tableau Cloud.
        SITE_ID (str): The ID of the Tableau Cloud site.

    Returns:
        list: A list of dictionaries containing datasource information.
              Raises an exception if the API request fails or the response is invalid.
    """
    url = f"{TABLEAU_BASE_URL}/sites/{SITE_ID}/datasources"
    headers = {"X-Tableau-Auth": auth_token}

    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        logging.info(f"🔍 Checking Data Sources: {response.status_code}")

        datasources = []
        ns = {"t": "http://tableau.com/api"}
        root = ET.fromstring(response.text)
        datasources_node = root.find("t:datasources", ns)

        if datasources_node is not None:
            for datasource in datasources_node.findall("t:datasource", ns):
                owner_node = datasource.find("t:owner", ns)

                datasource_data = {
                    "id": datasource.attrib.get("id", "N/A"),
                    "name": datasource.attrib.get("name", "N/A"),
                    "project": {
                        "id": datasource.find("t:project", ns).attrib.get("id", "N/A") if datasource.find("t:project", ns) is not None else "N/A",
                        "name": datasource.find("t:project", ns).attrib.get("name", "N/A") if datasource.find("t:project", ns) is not None else "N/A"
                    },
                    "owner": {
                        "id": owner_node.attrib.get("id", "N/A") if owner_node is not None else "N/A",
                        "name": owner_node.attrib.get("name", "N/A") if owner_node is not None else "N/A"
                    },
                    "contentUrl": datasource.attrib.get("contentUrl", "N/A"),
                    "createdAt": datasource.attrib.get("createdAt", "N/A"),
                    "updatedAt": datasource.attrib.get("updatedAt", "N/A"),
                    "size": datasource.attrib.get("size", "0"),
                    "encryptExtracts": datasource.attrib.get("encryptExtracts", "false"),
                    "hasExtracts": datasource.attrib.get("hasExtracts", "false"),
                    "type": datasource.attrib.get("type", "N/A"),
                    "isCertified": datasource.attrib.get("isCertified", "false"),
                    "useRemoteQueryAgent": datasource.attrib.get("useRemoteQueryAgent", "false"),
                    "description": datasource.find("t:description", ns).text if datasource.find("t:description", ns) is not None else ""
                }

                logging.info(f"📌 Found Data Source: {datasource_data['name']} (ID: {datasource_data['id']})")
                datasources.append(datasource_data)

        logging.info(f"✅ {len(datasources)} Data Sources processed successfully.")
        return datasources

    except requests.exceptions.RequestException as e:
        logging.error(f"❌ Error listing datasources: {e}")
        raise
    except ET.ParseError as e:
        logging.error(f"❌ Error parsing XML response when listing datasources: {e}. Response text: {response.text}")
        raise
    except Exception as e:
        logging.error(f"❌ Unexpected error listing datasources: {e}")
        raise


def list_extract_refresh_tasks(auth_token, SITE_ID):
    """
    Lists all extract refresh tasks in the Tableau Cloud site.

    Args:
        auth_token (str): The authentication token for Tableau Cloud.
        SITE_ID (str): The ID of the Tableau Cloud site.

    Returns:
        dict: A dictionary containing extract refresh task information.
              Raises an exception if the API request fails or the response is invalid.
    """
    url = f"{TABLEAU_BASE_URL}/sites/{SITE_ID}/tasks/extractRefreshes"
    headers = {"X-Tableau-Auth": auth_token}

    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        logging.info(f"🔍 Checking Extract Refresh Tasks: {response.status_code}")

        tasks = []
        ns = {"t": "http://tableau.com/api"}
        root = ET.fromstring(response.text)
        tasks_node = root.find("t:tasks", ns)

        if tasks_node is not None:
            for task_node in tasks_node.findall("t:task", ns):
                refresh_node = task_node.find("t:extractRefresh", ns)
                if refresh_node is None:
                    continue

                schedule_node = refresh_node.find("t:schedule", ns)
                frequency_details_node = schedule_node.find("t:frequencyDetails", ns) if schedule_node is not None else None
                intervals_node = frequency_details_node.find("t:intervals", ns) if frequency_details_node is not None else None

                intervals = []
                if intervals_node is not None:
                    for interval in intervals_node.findall("t:interval", ns):
                        interval_data = {}
                        if "hours" in interval.attrib:
                            interval_data["hours"] = interval.attrib["hours"]
                        if "weekDay" in interval.attrib:
                            interval_data["weekDay"] = interval.attrib["weekDay"]
                        intervals.append(interval_data)

                task_data = {
                    "extractRefresh": {
                        "schedule": {
                            "frequency": schedule_node.attrib.get("frequency", "N/A") if schedule_node is not None else "N/A",
                            "nextRunAt": schedule_node.attrib.get("nextRunAt", "N/A") if schedule_node is not None else "N/A",
                            "frequencyDetails": {
                                "intervals": {"interval": intervals},
                                "start": frequency_details_node.attrib.get("start", "N/A") if frequency_details_node is not None else "N/A",
                                "end": frequency_details_node.attrib.get("end", "N/A") if frequency_details_node is not None else "N/A"
                            }
                        },
                        "datasource": {
                            "id": refresh_node.find("t:datasource", ns).attrib.get("id", "N/A") if refresh_node.find("t:datasource", ns) is not None else "N/A"
                        },
                        "id": refresh_node.attrib.get("id", "N/A"),
                        "priority": refresh_node.attrib.get("priority", "N/A"),
                        "consecutiveFailedCount": refresh_node.attrib.get("consecutiveFailedCount", "0"),
                        "type": refresh_node.attrib.get("type", "N/A")
                    }
                }

                logging.info(f"📌 Found Extract Task ID: {task_data['extractRefresh']['id']}")
                tasks.append(task_data)

        logging.info(f"✅ {len(tasks)} Extract Refresh Tasks processed successfully.")
        return {"tasks": {"task": tasks}}

    except requests.exceptions.RequestException as e:
        logging.error(f"❌ Error listing extract refresh tasks: {e}")
        raise
    except ET.ParseError as e:
        logging.error(f"❌ Error parsing XML response when listing extract refresh tasks: {e}. Response text: {response.text}")
        raise
    except Exception as e:
        logging.error(f"❌ Unexpected error listing extract refresh tasks: {e}")
        raise


def list_flows(auth_token, SITE_ID):
    """
    Lists all flows in the Tableau Cloud site.

    Args:
        auth_token (str): The authentication token for Tableau Cloud.
        SITE_ID (str): The ID of the Tableau Cloud site.

    Returns:
        list: A list of dictionaries containing flow information.
              Raises an exception if the API request fails or the response is invalid.
    """
    url = f"{TABLEAU_BASE_URL}/sites/{SITE_ID}/flows"
    headers = {"X-Tableau-Auth": auth_token}

    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        logging.info(f"🔍 Checking Flows: {response.status_code}")

        flows = []
        ns = {"t": "http://tableau.com/api"}
        root = ET.fromstring(response.text)
        flows_node = root.find("t:flows", ns)

        if flows_node is not None:
            for flow in flows_node.findall("t:flow", ns):
                owner_node = flow.find("t:owner", ns)
                project_node = flow.find("t:project", ns)

                flow_data = {
                    "id": flow.attrib.get("id", "N/A"),
                    "name": flow.attrib.get("name", "N/A"),
                    "description": flow.attrib.get("description", ""),
                    "webpageUrl": flow.attrib.get("webpageUrl", "N/A"),
                    "fileType": flow.attrib.get("fileType", "N/A"),
                    "createdAt": flow.attrib.get("createdAt", "N/A"),
                    "updatedAt": flow.attrib.get("updatedAt", "N/A"),
                    "project": {
                        "id": project_node.attrib.get("id", "N/A") if project_node is not None else "N/A",
                        "name": project_node.attrib.get("name", "N/A") if project_node is not None else "N/A",
                        "description": project_node.attrib.get("description", "") if project_node is not None else ""
                    },
                    "owner": {
                        "id": owner_node.attrib.get("id", "N/A") if owner_node is not None else "N/A",
                        "name": owner_node.attrib.get("name", "N/A") if owner_node is not None else "N/A",
                        "email": owner_node.attrib.get("email", "N/A") if owner_node is not None else "N/A",
                        "fullName": owner_node.attrib.get("fullName", "N/A") if owner_node is not None else "N/A",
                        "lastLogin": owner_node.attrib.get("lastLogin", "N/A") if owner_node is not None else "N/A",
                        "siteRole": owner_node.attrib.get("siteRole", "N/A") if owner_node is not None else "N/A"
                    },
                    "tags": {},
                    "parameters": {}
                }

                logging.info(f"📌 Found Flow: {flow_data['name']} (ID: {flow_data['id']})")
                flows.append(flow_data)

        logging.info(f"✅ {len(flows)} Flows processed successfully.")
        return flows

    except requests.exceptions.RequestException as e:
        logging.error(f"❌ Error listing flows: {e}")
        raise
    except ET.ParseError as e:
        logging.error(f"❌ Error parsing XML response when listing flows: {e}. Response text: {response.text}")
        raise
    except Exception as e:
        logging.error(f"❌ Unexpected error listing flows: {e}")
        raise


def list_favorites(auth_token, SITE_ID, users):
    """
    Lists all favorites for all users in the Tableau Cloud site.

    Args:
        auth_token (str): The authentication token for Tableau Cloud.
        SITE_ID (str): The ID of the Tableau Cloud site.
        users (list): A list of dictionaries containing user information (id, name, email).

    Returns:
        dict: A dictionary containing favorites information, organized by user.
              Raises an exception if the API request fails or the response is invalid.
    """

    headers = {"X-Tableau-Auth": auth_token}
    favorites_data = {"favorites": {"favorite": []}}

    try:
        for user in users:
            user_id = user["id"]
            user_name = user.get("name", "N/A")
            user_email = user.get("email", "N/A")

            url = f"{TABLEAU_BASE_URL}/sites/{SITE_ID}/favorites/{user_id}"
            response = requests.get(url, headers=headers)
            response.raise_for_status()
            logging.info(f"🔍 Checking Favorites for User {user_id}: {response.status_code}")

            ns = {"t": "http://tableau.com/api"}
            root = ET.fromstring(response.text)
            favorites_node = root.find("t:favorites", ns)

            user_favorites = []

            if favorites_node is not None:
                for favorite in favorites_node.findall("t:favorite", ns):
                    view_node = favorite.find("t:view", ns)

                    if view_node is not None:
                        owner_node = view_node.find("t:owner", ns)
                        owner_id = owner_node.attrib.get("id", "N/A") if owner_node is not None else "N/A"
                        owner_name = owner_node.attrib.get("name", "N/A") if owner_node is not None else "N/A"

                        favorite_data = {
                            "user": {
                                "id": user_id,
                                "name": user_name
                            },
                            "view": {
                                "id": view_node.attrib.get("id", "N/A"),
                                "name": view_node.attrib.get("name", "N/A"),
                                "contentUrl": view_node.attrib.get("contentUrl", "N/A"),
                                "createdAt": view_node.attrib.get("createdAt", "N/A"),
                                "updatedAt": view_node.attrib.get("updatedAt", "N/A"),
                                "viewUrlName": view_node.attrib.get("viewUrlName", "N/A"),
                                "workbook": {
                                    "id": view_node.find("t:workbook", ns).attrib.get("id", "N/A") if view_node.find("t:workbook", ns) is not None else "N/A"
                                },
                                "owner": {
                                    "id": owner_id,
                                    "name": owner_name
                                },
                                "project": {
                                    "id": view_node.find("t:project", ns).attrib.get("id", "N/A") if view_node.find("t:project", ns) is not None else "N/A"
                                },
                                "location": {
                                    "id": view_node.find("t:location", ns).attrib.get("id", "N/A") if view_node.find("t:location", ns) is not None else "N/A",
                                    "type": view_node.find("t:location", ns).attrib.get("type", "N/A") if view_node.find("t:location", ns) is not None else "N/A"
                                },
                                "tags": {}
                            },
                            "label": favorite.attrib.get("label", "N/A"),
                            "position": favorite.attrib.get("position", "N/A"),
                            "addedAt": favorite.attrib.get("addedAt", "N/A")
                        }
                        user_favorites.append(favorite_data)

            if user_favorites:
                favorites_data["favorites"]["favorite"].extend(user_favorites)

        logging.info(f"✅ {len(favorites_data.get('favorites', {}).get('favorite', []))} Favorites processed successfully.")
        return favorites_data

    except requests.exceptions.RequestException as e:
        logging.error(f"❌ Error listing favorites: {e}")
        raise
    except ET.ParseError as e:
        logging.error(f"❌ Error parsing XML response when listing favorites: {e}. Response text: {response.text}")
        raise
    except Exception as e:
        logging.error(f"❌ Unexpected error listing favorites: {e}")
        raise


def list_subscriptions(auth_token, SITE_ID):
    """
    Lists all subscriptions in the Tableau Cloud site.

    Args:
        auth_token (str): The authentication token for Tableau Cloud.
        SITE_ID (str): The ID of the Tableau Cloud site.

    Returns:
        list: A list of dictionaries containing subscription information.
              Raises an exception if the API request fails or the response is invalid.
    """
    url = f"{TABLEAU_BASE_URL}/sites/{SITE_ID}/subscriptions"
    headers = {"X-Tableau-Auth": auth_token}

    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        logging.info(f"🔍 Checking Subscriptions: {response.status_code}")

        subscriptions = []
        ns = {"t": "http://tableau.com/api"}
        root = ET.fromstring(response.text)
        subscriptions_node = root.find("t:subscriptions", ns)

        if subscriptions_node is not None:
            for subscription in subscriptions_node.findall("t:subscription", ns):

                content_node = subscription.find("t:content", ns)
                content = {
                    "id": content_node.attrib.get("id", "N/A") if content_node is not None else "N/A",
                    "type": content_node.attrib.get("type", "N/A") if content_node is not None else "N/A",
                    "sendIfViewEmpty": content_node.attrib.get("sendIfViewEmpty", "false") if content_node is not None else "false"
                }

                schedule_node = subscription.find("t:schedule", ns)
                frequency_details_node = schedule_node.find("t:frequencyDetails", ns) if schedule_node is not None else None
                intervals_node = frequency_details_node.find("t:intervals", ns) if frequency_details_node is not None else None

                intervals = []
                if intervals_node is not None:
                    for interval in intervals_node.findall("t:interval", ns):
                        interval_data = {}
                        if "hours" in interval.attrib:
                            interval_data["hours"] = interval.attrib["hours"]
                        if "weekDay" in interval.attrib:
                            interval_data["weekDay"] = interval.attrib["weekDay"]
                        intervals.append(interval_data)

                schedule = {
                    "frequency": schedule_node.attrib.get("frequency", "N/A") if schedule_node is not None else "N/A",
                    "nextRunAt": schedule_node.attrib.get("nextRunAt", "N/A") if schedule_node is not None else "N/A",
                    "frequencyDetails": {
                        "intervals": {"interval": intervals},
                        "start": frequency_details_node.attrib.get("start", "N/A") if frequency_details_node is not None else "N/A",
                        "end": frequency_details_node.attrib.get("end", "N/A") if frequency_details_node is not None else "N/A"
                    }
                }

                user_node = subscription.find("t:user", ns)
                user = {
                    "id": user_node.attrib.get("id", "N/A") if user_node is not None else "N/A",
                    "name": user_node.attrib.get("name", "N/A") if user_node is not None else "N/A"
                }

                subscription_data = {
                    "id": subscription.attrib.get("id", "N/A"),
                    "subject": subscription.attrib.get("subject", "N/A"),
                    "message": subscription.attrib.get("message", "N/A"),
                    "attachImage": subscription.attrib.get("attachImage", "false"),
                    "attachPdf": subscription.attrib.get("attachPdf", "false"),
                    "suspended": subscription.attrib.get("suspended", "false"),
                    "content": content,
                    "schedule": schedule,
                    "user": user
                }

                logging.info(f"📌 Found Subscription: {subscription_data['subject']} (ID: {subscription_data['id']})")
                subscriptions.append(subscription_data)

        logging.info(f"✅ {len(subscriptions)} Subscriptions processed successfully.")
        return subscriptions

    except requests.exceptions.RequestException as e:
        logging.error(f"❌ Error listing subscriptions: {e}")
        raise
    except ET.ParseError as e:
        logging.error(f"❌ Error parsing XML response when listing subscriptions: {e}. Response text: {response.text}")
        raise
    except Exception as e:
        logging.error(f"❌ Unexpected error listing subscriptions: {e}")
        raise


def save_subscriptions_json(subscriptions, file_path):
    """
    Saves subscription data to a JSON file.

    Args:
        subscriptions (list): The list of subscription dictionaries.
        file_path (str): The path to the file.

    Raises:
        IOError: If there's an error writing to the file.
    """
    data = {"subscriptions": {"subscription": subscriptions}}
    try:
        with open(file_path, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=4, ensure_ascii=False)
        logging.info(f"✅ Subscriptions JSON saved: {file_path}")
    except IOError as e:
        logging.error(f"❌ Error saving Subscriptions to JSON: {e}")
        raise


def list_custom_views(auth_token, SITE_ID):
    """
    Lists all custom views in the Tableau Cloud site.

    Args:
        auth_token (str): The authentication token for Tableau Cloud.
        SITE_ID (str): The ID of the Tableau Cloud site.

    Returns:
        list: A list of dictionaries containing custom view information.
              Raises an exception if the API request fails or the response is invalid.
    """
    url = f"{TABLEAU_BASE_URL}/sites/{SITE_ID}/customviews"
    headers = {"X-Tableau-Auth": auth_token}

    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        logging.info(f"🔍 Checking Custom Views: {response.status_code}")

        custom_views = []
        ns = {"t": "http://tableau.com/api"}
        root = ET.fromstring(response.text)
        custom_views_node = root.find("t:customViews", ns)

        if custom_views_node is not None:
            for custom_view in custom_views_node.findall("t:customView", ns):

                view_node = custom_view.find("t:view", ns)
                view = {
                    "id": view_node.attrib.get("id", "N/A") if view_node is not None else "N/A",
                    "name": view_node.attrib.get("name", "N/A") if view_node is not None else "N/A"
                }

                workbook_node = custom_view.find("t:workbook", ns)
                workbook = {
                    "id": workbook_node.attrib.get("id", "N/A") if workbook_node is not None else "N/A",
                    "name": workbook_node.attrib.get("name", "N/A") if workbook_node is not None else "N/A"
                }

                owner_node = custom_view.find("t:owner", ns)
                owner = {
                    "id": owner_node.attrib.get("id", "N/A") if owner_node is not None else "N/A",
                    "name": owner_node.attrib.get("name", "N/A") if owner_node is not None else "N/A"
                }

                custom_view_data = {
                    "id": custom_view.attrib.get("id", "N/A"),
                    "name": custom_view.attrib.get("name", "N/A"),
                    "createdAt": custom_view.attrib.get("createdAt", "N/A"),
                    "updatedAt": custom_view.attrib.get("updatedAt", "N/A"),
                    "lastAccessedAt": custom_view.attrib.get("lastAccessedAt", "N/A"),
                    "shared": custom_view.attrib.get("shared", "false"),
                    "view": view,
                    "workbook": workbook,
                    "owner": owner
                }

                logging.info(f"📌 Found Custom View: {custom_view_data['name']} (ID: {custom_view_data['id']})")
                custom_views.append(custom_view_data)

        logging.info(f"✅ {len(custom_views)} Custom Views processed successfully.")
        return custom_views

    except requests.exceptions.RequestException as e:
        logging.error(f"❌ Error listing custom views: {e}")
        raise
    except ET.ParseError as e:
        logging.error(f"❌ Error parsing XML response when listing custom views: {e}. Response text: {response.text}")
        raise
    except Exception as e:
        logging.error(f"❌ Unexpected error listing custom views: {e}")
        raise


def save_custom_views_json(custom_views, file_path):
    """
    Saves custom view data to a JSON file.

    Args:
        custom_views (list): The list of custom view dictionaries.
        file_path (str): The path to the file.

    Raises:
        IOError: If there's an error writing to the file.
    """
    data = {"customViews": {"customView": custom_views}}
    try:
        with open(file_path, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=4, ensure_ascii=False)
        logging.info(f"✅ Custom Views JSON saved: {file_path}")
    except IOError as e:
        logging.error(f"❌ Error saving Custom Views to JSON: {e}")
        raise


def list_virtual_connections(auth_token, SITE_ID):
    """
    Lists all virtual connections in the Tableau Cloud site.

    Args:
        auth_token (str): The authentication token for Tableau Cloud.
        SITE_ID (str): The ID of the Tableau Cloud site.

    Returns:
        list: A list of dictionaries containing virtual connection information.
              Raises an exception if the API request fails or the response is invalid.
    """
    headers = {"X-Tableau-Auth": auth_token}
    url = f"{TABLEAU_BASE_URL}/sites/{SITE_ID}/virtualconnections"

    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        logging.info(f"🔍 Checking Virtual Connections: {response.status_code}")

        virtual_connections = []
        ns = {"t": "http://tableau.com/api"}
        root = ET.fromstring(response.text)
        vc_list = root.find("t:virtualConnections", ns)

        if vc_list is not None:
            for vc in vc_list.findall("t:virtualConnection", ns):
                vc_id = vc.attrib.get("id", "N/A")
                vc_name = vc.attrib.get("name", "N/A")

                detail_url = f"{TABLEAU_BASE_URL}/sites/{SITE_ID}/virtualconnections/{vc_id}"
                detail_response = requests.get(detail_url, headers=headers)
                detail_response.raise_for_status()
                logging.info(f"🔧 Getting details for Virtual Connection {vc_name}: {detail_response.status_code}")

                detail_root = ET.fromstring(detail_response.text)
                vc_node = detail_root.find("t:virtualConnection", ns)

                if vc_node is not None:
                    vc_data = {
                        "id": vc_id,
                        "name": vc_name,
                        "project": {
                            "id": vc_node.find("t:project", ns).attrib.get("id", "N/A")
                            if vc_node.find("t:project", ns) is not None else "N/A"
                        },
                        "owner": {
                            "id": vc_node.find("t:owner", ns).attrib.get("id", "N/A")
                            if vc_node.find("t:owner", ns) is not None else "N/A"
                        },
                        "content": vc_node.find("t:content", ns).text
                        if vc_node.find("t:content", ns) is not None else ""
                    }
                    virtual_connections.append(vc_data)
                    logging.info(f"📌 Saved VC: {vc_name} (ID: {vc_id})")
                else:
                    logging.warning(f"⚠️ No virtualConnection node found for {vc_name} (ID: {vc_id})")

        logging.info(f"✅ {len(virtual_connections)} Virtual Connections processed successfully.")
        return virtual_connections

    except requests.exceptions.RequestException as e:
        logging.error(f"❌ Error listing virtual connections: {e}")
        raise
    except ET.ParseError as e:
        logging.error(f"❌ Error parsing XML response when listing virtual connections: {e}. Response text: {response.text}")
        raise
    except Exception as e:
        logging.error(f"❌ Unexpected error listing virtual connections: {e}")
        raise


def save_virtual_connections_json(virtual_connections, file_path):
    """
    Saves virtual connection data to a JSON file.

    Args:
        virtual_connections (list): The list of virtual connection dictionaries.
        file_path (str): The path to the file.

    Raises:
        IOError: If there's an error writing to the file.
    """
    data = {"virtualConnections": {"virtualConnection": virtual_connections}}
    try:
        with open(file_path, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=4, ensure_ascii=False)
        logging.info(f"✅ Virtual Connections JSON saved: {file_path}")
    except IOError as e:
        logging.error(f"❌ Error saving Virtual Connections to JSON: {e}")
        raise


def run_metadata_backup(backup_folder):
    """
    Orchestrates the backup of Tableau Cloud metadata.

    This function authenticates with Tableau Cloud, retrieves metadata
    information for various resources (users, groups, projects, etc.),
    saves this information to JSON files, and uploads the files to S3.

    Args:
        backup_folder (str): The base folder in the S3 bucket where the backup
                             will be stored.

    Returns:
        dict: A dictionary containing the status code and a message indicating
              the success or failure of the backup, along with S3 paths to the
              saved JSON files.

        Raises:
            Exception: If any error occurs during the backup process.
    """
    try:
        auth_token = authenticate()
        
        backup_folder_metadata = f"{backup_folder}metadata_tableau/"

        users_data = list_users(auth_token, SITE_ID)
        users_list = users_data.get("users", {}).get("user", [])
        users_json_path = "/tmp/users_roles.json"
        save_json(users_data, users_json_path, "users")

        groups = list_groups(auth_token, SITE_ID)
        groups_json_path = "/tmp/groups.json"
        save_json(groups, groups_json_path, "groups")

        projects = list_projects(auth_token, SITE_ID)
        projects_json_path = "/tmp/projects.json"
        save_json(projects, projects_json_path, "projects")

        workbooks = list_workbooks(auth_token, SITE_ID)
        workbooks_json_path = "/tmp/workbooks.json"
        save_json(workbooks, workbooks_json_path, "workbooks")

        datasources = list_datasources(auth_token, SITE_ID)
        datasources_json_path = "/tmp/datasources.json"
        save_json(datasources, datasources_json_path, "datasources")

        flows = list_flows(auth_token, SITE_ID)
        flows_json_path = "/tmp/flows.json"
        save_json(flows, flows_json_path, "flows")

        favorites = list_favorites(auth_token, SITE_ID, users_list)
        favorites_json_path = "/tmp/favorites.json"
        save_json(favorites, favorites_json_path, "favorites")

        subscriptions = list_subscriptions(auth_token, SITE_ID)
        subscriptions_json_path = "/tmp/subscriptions.json"
        save_json(subscriptions, subscriptions_json_path, "subscriptions")

        custom_views = list_custom_views(auth_token, SITE_ID)
        custom_views_json_path = "/tmp/custom_views.json"
        save_json(custom_views, custom_views_json_path, "custom_views")

        extract_tasks = list_extract_refresh_tasks(auth_token, SITE_ID)
        extract_tasks_json_path = "/tmp/extract_tasks.json"
        save_json(extract_tasks, extract_tasks_json_path, "extract_tasks")

        virtual_connections = list_virtual_connections(auth_token, SITE_ID)
        virtual_connections_json_path = "/tmp/virtual_connections.json"
        save_json(virtual_connections, virtual_connections_json_path, "virtual_connections")

        s3 = boto3.client("s3")
        s3.upload_file(users_json_path, S3_BUCKET, f"{backup_folder_metadata}users_roles.json")
        s3.upload_file(groups_json_path, S3_BUCKET, f"{backup_folder_metadata}groups.json")
        s3.upload_file(projects_json_path, S3_BUCKET, f"{backup_folder_metadata}projects.json")
        s3.upload_file(workbooks_json_path, S3_BUCKET, f"{backup_folder_metadata}workbooks.json")
        s3.upload_file(datasources_json_path, S3_BUCKET, f"{backup_folder_metadata}datasources.json")
        s3.upload_file(flows_json_path, S3_BUCKET, f"{backup_folder_metadata}flows.json")
        s3.upload_file(favorites_json_path, S3_BUCKET, f"{backup_folder_metadata}favorites.json")
        s3.upload_file(subscriptions_json_path, S3_BUCKET, f"{backup_folder_metadata}subscriptions.json")
        s3.upload_file(custom_views_json_path, S3_BUCKET, f"{backup_folder_metadata}custom_views.json")
        s3.upload_file(extract_tasks_json_path, S3_BUCKET, f"{backup_folder_metadata}extract_tasks.json")
        s3.upload_file(virtual_connections_json_path, S3_BUCKET, f"{backup_folder_metadata}virtual_connections.json")

        return {
            "statusCode": 200,
            "body": json.dumps({
                "message": "✅ Backup completed for metadata",
                "users_json": f"s3://{S3_BUCKET}/{backup_folder_metadata}users_roles.json",
                "groups": f"s3://{S3_BUCKET}/{backup_folder_metadata}groups.json",
                "projects": f"s3://{S3_BUCKET}/{backup_folder_metadata}projects.json",
                "workbooks": f"s3://{S3_BUCKET}/{backup_folder_metadata}workbooks.json",
                "datasources": f"s3://{S3_BUCKET}/{backup_folder_metadata}datasources.json",
                "flows": f"s3://{S3_BUCKET}/{backup_folder_metadata}flows.json",
                "favorites": f"s3://{S3_BUCKET}/{backup_folder_metadata}favorites.json",
                "subscriptions": f"s3://{S3_BUCKET}/{backup_folder_metadata}subscriptions.json",
                "custom_views": f"s3://{S3_BUCKET}/{backup_folder_metadata}custom_views.json",
                "extract_tasks": f"s3://{S3_BUCKET}/{backup_folder_metadata}extract_tasks.json",
                "virtual_connections": f"s3://{S3_BUCKET}/{backup_folder_metadata}virtual_connections.json"
            })
        }

    except Exception as e:
        return {
            "statusCode": 500,
            "body": json.dumps(f"❌ Error: {str(e)}")
        }