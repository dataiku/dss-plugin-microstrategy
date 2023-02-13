import logging
import requests
import json
import pandas
from base64 import b64encode
from mstr_auth import MstrAuth
from dateutil.parser import parse


logging.basicConfig(level=logging.INFO, format='dss-plugin-microstrategy %(levelname)s - %(message)s')
logger = logging.getLogger()


SEARCH_PATTERN_EXACT = 2
OBJECT_TYPE_CUBE_DATASET = 3


class MstrSession(object):
    def __init__(self, server_url, username, password, generate_verbose_logs=False):
        self.server_url = parse_server_url(server_url)
        self.username = username
        self.password = password
        self.auth = None
        self.requests_verify = False
        self.generate_verbose_logs = generate_verbose_logs
        auth_token, cookies = self.request_auth_token(username, password)
        self.auth = MstrAuth(auth_token, cookies)

    def request_auth_token(self, username, password):
        url = "{}/auth/login".format(self.server_url)
        data = {
            "username": username,
            "password": password,
            "loginMode": 1
        }
        response = requests.post(url=url, data=data)
        auth_token = response.headers.get("X-MSTR-AuthToken")
        cookies = dict(response.cookies)
        return auth_token, cookies

    def get(self, url=None, headers=None, params=None):
        headers = headers or {}
        response = requests.get(url=url, headers=headers, params=params, verify=self.requests_verify, auth=self.auth)
        return response

    def post(self, url=None, headers=None, params=None, json=None):
        headers = headers or {}
        response = requests.post(url=url, json=json, headers=headers, params=params, verify=self.requests_verify, auth=self.auth)
        return response

    def patch(self, url=None, headers=None, json=None):
        headers = headers or {}
        response = requests.patch(url=url, headers=headers, json=json, verify=self.requests_verify, auth=self.auth)
        return response

    def update_dataset(self, rows, project_id, dataset_id, table_name, schema, dss_columns_types, update_policy='replace', can_raise=True):
        url = "{}/datasets/{}/tables/{}".format(self.server_url, dataset_id, table_name)
        headers = self.build_headers(project_id, update_policy=update_policy)
        json = self.build_table_update_json(table_name, schema, dss_columns_types, rows)
        response = self.patch(url=url, headers=headers, json=json)
        assert_response_ok(response, generate_verbose_logs=self.generate_verbose_logs, can_raise=can_raise)
        return response

    # def upload_multiple_rows(self, rows, project_id, dataset_id, table_name, schema, dss_columns_types):
    #     logger.info("upload_multiple_rows:Uploading {}".format(len(rows)))
    #     response = self.update_dataset(rows, project_id, dataset_id, table_name, schema, dss_columns_types, update_policy='add', can_raise=False)
    #     if response.status_code == 500 and "Mismatch columns" in response.content:
    #         logger.warning("Mismatch in column, reverting to row by row upload")
    #         for row in rows:
    #             response = self.update_dataset([row], project_id, dataset_id, table_name, schema, dss_columns_types, update_policy='add', can_raise=False)

    def get_project_list(self):
        url = "{}/projects".format(self.server_url)
        response = self.get(url=url)
        assert_response_ok(response, generate_verbose_logs=self.generate_verbose_logs)
        projects_list = safe_json_extract(response, default=[])
        return projects_list

    def get_dataset_id(self, project_id, searched_dataset_name, folder_id=None):
        dataset_id = None
        match_found = False

        url = "{}/searches/results".format(self.server_url)
        params = {
            "name": "{}".format(searched_dataset_name),
            "pattern": SEARCH_PATTERN_EXACT,
            "type": OBJECT_TYPE_CUBE_DATASET
        }
        if folder_id:
            params["root"] = folder_id
            params["getAncestors"] = True
        response = self.get(
            url=url,
            headers=self.build_headers(project_id),
            params=params
        )
        assert_response_ok(response, context="searching for dataset '{}'".format(searched_dataset_name), generate_verbose_logs=self.generate_verbose_logs)

        search_results = safe_json_extract(response)
        datasets = search_results.get("result", [])
        logger.info("Found {} datasets".format(len(datasets)))
        for dataset in datasets:
            dataset_name = dataset.get("name")
            if dataset_name == searched_dataset_name:
                if folder_id:
                    ancestor_id = self.get_ancestor_id(dataset)
                    if ancestor_id != folder_id:
                        continue
                dataset_id = dataset.get("id")
                if match_found:
                    raise Exception("Found more than one dataset named {} on your MicroStrategy instance".format(searched_dataset_name))
                else:
                    match_found = True

        return dataset_id

    def get_ancestor_id(self, dataset):
        ancestor_id = None
        ancestors = dataset.get("ancestors", [])
        if ancestors:
            ancestor = ancestors[-1]
            ancestor_id = ancestor.get("id")
        return ancestor_id

    def get_projects(self):
        url = "{}/projects".format(self.server_url)
        response = self.get(
            url=url
        )
        search_result = safe_json_extract(response)
        return search_result

    def get_project_id(self, project_name):
        projects_list = self.get_project_list()
        project_id = None
        for project in projects_list:
            if project["name"] == project_name:
                project_id = project["id"]
                return project_id

        if not project_id:
            raise ValueError("Project '{}' could not be found on this server.".format(project_name))

    def get_shared_folders(self, project_id):
        if not project_id:
            return []
        url = "{}/folders/preDefined/7".format(self.server_url)
        response = self.get(
            url=url,
            headers=self.build_headers(project_id)
        )
        search_result = safe_json_extract(response)
        return search_result

    def get_folder(self, project_id, parent_folder_id):
        if not project_id:
            return []
        url = "{}/folders/{}".format(self.server_url, parent_folder_id)
        response = self.get(
            url=url,
            headers=self.build_headers(project_id)
        )
        search_result = safe_json_extract(response)
        return search_result

    def create_dataset(self, project_id, project_name, dataset_name, table_name, columns_names, columns_types, folder_id=None):
        json = self.build_dataset_create_json(dataset_name, table_name, columns_names, columns_types, [], folder_id=folder_id)
        url = "{}/datasets".format(self.server_url)
        headers = self.build_headers(project_id)
        response = self.post(url=url, headers=headers, json=json)
        assert_response_ok(response, generate_verbose_logs=self.generate_verbose_logs)
        json_response = safe_json_extract(response)
        datatset_id = json_response.get("datasetId")
        return datatset_id

    def build_dataset_create_json(self, project_name, table_name, columns_names, columns_types, rows, folder_id=None):
        table_dictionary = self.build_table_update_json(table_name, columns_names, columns_types, rows)

        attributes, metrics = self.build_attributes_and_metrics(table_name, columns_names, columns_types)
        dataset_dictionary = {
            "name": project_name,
            "tables": [table_dictionary],
            "attributes": attributes,
            "metrics": metrics
        }
        if folder_id:
            dataset_dictionary["folderId"] = folder_id
        return dataset_dictionary

    def build_attributes_and_metrics(self, table_name, columns_names, columns_types):
        attributes = []
        metrics = []
        for column_name, column_type in zip(columns_names, columns_types):
            if convert_type(column_type) in ['INTEGER', 'DOUBLE']:
                metrics.append(self.build_metric(table_name, column_name))
            else:
                attributes.append(self.build_attribute(table_name, column_name))
        return attributes, metrics

    def build_attribute(self, table_name, column_name):
        return {
            "name": column_name,
            "attributeForms": [
                {
                    "category": "ID",
                    "expressions": [{
                        "formula": "{}.{}".format(table_name, column_name)
                    }]
                }
            ]
        }

    def build_metric(self, table_name, column_name):
        return {
            "name": column_name,
            "dataType": "number",
            "expressions": [{
                "formula": "{}.{}".format(table_name, column_name)
            }]
        }

    def build_table_update_json(self, table_name, columns_names, columns_types, rows):
        return {
            "name": table_name,
            "columnHeaders": self.get_column_header(columns_names, columns_types),
            "data": convert_rows_to_data(rows, columns_types)
        }

    def build_headers(self, project_id, update_policy=None):
        headers = {
            "X-MSTR-ProjectID": project_id,
        }
        if update_policy:
            headers["updatePolicy"] = update_policy
        return headers

    def get_column_header(self, columns_names, columns_types):
        mstr_columns = []
        for column_name, column_type in zip(columns_names, columns_types):
            mstr_column = {
                "name": convert_name(column_name),
                "dataType": convert_type(column_type)
            }
            mstr_columns.append(mstr_column)
        return mstr_columns


def convert_name(dss_name):
    mstr_name = dss_name
    return mstr_name


def convert_type(dss_type):
    DSS_TO_MSTR_TYPES = {
        "string": "STRING",
        "object": "STRING",
        "int": "DOUBLE",
        "bigint": "DOUBLE",
        "smallint": "DOUBLE",
        "tinyint": "DOUBLE",
        "float": "DOUBLE",
        "double": "DOUBLE",
        "boolean": "BOOL",
        "date": "DATETIME",
        "geopoint": "STRING",
        "geometry": "STRING",
        "array": "STRING",
        "map": "STRING",
        "object": "STRING"
    }
    DEFAULT_TYPE = "STRING"
    mstr_type = DSS_TO_MSTR_TYPES.get(dss_type, DEFAULT_TYPE)
    return mstr_type


def convert_rows_to_data(rows, columns_types):
    output_rows = []
    for row in rows:
        mstr_row = {}
        for item_key, item_type in zip(row, columns_types):
            item_value = row.get(item_key)
            value_type = type(item_value)
            if item_type == "string":
                if type(item_value) != str:
                    item_value = ""
            elif item_type == "date":
                if value_type != pandas._libs.tslibs.timestamps.Timestamp:
                    if not validate_date(item_value):
                        item_value = None
                else:
                    item_value = item_value.strftime("%Y-%m-%dT%H:%M:%S.%fZ")
            elif item_type == "boolean":
                pass
            mstr_row[item_key] = item_value
        output_rows.append(mstr_row)
    encoded_rows = b64encode(json.dumps(output_rows, separators=(',', ':')).encode('utf-8')).decode("utf-8")
    return encoded_rows


def assert_response_ok(response, context=None, can_raise=True, generate_verbose_logs=False):
    error_message = ""
    error_context = " while {} ".format(context) if context else ""
    if not isinstance(response, requests.models.Response):
        error_message = "Did not return a valide response"
    else:
        status_code = response.status_code
        if status_code >= 400:
            error_message = "Error {}{}".format(status_code, error_context)
            json_content = ""
            message = ""
            json_content = safe_json_extract(response, default={})
            message = json_content.get("message")
            content = response.content
            if message:
                error_message += ". " + message
            elif json_content:
                error_message += ". " + json_content
            logger.error(error_message)
            logger.error(content)
    if error_message and can_raise:
        if generate_verbose_logs:
            logger.error("last requests url={}, body={}".format(response.request.url, response.request.body))
        raise Exception(error_message)
    return error_message


def safe_json_extract(response, default=None):
    json = default
    try:
        json = response.json()
    except Exception as error_message:
        logging.error("Error '{}' while decoding json".format(error_message))
    return json


def parse_server_url(raw_url):
    return raw_url.strip("/")


def validate_date(date_text):
    try:
        parse(date_text)
    except Exception:
        return False
    return True
