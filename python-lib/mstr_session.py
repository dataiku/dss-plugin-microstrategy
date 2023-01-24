import logging
import requests
import json
import pandas
from base64 import b64encode
from mstr_auth import MstrAuth


logging.basicConfig(level=logging.INFO, format='dss-plugin-microstrategy %(levelname)s - %(message)s')
logger = logging.getLogger()


class MstrSession(object):
    def __init__(self, server_url, username, password):
        self.server_url = parse_server_url(server_url)
        self.username = username
        self.password = password
        self.auth = None
        self.requests_verify = False
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

    def update_dataset(self, rows, project_id, dataset_id, table_name, schema, dss_columns_types, update_policy='replace'):
        url = "{}/datasets/{}/tables/{}".format(self.server_url, dataset_id, table_name)
        headers = self.build_headers(project_id, update_policy=update_policy)
        json = self.build_table_update_json(table_name, schema, dss_columns_types, rows)
        response = self.patch(url=url, headers=headers, json=json)
        assert_response_ok(response)
        return response

    def get_project_list(self):
        url = "{}/projects".format(self.server_url)
        response = self.get(url=url)
        assert_response_ok(response)
        projects_list = safe_json_extract(response, default=[])
        return projects_list

    def search_cubes(self, project_id, dataset_name):
        url = "{}/searches/results".format(self.server_url)
        response = self.get(
            url=url,
            headers=self.build_headers(project_id),
            params={
                "name": dataset_name,
                "type": 3
            }
        )
        assert_response_ok(response)
        search_results = safe_json_extract(response)
        return search_results

    def create_dataset(self, project_id, project_name, dataset_name, table_name, columns_names, columns_types):
        json = self.build_dataset_create_json(dataset_name, table_name, columns_names, columns_types, [])
        url = "{}/datasets".format(self.server_url)
        headers = self.build_headers(project_id)
        response = self.post(url=url, headers=headers, json=json)
        assert_response_ok(response)
        json_response = safe_json_extract(response)
        datatset_id = json_response.get("datasetId")
        return datatset_id

    def build_dataset_create_json(self, project_name, table_name, columns_names, columns_types, rows):
        table_dictionary = self.build_table_update_json(table_name, columns_names, columns_types, rows)

        attributes, metrics = self.build_attributes_and_metrics(table_name, columns_names, columns_types)
        dataset_dictionary = {
            "name": project_name,
            "tables": [table_dictionary],
            "attributes": attributes,
            "metrics": metrics
        }
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
                    item_value = None
                else:
                    item_value = item_value.strftime("%Y-%m-%dT%H:%M:%S.%fZ")
            elif item_type == "boolean":
                pass
            mstr_row[item_key] = item_value
        output_rows.append(mstr_row)
    encoded_rows = b64encode(json.dumps(output_rows, separators=(',', ':')).encode('utf-8')).decode("utf-8")
    return encoded_rows


def assert_response_ok(response, can_raise=True):
    error_message = ""
    if not isinstance(response, requests.models.Response):
        error_message = "Did not return a valide response"
    else:
        status_code = response.status_code
        if status_code >= 400:
            error_message = "Error {}".format(status_code)
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
