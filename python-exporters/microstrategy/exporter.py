import logging
import requests
import pandas as pd
import numpy as np

from mstrio import microstrategy

import dataiku
from dataiku.exporter import Exporter


logging.basicConfig(level=logging.INFO, format='dss-plugin-microstrategy %(levelname)s - %(message)s')
logger = logging.getLogger()


class CustomExporter(Exporter):
    """
    The methods will be called like this:
       __init__
       open
       write_row
       write_row
       write_row
       ...
       write_row
       close
    """

    def __init__(self, config, plugin_config):
        """
        :param config: the dict of the configuration of the object
        :param plugin_config: contains the plugin settings
        """
        self.row_buffer = []
        self.buffer_size = 5000
        logger.info("Starting MicroStrategy exporter v1.1.1-beta.3")
        # Plugin settings
        self.base_url = plugin_config.get("base_url", None)
        self.project_name = config["microstrategy_project"].get("project_name", None)
        self.project_id = ""  # the project id, obtained through a later request
        self.dataset_name = str(config.get("dataset_name", None)).replace(" (created by Dataiku DSS)", "") + " (created by Dataiku DSS)"
        self.dataset_id = ""  # the dataset id, obtained at creation or on update
        self.table_name = "dss_data"
        self.username = config["microstrategy_api"].get("username", None)
        self.password = config["microstrategy_api"].get("password", '')

        if not (self.username and self.base_url):
            logger.error('Connection params: {}'.format(
                {
                    'username:': self.username,
                    'password:': '#' * len(self.password),
                    'base_url:': self.base_url
                })
            )
            raise ValueError("username and base_url must be filled")

        self.connection = microstrategy.Connection(base_url=self.base_url, username=self.username, password=self.password, project_name=self.project_name)

    def open(self, schema):
        self.dss_columns_types = get_dss_columns_types(schema)
        (self.schema, dtypes, parse_dates_columns) = dataiku.Dataset.get_dataframe_schema_st(schema["columns"])

        # Prevent problems when reading int
        # If we don't use it too, the initialization of the cube does not have the same dtypes as the read data (in write_row)
        # and we get mismatchs when updating
        if dtypes is not None:
            new_dtypes = {}
            for (key, value) in dtypes.items():
                if value == np.int64 or value == np.int32:
                    value = np.float64
                new_dtypes[key] = value
            dtypes = new_dtypes

        if parse_dates_columns:
            for date_column in parse_dates_columns:
                date_column_name = self.schema[date_column]
                dtypes[date_column_name] = 'datetime64[ns]'

        logger.info("Will create mstr dataset with dtypes = %s" % dtypes)
        self.dataframe = pd.DataFrame({k: pd.Series(dtype=dtypes[k]) for k in dtypes})

        logger.info('Opening connection to mstr')
        try:
            self.connection.connect()
        except Exception as error_message:
            logger.exception("Connection to MicroStrategy failed.")
            raise error_message

        # Get a project list, search for our project in the list, get the project ID for future API calls.
        response = requests.get(url=self.base_url+"/projects", headers={"X-MSTR-AuthToken": self.connection.auth_token}, cookies=self.connection.cookies)
        response.raise_for_status()
        try:
            projects_list = response.json()
        except Exception as error_message:
            raise error_message
        for project in projects_list:
            if project["name"] == self.project_name:
                self.project_id = project["id"]
                break

        if not self.project_id:
            raise ValueError("Project '{}' could not be found on this server.".format(self.project_name))

        self.dataset_id = self.get_dataset_id(self.project_id, self.dataset_name)

        if not self.dataset_id:
            logger.info("Creating new dataset {}.".format(self.dataset_name))
            try:
                self.dataset_id, newTableId = self.connection.create_dataset(
                    data_frame=self.dataframe, dataset_name=self.dataset_name, table_name=self.table_name
                )
            except Exception as error_message:
                logger.exception("Dataset creation issue: {}".format(error_message))
                raise error_message

        # Replace data (drop existing) by sending the empty dataframe, with correct schema
        self.connection.update_dataset(data_frame=self.dataframe, dataset_id=self.dataset_id, table_name=self.table_name, update_policy='replace')

    def get_dataset_id(self, project_id, searched_dataset_name):
        dataset_id = None
        # Search for objects of type 3 (datasets/cubes) with the right name
        response = requests.get(
            url=self.base_url+"/searches/results",
            headers={"X-MSTR-AuthToken": self.connection.auth_token, "X-MSTR-ProjectID": project_id},
            cookies=self.connection.cookies, params={"type": 3}
        )
        response.raise_for_status()
        search_results = response.json()
        print("ALX:search_results={}".format(search_results))
        results = search_results.get("result", [])
        total_items = 0  # Keeping this for back compatibility, but can two datasets have the same name in a first place ?
        for result in results:
            dataset_name = result.get("name")
            if dataset_name == searched_dataset_name:
                dataset_id = result.get("id")
                total_items = total_items + 1
        if total_items > 1:
            raise RuntimeError('Found more than one datasets named {} on your MicroStrategy instance.'.format(searched_dataset_name))
        return dataset_id

    def write_row(self, row):
        row_dict = {}
        for (column_name, cell_value, dtype) in zip(self.schema, row, self.dss_columns_types):
            if (not cell_value or (type(cell_value) == float and np.isnan(cell_value))) and dtype == 'string':
                cell_value = ""
            if (type(cell_value) == float and np.isnan(cell_value)) and dtype == 'boolean':
                logger.error("Boolean column '{}' contains an empty cell".format(column_name))
                raise ValueError(
                    "There is an empty cell in the boolean column '{}'. ".format(column_name)
                    + "Boolean columns can only contain true/false values."
                )
            row_dict[column_name] = cell_value
        self.row_buffer.append(row_dict)

        if len(self.row_buffer) > self.buffer_size:
            logger.info("Sending 5000 rows to MicroStrategy.")
            self.flush_data(self.row_buffer)
            self.row_buffer = []

    def close(self):
        logger.info("Sending {} final rows to MicroStrategy.".format(len(self.row_buffer)))
        self.flush_data(self.row_buffer)
        logger.info("Logging out.")
        response = requests.get(url=self.base_url+"/auth/logout", headers={"X-MSTR-AuthToken": self.connection.auth_token}, cookies=self.connection.cookies)
        logger.info("Logout returned status {}".format(response.status_code))

    def flush_data(self, rows):
        self.dataframe = pd.DataFrame(rows)
        try:
            self.connection.update_dataset(data_frame=self.dataframe, dataset_id=self.dataset_id, table_name=self.table_name, update_policy='add')
        except Exception as error_message:
            logger.exception("Dataset update issue: {}".format(error_message))
            raise error_message


def get_dss_columns_types(schema):
    columns = schema.get("columns", [])
    columns_types = []
    for column in columns:
        column_type = column.get("type")
        columns_types.append(column_type)
    return columns_types
