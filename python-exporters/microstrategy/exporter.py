# This file is the actual code for the custom Python exporter mstrio
import dataiku
from dataiku.exporter import Exporter
from dataiku.exporter import SchemaHelper
import os
import pandas as pd
import numpy as np
import logging

from mstrio import microstrategy
import requests


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
        logging.basicConfig(level=logging.INFO, format='dss-plugin-microstrategy %(levelname)s - %(message)s')
        self.logger = logging.getLogger()

        self.config = config
        self.plugin_config = plugin_config

        self.row_buffer = []
        self.mstr_buffer_size = 5000

        # Plugin settings
        self.base_url = self.plugin_config.get("base_url",None)

        self.project_name = self.config["MicroStrategy Project"].get("project_name",None)
        self.project_id = "" # the project id, obtained through a later request
        self.dataset_name = str(self.config.get("dataset_name",None)).replace(" (created by Dataiku DSS)", "") + " (created by Dataiku DSS)"
        self.dataset_id = "" # the dataset id, obtained at creation or on update
        #self.table_name = self.config.get("table_name",None)
        self.table_name = "dss_data"

        self.username = self.config["MicroStrategy API"].get("username", None)
        self.password = self.config["MicroStrategy API"].get("password",'')

        if not (self.username and self.password and self.base_url):
            self.logger.error('Connection params: {}'.format(
                {'username:' : self.username,
                'password:' : '#' * len(self.password),
                'base_url:' : self.base_url})
            )
            raise ValueError("username, password and base_url must be filled")

        self.conn = microstrategy.Connection(base_url=self.base_url, username=self.username, password=self.password, project_name=self.project_name)


    def open(self, schema):
        ## This is a much better way of doing things, but the mstrio-py does not handle all type casts
        ## Copied on what the exporter does already, too bad we can't reuse the object:
        (self.schema, dtypes, parse_dates_columns) = dataiku.Dataset.get_dataframe_schema_st(schema["columns"])

        # The exporter has this ad hoc code to prevent problems when reading int
        # If we don't use it too, the initialization of the cube does not have the same dtypes as the read data (in write_row) and we get mismatchs when updating
        if dtypes is not None:
            new_dtypes = {}
            for (k, v) in dtypes.items():
                if v == np.int64 or v == np.int32:
                    v = np.float64
                # if v == np.float32:
                #     v = np.float64
                new_dtypes[k] = v
            dtypes = new_dtypes

        self.logger.info("Will create mstr dataset with dtypes = %s" % dtypes)
        self.dataframe = pd.DataFrame({k: pd.Series(dtype=dtypes[k]) for k in dtypes})


        self.logger.info('Opening connection to mstr')
        try:
            self.conn.connect()
        except Exception as e:
            self.logger.exception("Connection to MicroStrategy failed.")
            raise e

        # Get a project list, search for our project in the list, get the project ID for future API calls.
        r = requests.get(url = self.base_url+"/projects", headers = {"X-MSTR-AuthToken": self.conn.auth_token}, cookies= self.conn.cookies)
        r.raise_for_status()
        projects_list = r.json()

        for project in projects_list:
            if project["name"] == self.project_name:
                self.project_id = project["id"]
                break

        # Search for objects of type 3 (datasets/cubes) with the right name
        r = requests.get(url = self.base_url+"/searches/results", headers = {"X-MSTR-AuthToken": self.conn.auth_token, "X-MSTR-ProjectID": self.project_id}, cookies = self.conn.cookies, params = {"name": self.dataset_name, "type": 3})
        r.raise_for_status()
        search_results = r.json()

        # No result, create a new dataset
        if search_results["totalItems"] == 0:
            try:
                self.dataset_id, newTableId = self.conn.create_dataset(data_frame=self.dataframe, dataset_name=self.dataset_name, table_name=self.table_name)
            except Exception as e:
                self.logger.exception("Dataset creation issue:")
                raise e
        # Found exactly 1 result, fetch the dataset ID
        elif search_results["totalItems"] == 1:
            self.dataset_id = search_results["result"][0]["id"]
        # Found more than 1 cube, fail
        else:
            raise RuntimeError('Found two datasets named {} on your MicroStrategy instance.'.format(self.dataset_name))

        # Replace data (drop existing) by sending the empty dataframe, with correct schema
        self.conn.update_dataset(data_frame=self.dataframe, dataset_id=self.dataset_id, table_name=self.table_name, update_policy='replace')


    def write_row(self, row):
        row_obj = {}
        for (col, val) in zip(self.schema, row):
            row_obj[col] = val
        self.row_buffer.append(row_obj)

        if len(self.row_buffer) > self.mstr_buffer_size:
            self.logger.info("Sending 5000 rows to MicroStrategy.")
            self.dataframe = pd.DataFrame(self.row_buffer)
            try:
                self.conn.update_dataset(data_frame=self.dataframe, dataset_id=self.dataset_id, table_name=self.table_name, update_policy='add')
            except Exception as e:
                self.logger.exception("Dataset update issue:")
                raise e
            self.row_buffer = []


    def close(self):
        self.logger.info("Sending {} final rows to MicroStrategy.".format(len(self.row_buffer)))
        self.dataframe = pd.DataFrame(self.row_buffer)
        self.conn.update_dataset(data_frame=self.dataframe, dataset_id=self.dataset_id, table_name=self.table_name, update_policy='add')

        self.logger.info("Logging out.")
        r = requests.get(url = self.base_url+"/auth/logout", headers = {"X-MSTR-AuthToken": self.conn.auth_token}, cookies= self.conn.cookies)
        self.logger.info("Logout returned status {}".format(r.status_code))
