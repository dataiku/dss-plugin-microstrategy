import logging
from numpy import isnan
from mstr_session import MstrSession

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
        logger.info("Starting MicroStrategy exporter v1.1.1-beta.2")
        # Plugin settings
        self.base_url = plugin_config.get("base_url", None)
        self.project_name = config["microstrategy_project"].get("project_name", None)
        self.project_id = ""  # the project id, obtained through a later request
        self.dataset_name = str(config.get("dataset_name", None)).replace(" (created by Dataiku DSS)", "") + " (created by Dataiku DSS)"
        self.dataset_id = ""  # the dataset id, obtained at creation or on update
        self.table_name = "dss_data"
        self.username = config["microstrategy_api"].get("username", None)
        self.password = config["microstrategy_api"].get("password", '')
        self.session = MstrSession(self.base_url, self.username, self.password)
        self.project_id, self.folder_id = self.get_ui_browse_results(config)

        if not (self.username and self.base_url):
            logger.error('Connection params: {}'.format(
                {
                    'username:': self.username,
                    'password:': '#' * len(self.password),
                    'base_url:': self.base_url
                })
            )
            raise ValueError("username and base_url must be filled")

    def get_ui_browse_results(self, config):
        import json
        folder_id = None
        project_id = config.get("selected_project_id", None)
        selected_folder_id = json.loads(config.get("selected_folder_id", "{}"))
        folder_ids = selected_folder_id.get("ids")
        if folder_ids:
            folder_id = folder_ids[-1]
        return project_id, folder_id

    def open(self, schema):
        self.dss_columns_types = get_dss_columns_types(schema)
        (self.schema, dtypes, parse_dates_columns) = dataiku.Dataset.get_dataframe_schema_st(schema["columns"])

        # Prevent problems when reading int
        # If we don't use it too, the initialization of the cube does not have the same dtypes as the read data (in write_row)
        # and we get mismatchs when updating
        # if dtypes is not None:

        # Get a project list, search for our project in the list, get the project ID for future API calls.
        if not self.project_id:
            self.project_id = self.session.get_project_id(self.project_name)

        # Search for objects of type 3 (datasets/cubes) with the right name
        logger.info("Searching for existing '{}' dataset in project '{}'.".format(self.dataset_name, self.project_id))
        self.dataset_id = self.session.get_dataset_id(self.project_id, self.dataset_name)

        # No result, create a new dataset
        if not self.dataset_id:
            logger.info("Creating dataset '{}'".format(self.dataset_name))
            try:
                self.dataset_id = self.session.create_dataset(
                    self.project_id,
                    self.project_name,
                    self.dataset_name,
                    self.table_name,
                    self.schema,
                    self.dss_columns_types,
                    self.folder_id
                )
            except Exception as error_message:
                logger.exception("Dataset creation issue: {}".format(error_message))
                raise error_message

        # Replace data (drop existing) by sending the empty dataframe, with correct schema
        self.session.update_dataset([], self.project_id, self.dataset_id, self.table_name, self.schema, self.dss_columns_types, update_policy='replace')

    def write_row(self, row):
        row_dict = {}
        for (column_name, cell_value, dtype) in zip(self.schema, row, self.dss_columns_types):
            if (type(cell_value) == float and isnan(cell_value)):
                cell_value = None
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
        response = self.session.get(url=self.base_url+"/auth/logout")
        logger.info("Logout returned status {}".format(response.status_code))

    def flush_data(self, rows):
        try:
            self.session.update_dataset(
                rows,
                self.project_id,
                self.dataset_id,
                self.table_name,
                self.schema,
                self.dss_columns_types,
                update_policy='add')
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
