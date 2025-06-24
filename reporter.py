from elasticsearch import Elasticsearch
from config import configReader as configreader
from abc import ABC,  abstractmethod
import os
import unittest
import urllib3
from datetime import datetime
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
import json
import logging
logging.basicConfig()
logger = logging.getLogger()
logger.setLevel(logging.INFO)

class connector(ABC):
    """Abstract class used to implement different connectors"""
    @abstractmethod
    def get_data_prev_month(self, start_date: str, end_date: str, query_file: str) -> None:
        pass

class ElasticConnector(connector):
    def __init__(self, config_dict: dict):
        """
        Constructor of the elastic connector class which abstracts the classes offered by the elasticsearch module
        adding a way to pass parameters via a dict object generated from a yaml config file
        :param config_dict: dict -> dictionary containing the parameters to be used
        """
        self.data = None
        self.index = config_dict['query_config']['index']
        self.elasticURL = '%s://%s:%s@%s:%s/%s' % (
        config_dict['elastic_client_config']['elastic_protocol'],
        config_dict['elastic_client_config']['elasticUser'],
        os.getenv("ELASTIC_PASSWORD"),
        config_dict['elastic_client_config']['elastichost'],
        config_dict['elastic_client_config']['elasticport'],
        config_dict['elastic_client_config']['elasticPrefix'])
        self.elasticclient = Elasticsearch([self.elasticURL],verify_certs=False)

    def get_data_prev_month(self, start_date: str, end_date: str, query_file: str) -> list:
        """
        Method that gets the data from elasticsearch
        start_date:[str] start date of the query
        end_date[str]: end date of the query
        query_file[str]: path to the query file to be used. The query file stores the data in json format

        :return: None
        """
        with open(query_file) as f:
            payload = json.load(f)
            # TO DO override the values in the query after the data is imported with correct timestamp
        previousMonth: str = str(int(datetime.now().strftime("%Y%m")) - 1) + "01000000"
        actualMonth: str = datetime.now().strftime("%Y%m") + "01000000"
        self.data = self.elasticclient.search(
                    index=self.index,
                    request_timeout=600,
                    body=payload
        )
        return self.data['hits']['hits']

class QueryConstructor(ABC):
    @abstractmethod
    def construct_query(self) -> dict:
        pass

class ElasticQueryConstructor(QueryConstructor):
    """Class that constructs the queries based on the paths and files specified in the config"""
    def __init__(self, base_path: str, query_files: list):
        """
        Constructor of the QueryConstructor class.
        :param base_path[str]: base path for the files that contain the queries.
        :param query_files[str]: query files names
        """
        self.base_path = base_path
        self.query_files = query_files

    def construct_query(self):
        query_dict: dict = {}
        for item in self.query_files:
            file_name = item + ".json"
            file_path = os.path.join(self.base_path, file_name)
            query_dict[item] = file_path
        return query_dict


def main() -> None:
    """Main function calling all other functions"""
    configObject: configreader = configreader()
    configObject.generateConfigObject()
    configuration: dict = configObject.configObject
    elastic_query_base_path: str = configuration["query_config"]["base_query_path"]
    elastic_queries: list = configuration["query_config"]["query_list"]
    elastic_query_dict = ElasticQueryConstructor(elastic_query_base_path, elastic_queries).construct_query()
    elastic_connector = ElasticConnector(configuration)
    for item in elastic_query_dict.items():
        query_name = item[0]
        query_path = item[1]
        log_message = f"Running query for {query_name}. Using file in {query_path}"
        logger.info(log_message)
        sms_data = elastic_connector.get_data_prev_month(start_date="", end_date="", query_file=query_path)
        log_message = f"Retrieved data for {query_name}. Start processing..."
        logger.info(log_message)
        for record in sms_data:
            print(record["fields"])

if __name__ == '__main__':
    main()