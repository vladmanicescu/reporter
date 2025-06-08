from elasticsearch import Elasticsearch
from config import configReader as configreader
from abc import ABC,  abstractmethod
import os
import unittest
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
import json

class connector(ABC):
    """Abstract class used to implement different connectors"""
    @abstractmethod
    def get_data(self, start_date: str, end_date: str, query_file: str) -> None:
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

    def get_data(self, start_date: str, end_date: str, query_file: str) -> None:
        """
        Method that gets the data from elasticsearch
        start_date:[str] start date of the query
        end_date[str]: end date of the query
        query_file[str]: path to the query file to be used. The query file stores the data in json format

        :return: None
        """
        with open(query_file) as f:
            payload = json.load(f)
        self.data = self.elasticclient.search(
                    index=self.index,
                    request_timeout=600,
                    body=payload
        )
        print(self.data)


def main() -> None:
    """Main function calling all other functions"""
    configObject: configreader = configreader()
    configObject.generateConfigObject()
    configuration: dict = configObject.configObject
    connector = ElasticConnector(configuration)
    print(connector.__dict__)
    connector.get_data(start_date="", end_date="", query_file="./queries/match_all.json")

if __name__ == '__main__':
    main()