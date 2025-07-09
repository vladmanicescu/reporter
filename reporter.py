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
from time_eval import TimeEval as time_eval

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

class DataTransform:
    """
    Class that Transforms the data for the final calculation
    """
    def __init__(self, data: list[dict]) -> None:
        """
        Constructor of the data processor.
        :param data: List of dictionaries representing records
        """
        self.data: list = data
        self.uk_geo_day: list = []
        self.uk_nw_day: list = []
        self.mobile_day: list = []
        self.uk_geo_eve: list = []
        self.uk_nw_eve: list = []
        self.mobile_eve: list = []
        self.uk_geo_weekend: list = []
        self.uk_nw_weekend: list = []
        self.mobile_weekend: list = []
        self.weekend_total: list = []
        self.business_total: list = []
        self.non_business_week_total: list = []

    @staticmethod
    def __check_uk_geo(prefix: str) -> bool:
        """
        :param prefix: -> str -> Prefix to be checked
        :return: -> bool True if prefix belongs to the uk_geo operator, false if it doesn't
        """
        return prefix[:3] in ["441", "442"]

    @staticmethod
    def __check_uk_nw(prefix: str) -> bool:
        """
        :param prefix: -> str -> Prefix to be checked
        :return: -> bool True if prefix belongs to the uk_nw operator, false if it doesn't
        """
        return prefix[:2] == "443"

    @staticmethod
    def __check_uk_mobile(prefix: str) -> bool:
        """
        :param prefix: -> str -> Prefix to be checked
        :return: -> bool True if prefix is mobile, false if it isn't
        """
        return (prefix != "447606" and prefix[:2] == "447")

    def _split_data_by_timestamp(self) -> None:
        """
        Internal method that splits data based on call moment and ingress call info party
        :return: None
        """
        for record in self.data:
            _ingress_call_info_inviting_ts  =  str(record['fields']['ingress_call_info_inviting_ts'][0])
            _parsed_date = datetime.strptime(_ingress_call_info_inviting_ts, "%Y%m%d%H%M%f")
            if time_eval(_parsed_date).is_weekend():
                self.weekend_total.append(record['fields'])
            elif time_eval(_parsed_date).is_business():
                self.business_total.append(record["fields"])
            else:
                self.non_business_week_total.append(record["fields"])
        print("This is weekend",self.weekend_total)
        print("This is business",self.business_total)
        print("This is non business",self.non_business_week_total)

    def split_data_by_operator(self) -> None:
        """
        Method that splits  data by operator
        :return: -> None
        """
        self._split_data_by_timestamp()
        for record in self.weekend_total:
            _ingress_call_info_called_party = str(record["ingress_call_info_called_party"][0])
            _prefix = _ingress_call_info_called_party[0:6]
            print(_prefix)
            if self.__check_uk_nw(_prefix):
                self.uk_nw_weekend.append(record)
            elif self.__check_uk_geo(_prefix):
                self.uk_geo_weekend.append(record)
            elif self.__check_uk_mobile(_prefix):
                self.mobile_weekend.append(record)
        for record in self.business_total:

            _ingress_call_info_called_party = str(record["ingress_call_info_called_party"][0])
            _prefix = _ingress_call_info_called_party[0:6]
            print(_prefix)
            if self.__check_uk_nw(_prefix):
                self.uk_nw_day.append(record)
            elif self.__check_uk_geo(_prefix):
                self.uk_geo_day.append(record)
            elif self.__check_uk_mobile(_prefix):
                self.mobile_day.append(record)
        for record in self.non_business_week_total:
            _ingress_call_info_called_party = str(record["ingress_call_info_called_party"][0])
            _prefix = _ingress_call_info_called_party[0:6]
            if self.__check_uk_nw(_prefix):
                self.uk_nw_eve.append(record)
            elif self.__check_uk_geo(_prefix):
                self.uk_geo_eve.append(record)
            elif self.__check_uk_mobile(_prefix):
                self.mobile_eve.append(record)
        print(self.mobile_eve)
        print(self.uk_geo_eve)
        print(self.uk_nw_eve)
        print(self.mobile_day)
        print(self.uk_nw_day)
        print(self.uk_geo_day)
        print(self.mobile_weekend)
        print(self.uk_geo_weekend)
        print(self.uk_nw_weekend)


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
        voice_data = elastic_connector.get_data_prev_month(start_date="", end_date="", query_file=query_path)
        log_message = f"Retrieved data for {query_name}. Start processing..."
        logger.info(log_message)
        DataTransform(voice_data).split_data_by_operator()


if __name__ == '__main__':
    main()