import json

from django.apps import AppConfig
from django.conf import settings
from elasticsearch import Elasticsearch


class ElasticMapperConfig(AppConfig):
    name = 'aj_libelastic'

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.es = Elasticsearch(settings.LIB_ELASTIC['HOST'])
        self.index_mapping_in_elastic_server = {}
        self.index_mapping_in_application = {}
        self.ignored_filed = ['format']

    def ready(self):
        try:
            print("----------------------------------------------------------------------------------")
            index = settings.LIB_ELASTIC
            for key, value in index['INDICES'].items():
                print(value['mapping_file'])
                print(index['ENV'] + "_" + value['index_prefix'])
                mapping_file = settings.BASE_DIR + value['mapping_file']

                # loading mapping details from elastic server
                server_mapping = self.es.indices.get_mapping(index=value['index_prefix'] + "_" + index['ENV'])
                for key in server_mapping.keys():
                    index_prop = server_mapping.get(key)
                    mapping_details = index_prop.get('mappings')
                    index_data = mapping_details.get(value['doc_type'])
                    properties_data = index_data.get('properties')
                    self.index_mapping_in_elastic_server = properties_data

                # loading mapping details from local mapping file
                with open(mapping_file, 'r') as fp:
                    application_mapping_details = json.load(fp)
                    self.index_mapping_in_application = application_mapping_details.get(value['doc_type']).get('properties')
                self.compare_mapping(value)
        except Exception as ex:
            print("found exception ", ex)
            print("\n")
        print("----------------------------------------------------------------------------------")

    def compare_mapping(self, index_details):
        z1 = True
        for key in self.index_mapping_in_application.keys():
            if key in self.index_mapping_in_elastic_server:
                first_level_value_in_elastic = self.index_mapping_in_elastic_server.get(key)
                first_level_value_in_application = self.index_mapping_in_application.get(key)

                z1 = z1 and self.check_mapping(first_level_value_in_application, first_level_value_in_elastic)

        if not z1:
            print('\033[91m'+"found mismatch of index mapping in local and server ::  ", index_details['index_prefix']+'\033[0m')
        else:
            print('\033[92m'+"Everything is fine !!!!!!!  for  :  ", index_details['index_prefix']+'\033[0m')

    def check_mapping(self, d1, d2):
        if 'dynamic' in d1.keys():
            return True
        else:
            z = True
            for key in d1.keys():
                if key not in self.ignored_filed:
                    value1 = d1.get(key)
                    value2 = d2.get(key)
                    if type(value1) is str:
                        if value1 != value2:
                            return False
                    elif type(value1) is dict:
                        z = self.check_mapping(value1, value2)
                        if not z:
                            return False
        return True