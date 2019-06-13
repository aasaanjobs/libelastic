import json
from django.apps import AppConfig
from django.conf import settings
from elasticsearch import Elasticsearch

class ElasticMapperConfig(AppConfig):
    name = 'elastic_mapper'

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.es = Elasticsearch(settings.ELASTIC_HOST_DETAILS)
        self.index_mapping_in_elastic_server = {}
        self.index_mapping_in_application = {}
        self.ignored_filed = ['format']

    def ready(self):
        try:
            print("----------------------------------------------------------------------------------")
            elastic_index_details = settings.ELASTIC_MAPPING_DETAILS
            for index_details in elastic_index_details:
                mapping_file = settings.BASE_DIR + index_details['mapping_file']

                # loading mapping details from elastic server
                server_mapping = self.es.indices.get_mapping(index=index_details['alias'])
                for key in server_mapping.keys():
                    index_prop = server_mapping.get(key)
                    mapping_details = index_prop.get('mappings')
                    index_data = mapping_details.get(index_details['doc_type'])
                    properties_data = index_data.get('properties')
                    self.index_mapping_in_elastic_server = properties_data

                # loading mapping details from local mapping file
                with open(mapping_file, 'r') as fp:
                    application_mapping_details = json.load(fp)
                    self.index_mapping_in_application = application_mapping_details.get(index_details['doc_type']).get(
                        'properties')
                self.compare_mapping(index_details)
        except Exception as ex:
            print('\033[91m' + "found exception ", ex + '\033[0m')
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
            print('\033[91m' + "found mismatch of index mapping in local and server ::  ",
                  index_details['alias'] + '\033[0m')
        else:
            print('\033[92m' + "Everything is fine !!!!!!!  for  :  ", index_details['alias'] + '\033[0m')

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
