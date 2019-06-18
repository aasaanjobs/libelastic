import importlib
import os
import json
import ujson
import time
import threading
import logging

from django.core.management.base import BaseCommand
from django.conf import settings
from elasticsearch import Elasticsearch, TransportError, NotFoundError
from elasticsearch.exceptions import ConnectionError as ElasticConnectionError

logger = logging.getLogger('threaded_logger')

print_module = importlib.import_module(settings.LIB_ELASTIC['custom_print_path'])
print_info = print_module.print_info
print_success = print_module.print_success
print_fail = print_module.print_fail
print_warn = print_module.print_warn

# Initialize the SMS Recipients to whom the final result of the command will be sent
try:
    if settings.ELASTIC_SMS_INDEX_RECIPIENT:
        elastic_sms_recipients = settings.ELASTIC_SMS_INDEX_RECIPIENT
    else:
        elastic_sms_recipients = []
except AttributeError:
    elastic_sms_recipients = []


class Command(BaseCommand):
    """
    Created By: Sohel Tarir
    Created On: 16/12/2015
    """
    help = "Used for indexing latest objects from DB to Elasticsearch"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.es = Elasticsearch(settings.LIB_ELASTIC['HOST'])
        self.env = settings.LIB_ELASTIC['ENV']
        self.data_type = None
        self.threads_count = 1
        self.doc_count = 0

    def add_arguments(self, parser):
        parser.add_argument('command_name')
        parser.add_argument('args1')

    def get_index_type(self):
        return settings.LIB_ELASTIC['INDICES'][self.data_type].get("doc_type")

    def get_index_alias(self):
        return "".join([settings.LIB_ELASTIC['INDICES'][self.data_type].get('index_prefix'),
                        settings.LIB_ELASTIC['ENV']])

    def has_parent(self, data_type=None):
        if data_type:
            parent = settings.LIB_ELASTIC['INDICES'][data_type].get('parent')
        else:
            parent = settings.LIB_ELASTIC['INDICES'][self.data_type].get('parent')
        if parent:
            return True
        else:
            return False

    @staticmethod
    def get_data_type(index_type):
        for key, value in settings.LIB_ELASTIC['INDICES'].items():
            if value["type"] == index_type:
                return key
        raise AttributeError("Failed to find Data Type for index type: %s" % index_type)

    def get_parent_attr(self, data_type=None):
        if self.has_parent(data_type=data_type):
            if data_type:
                return settings.LIB_ELASTIC['INDICES'][data_type].get('parent')
            else:
                return settings.LIB_ELASTIC['INDICES'][self.data_type].get('parent')
        else:
            raise AttributeError("Data type has no parent!")

    def __get_index_name(self):
        """
        Returns the index of the corresponding alias name
        """
        try:
            res = self.es.indices.get_alias(index='_all', name=self.get_index_alias())
        except NotFoundError:
            return None
        try:
            index = list(res.keys())[0]
        except IndexError:
            index = None
        return index

    def list_types(self):
        """
        List downs all the types of an Index
        :return:
        """
        index = self.__get_index_name()
        res = self.es.indices.get_mapping(index=index)
        return list(res[index]['mappings'].keys())

    def __get_mapping(self, index_type):
        mapping_file = "%s/%s" % (
        settings.BASE_DIR, settings.LIB_ELASTIC['INDICES'][self.data_type].get('mapping_file'))
        fp = open(mapping_file, 'r')
        data_mapping = json.load(fp)
        fp.close()
        return data_mapping

    def __init_index(self):
        """
        Initialises the index with timestamp while creating a new index
        :return:
        """
        if settings.LIB_ELASTIC['INDICES'][self.data_type] is None:
            raise NotImplementedError("No such index type (%s) is supported right now." % self.data_type)
        epoch_now = int(time.time())
        alias = "".join([settings.LIB_ELASTIC['INDICES'][self.data_type].get('index_prefix'),
                        settings.LIB_ELASTIC['ENV']])
        new_index = "_".join([alias, str(epoch_now)])
        return new_index

    def __exists_type(self):
        """
        Checks whether type exists for that data type or not
        :return:
        """
        res = self.es.indices.exists_type(index=self.get_index_alias(), doc_type=self.get_index_type())
        if res:
            return True
        else:
            return False

    def __create_index(self, index):
        """
        Creates a new index in the Elasticsearch cluster
        :param index: The index name
        :return:
        """
        res = self.es.indices.create(index=index)
        if res:
            print_success("Created Index: %s" % index)
            return True
        else:
            print_fail("Failed to create Index: %s" % index)
            return False

    def __delete_index(self, index):
        """
        Deletes an existing index in the Elasticsearch cluster
        :param index: The index name
        :return:
        """
        res = self.es.indices.delete(index=index)
        if res:
            print_success("Deleted Index: %s" % index)
            return True
        else:
            print_fail("Failed to delete Index: %s" % index)
            return False

    def __put_index_settings(self, new_index):
        """
        Puts the Index settings
        :param new_index:
        :return:
        """
        # Initialize the settings file location
        file_identifier = settings.LIB_ELASTIC['INDICES'][self.data_type].get('parent_doc_type', self.get_index_type())
        setting_file = "%s/%s" % (settings.BASE_DIR,
                                  settings.LIB_ELASTIC['INDICES'][self.data_type].get('setting_file_dir')
                                  + file_identifier + ".json")
        if not os.path.isfile(setting_file):
            print_warn("Found no settings file for " + self.get_index_type() + ". Skipping...")
            return True
        while True:
            res = self.es.cluster.health(index=new_index)
            print_info("Checking health of the cluster...Its " + res["status"])
            if res["status"] == "yellow" or res["status"] == "green":
                break
        # Close the index before putting the settings
        self.es.indices.close(index=new_index)
        print_info("Closed index for read/write...")
        print_info("Attempting to read setting file %s" % setting_file)
        # Read the settings file
        with open(setting_file, 'r') as fp:
            data_setting = json.load(fp)
            res = self.es.indices.put_settings(index=new_index, body=data_setting)
            if res:
                print_info("Added settings for index " + new_index)
            else:
                print_fail("Failed to add settings for index " + new_index)
                res = self.es.indices.delete(index=new_index)
                if res:
                    print_info("Deleted index " + new_index)
                else:
                    print_fail("Failed to delete the index " + new_index + ". Delete it manually.")
                exit(1)
        self.es.indices.open(index=new_index)
        print_info("Reopened index for read/write...")
        return True

    def __put_index_mapping(self, new_index):
        """
        Puts the Index mapping
        :return:
        """
        # Initialize the mapping file location
        mapping_file = "%s/%s" % (settings.BASE_DIR, settings.LIB_ELASTIC['INDICES'][self.data_type].get('mapping_file'))
        if not os.path.isfile(mapping_file):
            print_fail("Failed to find mapping file %s" % mapping_file)
            return False
        print_info("Attempting to read mapping file %s" % mapping_file)
        with open(mapping_file, 'r') as fp:
            data_mapping = json.load(fp)
            try:
                res = self.es.indices.put_mapping(doc_type=self.get_index_type(), index=new_index,
                                                  body=data_mapping)
            except TransportError as ex:
                print_fail("Problem in putting mapping for type: %s. Reason: %s" % (self.get_index_type(), ex.info))
                return False
            if res:
                print_success("Mapping added for index type: %s" % self.get_index_type())
                return True
            else:
                print_fail("Problem in putting mapping for index type: %s" % self.get_index_type())
                return False

    def __set_alias(self, new_index):
        """
        Sets the alias of the Index
        :param new_index:
        :return:
        """
        old_index = self.__get_index_name()
        res = self.es.indices.put_alias(index=new_index, name=self.get_index_alias())
        if res:
            print_success("Set alias (%s) for index %s." % (self.get_index_alias(), new_index))
            if old_index:
                print_info("Deleting Old Index %s" % old_index)
                self.__delete_index(old_index)
            return True
        else:
            print_fail("Failed to set alias (%s) for index %s." % (self.get_index_alias(), new_index))
            return False

    def __get_data_objects(self, offset=0, limit=20):
        """
        Retrieves data from the database
        :param offset: The offset of the queryset
        :param limit: The limit of the queryset
        :return:
        """
        try:
            module = __import__(settings.LIB_ELASTIC['INDICES'][self.data_type].get('pg_func_path'))
            list_candidate_data = getattr(module,
                                          settings.LIB_ELASTIC['INDICES'][self.data_type].get('list_pg_func'))
            return list_candidate_data(offset=offset, limit=limit)
        except KeyError:
            raise NotImplementedError("No such index type (%s) is supported right now." % self.data_type)

    def __scroll_and_copy(self, new_index, index_type, scroll_id, total_count, transform=False):
        """
        Scrolls through the documents and Copies the data to new index
        :param new_index:
        :param index_type:
        :param scroll_id:
        :param total_count: The Total Count of documents
        :return:
        """
        indexed_docs = 0
        while True:
            try:
                res = self.es.scroll(scroll_id=scroll_id, scroll="2m")
                scroll_id = res["_scroll_id"]  # This is the next scroll ID
                bulk_data = []
                for hit in res["hits"]["hits"]:
                    content = {
                        "index": {
                            "_index": new_index,
                            "_type": index_type,
                            "_id": hit["_id"]
                        }
                    }
                    if self.has_parent(data_type=self.get_data_type(index_type)):
                        content["index"]["parent"] = hit["_source"][self.get_parent_attr(
                            self.get_data_type(index_type))]
                    bulk_data.append(ujson.dumps(content))
                    if transform:
                        transform_module = importlib.import_module(
                            settings.LIB_ELASTIC['INDICES'][self.data_type].get('transform_func'))
                        transform_func = transform_module.transform_func
                        transformed_data = transform_func(hit["_source"], type=index_type)
                        bulk_data.append(ujson.dumps(transformed_data))
                    else:
                        bulk_data.append(ujson.dumps(hit["_source"]))
                    indexed_docs += 1
                if not len(bulk_data):
                    return True
                body = "\n".join(str(x) for x in bulk_data)
                res = self.es.bulk(body=body, request_timeout=45)
                if not res["errors"]:
                    print_info("Total " + str(indexed_docs) + " documents have been reindexed for type: " + index_type)
                else:
                    epoch_now = int(time.time())
                    dump_file = "es_update_" + str(epoch_now) + ".dump"
                    with open(dump_file, 'w') as outfile:
                        json.dump({"type": self.get_index_type(), "response": res}, outfile)
                    print_fail("Failed to index document. Error has been dumped in %s" % dump_file)
                    return False
            except Exception as ex:
                if not indexed_docs == total_count:
                    import traceback;
                    print_fail("Failed to copy type: " + index_type + ". Reason: " + str(traceback.format_exc()))
                    return False
                return True

    def __handle_other_types(self, new_index, bulk_size=100, transform=False):
        """
        :param new_index: The New Index being created
        :return:
        """
        if self.__get_index_name() is None:
            return True

        # List down all the types in the Index
        types = self.list_types()

        # Exclude the type requested for indexing
        if self.get_index_type() in types:
            types.remove(self.get_index_type())

        # First copy the other types
        if "percolator" in types:
            types.remove("percolator")

        for index_type in types:
            try:
                res = self.es.indices.put_mapping(index=new_index, doc_type=index_type,
                                                  body=self.__get_mapping(index_type))
            except TransportError as ex:
                print_fail("Problem in putting mapping for type: %s. Reason: %s" % (index_type, ex.info))
                return False
            if not res:
                print_fail("Failed to put mapping for type: " + index_type)
                return False
            query = {"query": {"match_all": {}}, "size": bulk_size / 2}
            scan_res = self.es.search(index=self.get_index_alias(), doc_type=index_type, body=query,
                                      scroll="2m")
            if not self.__scroll_and_copy(new_index=new_index, index_type=index_type, scroll_id=scan_res["_scroll_id"],
                                          total_count=scan_res["hits"]["total"], transform=transform):
                return False
        return True

    def __copy_index_type(self, new_index, doc_type, transform=False, bulk_size=100):
        query = {"query": {"match_all": {}}, "size": bulk_size / 2}
        scan_res = self.es.search(index=self.get_index_alias(), doc_type=doc_type, body=query,
                                  scroll="2m")
        if not self.__scroll_and_copy(new_index=new_index, index_type=doc_type, scroll_id=scan_res["_scroll_id"],
                                      total_count=scan_res["hits"]["total"], transform=transform):
            return False
        return True

    def __handle_indexing_in_threads(self, new_index, bulk_size):
        total_doc_count = self.doc_count
        if self.threads_count == 1:
            if self.__indexing(new_index, bulk_size, 0, total_doc_count):
                logger.info("Finished Indexing!!!")
                return True
        else:
            threads = []
            response = list()

            for i in range(self.threads_count):
                t = threading.Thread(name='Thread-' + str(i), target=lambda response, arg1:
                response.append(self.__indexing(arg1, bulk_size, bulk_size * i, bulk_size * (i + 1))),
                                     args=(response, new_index), daemon=True)
                threads.append(t)
                t.start()
            # joining thread!! so that main thread can wait till other threads get finished
            for th in threads:
                th.join()
            for resp in response:
                if not resp:
                    logger.error("Finished Failed!!")
                    return False
            else:
                logger.info("Finished Indexing!!!")
                return True

    def __indexing(self, new_index, bulk_size, starting_offset=0, end_offset=None):
        """
        Index the Data
        :param new_index: The New Index to be indexed to
        :param bulk_size: The Limit
        :return:
        """
        iteration = 0
        repetition = 0
        last_offset = end_offset
        while True:
            current_offset = starting_offset + iteration * bulk_size

            # Managing Offset on basis of round-robin for each thread
            if self.threads_count > 1:
                current_offset = starting_offset + (iteration * self.threads_count * bulk_size)
                end_offset = last_offset + (iteration * self.threads_count * bulk_size)
            if end_offset and end_offset <= current_offset:
                return True
            if self.doc_count == 0:
                self.doc_count = 1e+15
            if self.doc_count - current_offset > 0:
                objects = self.__get_data_objects(offset=current_offset,
                                                  limit=min(bulk_size, self.doc_count - current_offset))
            else:
                break

            if objects.__len__() == 0:
                break
            bulk_data = []
            doc_count = 0

            for obj in objects:
                if self.data_type == "Suggestions" or self.data_type == "Autocomplete" \
                        or self.data_type == "SearchKeyword":
                    content = {
                        "index": {
                            "_index": new_index,
                            "_type": self.get_index_type()
                        }
                    }
                else:
                    content = {
                        "index": {
                            "_index": new_index,
                            "_type": self.get_index_type(),
                            "_id": str(obj["id"] + str(repetition + 1))
                        }
                    }
                    if self.has_parent():
                        content["index"]["parent"] = obj[self.get_parent_attr()]
                # obj["last_indexed_on"] = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
                bulk_data.append(ujson.dumps(content))
                bulk_data.append(ujson.dumps(obj))
                doc_count += 1
            body = "\n".join(str(x) for x in bulk_data)
            res = self.es.bulk(body=body, request_timeout=45)
            if not res["errors"]:
                logger.info("Finished iteration: %d with total documents: %d." % (iteration + 1, doc_count))
                iteration += 1
            else:
                epoch_now = int(time.time())
                dump_file = "es_update_" + str(epoch_now) + ".dump"
                with open(dump_file, 'w') as outfile:
                    json.dump({"type": self.get_index_type(), "response": res}, outfile, indent=4)
                logger.error("Failed to index document. Error has been dumped in %s" % dump_file)
                return False
            if self.doc_count > 0 and starting_offset + (iteration * self.threads_count * bulk_size) >= self.doc_count:
                break
        return True

    def put_percolator_data(self, new_index):
        bulk_data = []
        try:
            objects = settings.LIB_ELASTIC['INDICES']['Percolator'].get("list_pg_func")()
            module = __import__(settings.LIB_ELASTIC['INDICES'][self.data_type].get('pg_func_path'))
            generate_percolate_list = getattr(module,
                                              settings.LIB_ELASTIC['INDICES'][self.data_type].get(
                                                  'list_pg_func'))
            objects = generate_percolate_list()
            for obj in objects:
                content = {
                    "index": {
                        "_index": new_index,
                        "_type": ".percolator",
                        "_id": str(obj.pop("id"))
                    }
                }
                bulk_data.append(ujson.dumps(content))
                bulk_data.append(ujson.dumps(obj))
            body = "\n".join(str(x) for x in bulk_data)
            res = self.es.bulk(body=body, request_timeout=45)
            if not res["errors"]:
                logger.info("Successfully added percolator type in candidate index..!!")
            else:
                epoch_now = int(time.time())
                dump_file = "es_update_" + str(epoch_now) + ".dump"
                with open(dump_file, 'w') as outfile:
                    json.dump({"type": self.get_index_type(), "response": res}, outfile, indent=4)
                logger.error("Failed to index .percolator type,  error has been dumped in %s, continuing indexing.."
                             % dump_file)
        except Exception as e:
            print_info("Exception while indexing .percolator type {0}".format(str(e)))

    def handle(self, *args, **options):
        start_time = time.time()
        try:
            self.es.ping()
        except ElasticConnectionError as e:
            print_fail("Unable to connect to Elastic search for Host: %s. Reason: %s" % (settings.ES_HOST, e.error))
            exit(1)
        command_name = options['command_name']
        args1 = options['args1']
        args_arr = args1.split(',')
        self.data_type = args_arr[0]
        try:
            chunk = args_arr[1]
            if chunk.isdigit():
                bulk_size = int(chunk)
            else:
                bulk_size = 20
        except IndexError:
            bulk_size = 20  # by default
        try:
            no_of_threads = args_arr[2]
            if no_of_threads.isdigit():
                self.threads_count = int(no_of_threads)
                doc_count = args_arr[3]
                self.doc_count = int(doc_count)
        except IndexError:
            pass  # by default threads_count = 1
        if command_name == 'force_index':
            # Create a new Index without the alias, will be setting the alias last.
            new_index = self.__init_index()
            try:
                if not (self.__create_index(new_index) and self.__put_index_settings(new_index)):
                    print_fail("Indexing Failed")
                    self.__delete_index(index=new_index)
                if self.has_parent():
                    if self.__put_index_mapping(new_index) \
                            and self.__handle_other_types(new_index=new_index, bulk_size=bulk_size) \
                            and self.__handle_indexing_in_threads(new_index=new_index, bulk_size=bulk_size) \
                            and self.__set_alias(new_index=new_index):
                        print_success("Indexing Completed in {0} seconds".format(str(time.time() - start_time)))
                    else:
                        print_fail("Indexing Failed")
                        self.__delete_index(index=new_index)
                else:
                    if self.__handle_other_types(new_index=new_index, bulk_size=bulk_size) \
                            and self.__put_index_mapping(new_index) \
                            and self.__handle_indexing_in_threads(new_index=new_index, bulk_size=bulk_size) \
                            and self.__set_alias(new_index=new_index):
                        if self.data_type == "Candidate":
                            self.put_percolator_data(new_index)
                        print_success("Indexing Completed in {0} seconds".format(str(time.time() - start_time)))
                    else:
                        print_fail("Indexing Failed")
                        self.__delete_index(index=new_index)
            except Exception as ex:
                print_fail(str(ex))
                self.__delete_index(index=new_index)
                raise ex
        elif command_name == 'remap' or command_name == 'transform':
            if command_name == 'transform':
                transform = True
            else:
                transform = False
            new_index = self.__init_index()
            try:
                if not (self.__create_index(new_index) and self.__put_index_settings(new_index)):
                    print_fail("Indexing Failed")
                    self.__delete_index(index=new_index)
                if self.has_parent():
                    if self.__put_index_mapping(new_index) and \
                            self.__handle_other_types(new_index=new_index, bulk_size=bulk_size) \
                            and self.__copy_index_type(new_index=new_index, doc_type=self.get_index_type(),
                                                       transform=transform, bulk_size=bulk_size) \
                            and self.__set_alias(new_index=new_index):
                        print_success("Indexing Completed in {0} seconds".format(str(time.time() - start_time)))
                    else:
                        print_fail("Indexing Failed")
                        self.__delete_index(index=new_index)
                else:
                    if self.__handle_other_types(new_index=new_index, bulk_size=bulk_size) \
                            and self.__put_index_mapping(new_index) \
                            and self.__copy_index_type(new_index=new_index, doc_type=self.get_index_type(),
                                                       transform=transform, bulk_size=bulk_size) \
                            and self.__set_alias(new_index=new_index):
                        if self.data_type == "Candidate":
                            self.put_percolator_data(new_index)
                        print_success("Indexing Completed in {0} seconds".format(str(time.time() - start_time)))
                    else:
                        print_fail("Indexing Failed")
                        self.__delete_index(index=new_index)
            except Exception as ex:
                print_fail(str(ex))
                self.__delete_index(index=new_index)
                raise ex
