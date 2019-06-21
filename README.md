# libelastic
Elasticsearch library and utils used by microservices in Aasaanjobs

Add 'aj_libelastic' as a app in INSTALLED_APP or INSTALLED_APP_CUSTOM
Under the main settings add the below snippet:

```
LIB_ELASTIC = {
    'HOST': 'http://localhost:9200',
    'ENV': 'local',
    'custom_print_path': 'core.utils.custom_print',
    'INDICES': {
        'Candidate': {
            'index_prefix': 'candidates',
            'doc_type': 'candidate',
            'mapping_file': '/core/elastic/mappings/candidate.json',
            'setting_file_dir': '/core/elastic/settings/',
            'pg_func_path': 'database.pg_functions.candidate.candidate',
            'detail_pg_func': 'get_candidate_data',
            'list_pg_func': 'list_candidate_data',
            'transform_func_path': 'core.elastic.tranform_script',
            'parent': 'candidate_id',         (If required)
            'parent_doc_type': 'candidate',   (If required)
        }
    }
}
```
