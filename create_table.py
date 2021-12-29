import os
import json
from google.cloud import bigquery as bq
import google.api_core.exceptions


#
def create_table(dataset_name: str, table_name: str, data_definition: list) -> dict:

    # build output object
    _return_value = {
        'project': {},
        'dataset': {},
        'table': {},
        'errors': []
    }

    # get service account info
    try:
        _service_account_info = os.environ['service_account_info']
        _service_account_info = json.loads(_service_account_info)
    except KeyError:
        _return_value['errors'].append('Service account information is not found.')
        return _return_value
    except json.decoder.JSONDecodeError:
        _return_value['errors'].append('Service account information is not in a valid JSON format.')
        return _return_value

    # get the big query client from the service account info
    try:
        client = bq.Client.from_service_account_info(_service_account_info)
        project_id = client.project
        _return_value['project']['id'] = project_id
    except Exception as ex:
        _return_value['errors'].append(ex)
        return _return_value

    #
    dataset_id = '.'.join([project_id, dataset_name])
    _return_value['dataset']['name'] = dataset_name
    dataset = bq.Dataset(dataset_id)

    # TODO: add kwargs
    # dataset.location = ''
    # dataset.description = ''

    # ensure the dataset exists
    try:
        dataset_out = client.create_dataset(dataset, timeout=30)
        _return_value['dataset']['created'] = True
    except google.api_core.exceptions.Conflict as ex:
        _return_value['dataset']['created'] = False
        _return_value['errors'].append(ex.message)

    # create the table
    try:
        table_out = dataset.table(table_name)

        # TODO: fix this to allow for all SchemaField variables
        schema = []
        for attribute in data_definition:
            schema.append(bq.SchemaField(str(attribute[0]), str(attribute[1])))

        table = bq.Table(table_out, schema=schema)
        client.create_table(table)
        _return_value['table']['created'] = True
    except google.api_core.exceptions.Conflict as conflict:
        _return_value['table']['created'] = False
        _return_value['errors'].append(conflict.message)

    return _return_value
