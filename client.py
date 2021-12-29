import os
import json
from google.cloud import bigquery as bq


#
def get_client() -> dict:

    # build output object
    _return_value = {
        'project_id': None,
        'scope': None,
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
    # TODO: catch specific exceptions
    try:
        _client = bq.Client.from_service_account_info(_service_account_info)
        _return_value['project_id'] = _client.project
        _return_value['scope'] = _client.SCOPE
    except Exception as ex:
        _return_value['errors'].append(ex)
        return _return_value

    return _client, _return_value

