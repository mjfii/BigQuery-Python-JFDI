import os
import json
from google.cloud import bigquery as bq
from create import create_dataset
from create import create_table
from client import get_client

project_id = 'big-query-learning-334420'
dataset_name = 'bls_cpx'


#
table_name = "test_table"

data_definition = [
    ("name", "STRING"),
    ("gender", "STRING"),
    ("count", "INTEGER")
]

client, rv = get_client()
print(json.dumps(rv, indent=3))

dataset, rv = create_dataset(client, dataset_name)
print(json.dumps(rv, indent=3))

   # create_table(dataset_name, table_name, data_definition)


