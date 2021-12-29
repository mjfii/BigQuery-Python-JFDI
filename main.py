import os
import json
from google.cloud import bigquery as bq
from create_table import create_table
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

x = get_client()

   # create_table(dataset_name, table_name, data_definition)

print(json.dumps(x, indent=3))

