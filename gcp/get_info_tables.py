import google.cloud.bigquery as bigquery
import pandas as pd
from datetime import date
import os

project_name = 'anz-x-cosmos-prod-expt-3e0729'
dataset_list = [
    'pd_cosmos_analytics_fireant',
    'pd_cosmos_fireant_reporting'
]
dataset_name = 'pd_cosmos_analytics_fireant'
for dataset_name in dataset_list:
    dataset = f'{project_name}.{dataset_name}'

    df = pd.DataFrame(columns=['Project','Dataset','Table','Created', 'Modified', 'RowCount']) 

    bq_client = bigquery.Client()
    tables = bq_client.list_tables(dataset=dataset)

    for table in tables:
        # Get full attributes from table
        table = bq_client.get_table("{}.{}.{}".format(table.project, table.dataset_id,table.table_id))

        df = df._append(
            {
                'Project'  : table.project,
                'Dataset'  : table.dataset_id,
                'Table'    : table.table_id,
                'Created'  : "{}|{}".format(table.created.date(),table.created.time()),
                'Modified' : "{}|{}".format(table.modified.date(),table.modified.time()),
                'RowCount' : table.num_rows,
            },
            ignore_index=True
        )
    # print(df)

    df.to_csv(
        "reports/{}-{}-report.csv".format(dataset_name, date.today()),
        sep=',',
        encoding='utf-8'
    )