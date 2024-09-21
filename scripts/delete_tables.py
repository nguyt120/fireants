import google.cloud.bigquery as bq

project_name = 'anz-x-cosmos-prod-expt-3e0729'
dataset_list = [
    'pd_cosmos_analytics_fireant',
    # 'pd_cosmos_fireant_reporting'
]

table_starts_with = 'nguyek42'

count = 0

for dataset_name in dataset_list:
    print(f'------Checking dataset {dataset_name} -------')
    dataset = f'{project_name}.{dataset_name}'
    bq_client = bq.Client()
    tables = bq_client.list_tables(dataset=dataset)
    for table in tables:
        if table.table_id.startswith(table_starts_with):
            table_to_delete = f'{dataset}.{table.table_id}'
            print('Deleting table' + table_to_delete)
            # bq_client.delete_table(table_to_delete)
            count += 1
        else:
            None 
print(f'-----Successfully delete {count} tables! ------')