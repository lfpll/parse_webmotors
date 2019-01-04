from pandas import DataFrame
from google.cloud import bigquery

# Cria o novo dataset
def create_dataset(client,name_dataset):
	dataset_id = name_dataset
	dataset_ref = client.dataset(dataset_id)
	dataset = bigquery.Dataset(dataset_ref)
	dataset.location = 'US'
	client.create_dataset(dataset)


# Cria a tabela a partir dataset, nome de tabela e schema json como a da documentação
def create_table(query_client,name_dataset,table_name,json_array_schema):
	dataset_ref = query_client.dataset(name_dataset)
	bigquery_schemas = [bigquery.SchemaField(name=json_schema['name'],
	                                         field_type=json_schema['type'],
	                                         mode=json_schema['mode']) for json_schema in json_array_schema]
	table_ref = dataset_ref.table(table_name)
	table = bigquery.Table(table_ref, schema=bigquery_schemas)
	table = query_client.create_table(table)  # API request
	assert table.table_id == table_name


# Insere dados de ma lista de dicionários
def insert_dict_list(insert_client,name_dataset,table_name,dict_list):
	dataset_ref = insert_client.dataset(name_dataset)
	job_config = bigquery.LoadJobConfig()
	job_config.autodetect = True
	job_config.source_format = bigquery.SourceFormat.NEWLINE_DELIMITED_JSON
	load_job = insert_client.load_table_from_dataframe(
		DataFrame.from_dict(dict_list),
		dataset_ref.table(table_name),
		job_config=job_config)
	load_job.result()
	assert load_job.state == 'DONE'
	assert insert_client.get_table(dataset_ref.table(table_name)).num_rows == len(dict_list)
