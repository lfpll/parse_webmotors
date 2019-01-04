from pymongo import MongoClient,InsertOne,UpdateOne
from pymongo.errors import BulkWriteError
import re
import itertools
import unidecode

# Corrige os formatos para otimizar as queries no uso de espaço no bigquery
def fix_json_types(dict_list):

	# Retorna valores númericos do tipo da coluna
	# Utilizado para evitar uso de espaço desnecessário nas consultas
	file_type_keys = {0: 'BOOLEAN', 1: 'INTEGER', 2: 'FLOAT', 3: 'STRING', 4: 'RECORD'}
	int_regexp = re.compile('^[0-9]+$')
	float_regexp = re.compile('^[0-9]+\,[0-9]+$')

	def define_type(undefined):
		undefined = undefined.replace('.', '')
		# Usa boleanas para sim e não
		if isinstance(undefined, int) or undefined.lower() in ['não', 'sim']:
			return 0
		if int_regexp.search(undefined):
			return 1
		elif float_regexp.search(undefined):
			return 2

		else:
			return 3

	client_mongo = MongoClient()

	coll = client_mongo['webmotors']['carros']
	for dict_chunk in [dict_list[i:i + 10] for i in range(0, len(dict_list), 10)]:
		try:
			coll.bulk_write([InsertOne(chunk) for chunk in dict_chunk])
		except BulkWriteError as e:
			print(e)
			print(e.details)

	chaves_schema = set(itertools.chain.from_iterable([list(mdb_obj.keys()) for mdb_obj in list(coll.find())]))
	chaves_schema.remove('_id')


	for key in chaves_schema:
		options_list = list(coll.distinct(key))

		# Remove acentos e transorma dashcase
		name =  unidecode.unidecode(key.lower().replace(' ','_').replace('?',''))
		# M
		if name != key:
			coll.update_many({},{'$rename':{key:name}})
			key = name

		# Separa as boleanas dos outros tipos
		options_list = list(filter(lambda val: not isinstance(val,float),options_list))
		if len(options_list) >0:
			if any(isinstance(s,str) for s in options_list):
				type =  file_type_keys[max([define_type(option) for option in options_list if isinstance(option,str)])]
				if type == 'BOOLEAN':
					coll.update_many({key: {'$in': ['Não', 'não']}}, {'$set': {key: False}})
					coll.update_many({key: {'$in': ['Sim', 'sim']}}, {'$set': {key: True}})
				elif type == 'INTEGER':
					update_ids = [UpdateOne({'_id': mdb_obj['_id']}, {'$set':{key: int(mdb_obj[key].replace('.',''))}}) for mdb_obj in
					              list(coll.find({key: {'$exists': 1}}, {key: 1}))]
					coll.bulk_write(update_ids)
				elif type == 'FLOAT':
					update_ids = [UpdateOne({'_id': mdb_obj['_id']}, {'$set':{key: float(mdb_obj[key].replace(',','.'))}}) for mdb_obj in
					              list(coll.find({key: {'$exists': 1}}, {key: 1}))]
					coll.bulk_write(update_ids)

	json_correct = list(coll.find({},{'_id':0}))
	coll.drop()
	client_mongo.close()
	return json_correct


