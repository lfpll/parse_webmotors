import requests
import asyncio
import os
import json
import re
from google.cloud import bigquery
from bs4 import BeautifulSoup
from random import randint

from query import insert_dict_list,create_dataset
from http_calls import call_urls
from treat_json import fix_json_types
from webmotors_vars import headers,proxys
from webmotors_funcs import generate_url,parse_car_page
from proxy import Proxy_rotate


# Pegando a numeração da ultima página
response = requests.get(generate_url(page_num=1), headers=headers)
soup = BeautifulSoup(response.content,'lxml')
last_page_num = re.search('(\?|^|\&)p=([0-9]+)($|\&)',soup.select_one('a.paginationResult.last')['href']).group(2)

# Gerando todas as urls em uma lista para fazer chamadas assincronas e acelerar o scraping
urls = [generate_url(page_num=page_num) for page_num in range(1,int(last_page_num)+1)]

# Classe proxy que roda a partir de uma queue para utilizar multiplos acessos unicos por proxy e evitar bloqueios
proxy_rotator = Proxy_rotate(proxys)

# Html das páginas das listas dos carros
html_pages = call_urls(urls=urls, proxy_rotator=proxy_rotator)
print('Parsing the urls of the cars')

cars_urls = []

# Scraping das urls das páginas dos carros,salvando em uma lista e um txt como backup
with open('urls_list.txt','w') as url_file:
	for html in html_pages:
		soup = BeautifulSoup(html,'lxml')
		[(url_file.write(a['href']+'\n'),cars_urls.append(a['href'])) for a in soup.select('a.nn.tipo1.c-after')]

# Html de cada página unica do carro
cars_htmls = call_urls(urls=cars_urls, proxy_rotator=proxy_rotator)

# Scrape das páginas html
car_dicts = []
i =0
print('Scraping car pages')
for car_html in cars_htmls:
	try:
		car_dicts.append(parse_car_page(BeautifulSoup(car_html, 'lxml')))
	except Exception as e:
		print(e)
		# Caso hajam falhas no scraping salva o arquivo.
		with open('file%s.html'%str(i),'w') as file:
			file.write(str(car_html))
			i = i+ 1

# Uso do mongodb para arrumar o 'schema' da tabela modificando para boleanos,floats,ints...
car_dicts = fix_json_types(car_dicts)

# Salvando as informações dos carros em formato json para backup
with open('cars.json','w') as jsonfile:
	json.dump(car_dicts, jsonfile)


os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = "./chave.json"
bigquery_client = bigquery.Client()
dataset_name = 'luiz_webmotors'
create_dataset(client=bigquery_client,name_dataset=dataset_name)
insert_dict_list(insert_client=bigquery_client,name_dataset=dataset_name,table_name='onix_2015',dict_list=car_dicts)