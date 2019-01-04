import re
from webmotors_vars import ENV_ANO,ENV_MODEL

regex_id_ficha = re.compile('idFichaTecnica:([0-9]+),')

# Retorna a url para scraping no formato correto da webmotors
def generate_url(page_num,modelo = ENV_MODEL,ano = ENV_ANO):
	return "https://www.webmotors.com.br/carros/?tipoveiculo=carros&anunciante=acessórios e serviços para autos" \
	      "|agências de publicidade|concessionária|loja|pessoa física&palavrachave={modelo}&anode={ano}&anoate={ano}&estadocidade=estoque" \
	      "&p={pagina}&o=1&qt=36".format(modelo=ENV_MODEL, ano=ENV_ANO, pagina=page_num)

# Retorna as informações da página do carro
# O schema muda, algumas páginas não possuem certas infos
def parse_car_page(soup_obj):

	car = dict()
	tables = list(filter(lambda obj: len(obj.text) > 0, soup_obj.select('table.vehicle-details__table')))
	car['carro_modelo'] = soup_obj.select_one("span.makemodel.makemodel-opinion").text.strip()
	car['carro_descr'] = soup_obj.select_one("span.col-10.versionyear-of").text.strip()
	car['preco'] = soup_obj.select_one("span.proposal-b__price").text.replace('R$','').replace('.','').strip()

	if soup_obj.select_one('p.info-seller'):
		car['info_vendedor'] =soup_obj.select_one('p.info-seller').text.strip()

	car['cidade'] = soup_obj.select_one('li.icon-map').text.strip()
	car['tipo_vendedor'] = soup_obj.select_one('small.store').text.strip()

	for table in tables:
		# Separando os dois tipos de formatos de informação que existem nas tabelas
		if table.find('strong'):
			# Atributos que variam de acordo com o anúncio
			keys = table.find_all('strong')
			values = table.find_all(attrs={'class':'vehicle-details__table__column__value'})
			for key,value in zip(keys,values):
				car[key.text.strip()] = value.text.strip()

		elif table.select_one("td.vehicle-details__table__column.options"):
			car['itens'] = [table_line.text.strip() for table_line in table.select('td.vehicle-details__table__column.options')]
	scripts = soup_obj.find_all('script')
	script = list(filter(lambda script: script.text.find('ficha-tecnica') > -1, scripts))[0].text
	car['id_ficha'] = regex_id_ficha.search(script).group(1)
	return car


