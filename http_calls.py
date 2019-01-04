import asyncio
import aiohttp
import random
from proxy import Proxy_rotate
from webmotors_vars import headers
from time import sleep

from aiohttp import ClientOSError,ClientHttpProxyError,ClientResponseError,ClientError

# Busca páginas assincronas
# Retorna dicionários caso hajam erros
# Retorna o corpo do html caso haja sucesso
async def fetch_page(session,queue_urls,proxy,timeout):
	url = await queue_urls.get()
	try:
		async with session.get(url,headers=headers,proxy=proxy,timeout=timeout) as response:
			if response.status == 200:
				return await response.text()
			elif response.status == 403:
				# Remover este proxy da lista já que o site bloqueou
				return {'url': url, 'proxy': proxy}
	except Exception as e:
		return {'url': url, 'proxy':''}

# Função assincrona para necessária para as chamadas http
# Retorna duas listas -> successes = corpo html, fails = dicionário
async def get_html(urls, loop, proxies):
	q_url = asyncio.Queue()
	[q_url.put_nowait(url) for url in urls]

	async with aiohttp.ClientSession(loop=loop, connector=aiohttp.TCPConnector(limit=100)) as session:
		tasks = []
		for i in range(len(urls)):
			task = asyncio.ensure_future(fetch_page(session=session, queue_urls=q_url,
			                                        proxy=await proxies.get_proxy(),timeout=aiohttp.ClientTimeout(5)))
			tasks.append(task)
		responses = await asyncio.gather(*tasks)

		successes = list(filter(lambda x: not isinstance(x, dict), responses))
		fails = list(filter(lambda x: isinstance(x,dict),responses))
		return successes,fails

# Função recursiva que faz 200 chamadas por vez com proxy via http assincronamente
# Itera os proxies,remove os falhos até conseguir todas as chamadas
def call_urls(urls, proxy_rotator, loop = asyncio.get_event_loop(), htmls=None):

	if htmls is None:
		htmls = []

	# Fila de urls sempre igual a 200 para otimizar o uso de varias chamadas de proxy
	urls,choosen_urls = urls[200:],urls[:200]

	# Respostas das chamadas http, retorna o corpo html ou dicionário caso sem sucesso {'proxy':XXXXXX,'url':http://...}
	responses,fails = loop.run_until_complete(get_html(choosen_urls, loop, proxy_rotator))

	# Mais um tweak para evitar ser bloqueado
	sleep(random.randint(2,5))
	htmls.extend(responses)

	# Removendo os proxys e pegando as urls que falharam
	[proxy_rotator.remove_value(fail_dict['proxy']) for fail_dict in fails]
	urls.extend([fail_dict['url'] for fail_dict in fails])

	# Urls que não tiveram sucesso
	if len(urls) >0:
		print('%s proxys restantes' % str(len(proxy_rotator.proxys)))
		print('%s urls restantes'%len(urls))
		return call_urls(urls=urls, proxy_rotator=proxy_rotator, loop=loop, htmls=htmls)
	else:
		return htmls

