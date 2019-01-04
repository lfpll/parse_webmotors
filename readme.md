## Scraper proxy do site do webmotors##

Scraper de chamadas http assincronas para fazer web scraping do site da webmotors
Esta configurado para o onix 2015 mas pode ser modificado no webmotors_vars.py

### main.py 

Script que executa todo o processo para scraping e inserção na tabela da bigquery


### http\_calls.py

Possui as funções assincronas para otimizar as chamadas aos sites

### webmotors\_funcs.py e webmotors\_vars.py

Funções e variáveis apenas para esse projeto para otimizar e facilitar alguns usos como a criação de urls com padrões similares.
Possui uma customização para a pesquisa do carro e ano, se modificado observar o padrão do site.

### treat_json.py
_Utiliza do mongodb local_

Script para formatar os dicionários e otimizar os custos das queries, modificando strings para não apenas strings mas também outros tipos que consomem menos bytes no BigQuery.
Separado do scraping para debug mais fácil.

### query.py

Funções criadas para facilitar o uso da BigQuery Api

### proxy.py

Possui uma classe Proxy que criei para rotacionar os proxys e evitar a repetição de uso na mesma sessão.

### queries.sql 

Queries em sql para fazer as chamadas pedidas

p.s: erros de SSL irão acontecer sem afetar o código, acredito que seja problemas de autênticação sem proxys https.

p.s.2: caso a função treat_json seja removida as queries terão de ser modificadas para datatypes de strings

