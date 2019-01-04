-- Preco medio  do caror nas cidades
select CONCAT('R$ ',cast(round(avg(price),2) as string)) as preco_medio,city
from `luiz_webmotors.onix_2015` 
group by(city)
order by preco_medio desc

-- Retorna a cidade com maior número de carros a venda
select count(1) as numero_de_carros,city
from `luiz_webmotors.onix_2015` 
group by(city)
order by count(1) desc
limit 1

-- Retorna os 5 carros mais baratos
select model_car ,price,city,seller_type, combustivel, cor, model_desc  
from `luiz_webmotors.onix_2015` 
order by price
limit 5

-- Retorna o carro mais caro
-- Alguém realmente listou um Onix por esse valor https://tinyurl.com/ydan5t3e
select model_car ,price,city,seller_type, combustivel, cor, model_desc  
from `luiz_webmotors.onix_2015` 
order by price desc
limit 1
