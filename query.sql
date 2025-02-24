with cte_ceara as 
(
	select 'Ceará' as localidade,
	count(*) as numero_de_atracacoes,
	COUNT(*) - LAG(COUNT(*), 1) OVER (PARTITION BY mes_inicio_operacao ORDER BY ano_inicio_operacao) AS variacao_num_atracacao_ano_anterior,
	avg(tempo_espera_atracacao) as tempo_medio_de_espera,
	avg(tempo_atracado) as tempo_medio_atracado,
	ano_inicio_operacao,
	mes_inicio_operacao
	from dbo.atracacao_fato 
	where sguf = 'CE'
	group by ano_inicio_operacao, mes_inicio_operacao
),
cte_nordeste as 
(
	select 'Nordeste' as localidade,
	count(*) as numero_de_atracacoes,
	COUNT(*) - LAG(COUNT(*), 1) OVER (PARTITION BY mes_inicio_operacao ORDER BY ano_inicio_operacao) AS variacao_num_atracacao_ano_anterior,
	avg(tempo_espera_atracacao) as tempo_medio_de_espera,
	avg(tempo_atracado) as tempo_medio_atracado,
	ano_inicio_operacao,
	mes_inicio_operacao
	from dbo.atracacao_fato
	where regiao_geografica = 'Nordeste'
	group by ano_inicio_operacao, mes_inicio_operacao
),
cte_brasil as
(
	select 'Brasil' as localidade,
	count(*) as numero_de_atracacoes,
	COUNT(*) - LAG(COUNT(*), 1) OVER (PARTITION BY mes_inicio_operacao ORDER BY ano_inicio_operacao) AS variacao_num_atracacao_ano_anterior,
	avg(tempo_espera_atracacao) as tempo_medio_de_espera,
	avg(tempo_atracado) as tempo_medio_atracado,
	ano_inicio_operacao,
	mes_inicio_operacao
	from dbo.atracacao_fato
	group by ano_inicio_operacao, mes_inicio_operacao
),
cte_union as 
(
select * from cte_ceara
union all
select * from cte_nordeste 
union all 
select * from cte_brasil
)
select * from cte_union where ano_inicio_operacao in (2021, 2023);