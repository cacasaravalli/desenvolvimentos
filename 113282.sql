-- Possuí algum Saldo em sua conta (Saldo > 0);  

CREATE TEMP TABLE tmp_cliente_ativo_saldoemconta_2311 AS 
SELECT
	ac."foxId" user_id,
	hb.balance,
	hb."balanceDate" AS balance_date
FROM 
	dock_data.hist_balance hb
LEFT JOIN 
	account_service."Account" ac ON 
		ac."providerUid" = hb."idAccount"::varchar(126)
WHERE 
	hb.balance > 0 AND -- SALDO MAIOR QUE ZERO
	hb."balanceDate" in 
	('2023-01-31','2023-02-28','2023-03-31','2023-04-30','2023-05-31','2023-06-30','2023-07-31','2023-08-31','2023-09-30','2023-10-31') AND
	(NOT EXISTS (
		SELECT 1 
		FROM dock_data.dock_control_accounts dca 
		WHERE hb."idAccount" = dca."idAccount") -- EXCLUINDO CONTAS CONTROLE DOCK
	AND	NOT EXISTS (
		SELECT 1
		FROM account_service."Account" a
		WHERE 
			a."accountType" = 'control' AND 
			hb."idAccount"::character varying(40) = a."providerUid")); -- EXCLUINDO CONTAS CONTROLE BANQI
		
CREATE TABLE business_analytics.tmp_cliente_ativo_saldoemconta AS 			
SELECT DISTINCT user_id, date_trunc('month',balance_date)::date AS safra
FROM tmp_cliente_ativo_saldoemconta_2311;

/*

grant all privileges
	on table business_analytics.tmp_cliente_ativo_saldoemconta
	to business_analytics_role;
*/



--Clientes que geraram alguma Receita;			

CREATE TABLE tmp_cliente_ativo_geramreceita_2311 AS 
SELECT te.user_id,
       te.created_at
  FROM reporting.transactions_ext te 
 WHERE te.status = 'Complete'
   AND te.created_at  BETWEEN '2022-11-01'::DATE  AND '2023-10-31'
   AND te.transaction_type IN ('CdcInstallmentPayment', 'VirtualCardTransaction', 'PhysicalCardTransaction', 'BoletoPayment', 'MobileRecharge', 'TransportationRecharge', 'Marketplace')
UNION ALL   							  
SELECT oc.user_id,
       oc.received_at AS created_at
  FROM node_js.order_created as oc
  JOIN (SELECT DISTINCT order_id 
		  FROM node_js.order_tracking_updated
	 	 WHERE tracking_code = 'PAP' 
	 	   AND tracking_date IS NOT NULL) AS apr ON (oc.order_id = apr.order_id)
 WHERE  oc.received_at  BETWEEN '2022-11-01'::DATE  AND '2023-10-31';	


-- 30

CREATE TABLE business_analytics.tmp_cliente_ativo_geramreceita AS 			
SELECT DISTINCT user_id, date_trunc('month','2023-01-31'::date)  AS safra
FROM tmp_cliente_ativo_geramreceita_2311
WHERE created_at BETWEEN '2023-01-31'::DATE - (30 - 1) AND '2023-01-31'::DATE
UNION 
SELECT DISTINCT user_id, date_trunc('month','2023-02-28'::date)  AS safra
FROM tmp_cliente_ativo_geramreceita_2311
WHERE created_at BETWEEN '2023-02-28'::DATE - (30 - 1) AND '2023-02-28'::DATE
UNION 
SELECT DISTINCT user_id, date_trunc('month','2023-03-31'::date)  AS safra
FROM tmp_cliente_ativo_geramreceita_2311
WHERE created_at BETWEEN '2023-03-31'::DATE - (30 - 1) AND '2023-03-31'::DATE
UNION 
SELECT DISTINCT user_id, date_trunc('month','2023-04-30'::date)  AS safra
FROM tmp_cliente_ativo_geramreceita_2311
WHERE created_at BETWEEN '2023-04-30'::DATE - (30 - 1) AND '2023-04-30'::DATE
UNION 
SELECT DISTINCT user_id, date_trunc('month','2023-05-31'::date)  AS safra
FROM tmp_cliente_ativo_geramreceita_2311
WHERE created_at BETWEEN '2023-05-31'::DATE - (30 - 1) AND '2023-05-31'::DATE
UNION 
SELECT DISTINCT user_id, date_trunc('month','2023-06-30'::date)  AS safra
FROM tmp_cliente_ativo_geramreceita_2311
WHERE created_at BETWEEN '2023-06-30'::DATE - (30 - 1) AND '2023-06-30'::DATE
UNION 
SELECT DISTINCT user_id, date_trunc('month','2023-07-31'::date)  AS safra
FROM tmp_cliente_ativo_geramreceita_2311
WHERE created_at BETWEEN '2023-07-31'::DATE - (30 - 1) AND '2023-07-31'::DATE
UNION 
SELECT DISTINCT user_id, date_trunc('month','2023-08-31'::date)  AS safra
FROM tmp_cliente_ativo_geramreceita_2311
WHERE created_at BETWEEN '2023-08-31'::DATE - (30 - 1) AND '2023-08-31'::DATE
UNION 
SELECT DISTINCT user_id, date_trunc('month','2023-09-30'::date)  AS safra
FROM tmp_cliente_ativo_geramreceita_2311
WHERE created_at BETWEEN '2023-09-30'::DATE - (30 - 1) AND '2023-09-30'::DATE
UNION 
SELECT DISTINCT user_id, date_trunc('month','2023-10-31'::date) AS safra
FROM tmp_cliente_ativo_geramreceita_2311
WHERE created_at BETWEEN '2023-10-31'::DATE - (30 - 1) AND '2023-10-31'::DATE;

-- 90

CREATE TABLE business_analytics.tmp_cliente_ativo_geramreceita_90 AS 			
SELECT DISTINCT user_id, date_trunc('month','2023-01-31'::date)  AS safra
FROM tmp_cliente_ativo_geramreceita_2311
WHERE created_at BETWEEN '2023-01-31'::DATE - (90 - 1) AND '2023-01-31'::DATE
UNION 
SELECT DISTINCT user_id, date_trunc('month','2023-02-28'::date)  AS safra
FROM tmp_cliente_ativo_geramreceita_2311
WHERE created_at BETWEEN '2023-02-28'::DATE - (90 - 1) AND '2023-02-28'::DATE
UNION 
SELECT DISTINCT user_id, date_trunc('month','2023-03-31'::date)  AS safra
FROM tmp_cliente_ativo_geramreceita_2311
WHERE created_at BETWEEN '2023-03-31'::DATE - (90 - 1) AND '2023-03-31'::DATE
UNION 
SELECT DISTINCT user_id, date_trunc('month','2023-04-30'::date)  AS safra
FROM tmp_cliente_ativo_geramreceita_2311
WHERE created_at BETWEEN '2023-04-30'::DATE - (90 - 1) AND '2023-04-30'::DATE
UNION 
SELECT DISTINCT user_id, date_trunc('month','2023-05-31'::date)  AS safra
FROM tmp_cliente_ativo_geramreceita_2311
WHERE created_at BETWEEN '2023-05-31'::DATE - (90 - 1) AND '2023-05-31'::DATE
UNION 
SELECT DISTINCT user_id, date_trunc('month','2023-06-30'::date)  AS safra
FROM tmp_cliente_ativo_geramreceita_2311
WHERE created_at BETWEEN '2023-06-30'::DATE - (90 - 1) AND '2023-06-30'::DATE
UNION 
SELECT DISTINCT user_id, date_trunc('month','2023-07-31'::date)  AS safra
FROM tmp_cliente_ativo_geramreceita_2311
WHERE created_at BETWEEN '2023-07-31'::DATE - (90 - 1) AND '2023-07-31'::DATE
UNION 
SELECT DISTINCT user_id, date_trunc('month','2023-08-31'::date)  AS safra
FROM tmp_cliente_ativo_geramreceita_2311
WHERE created_at BETWEEN '2023-08-31'::DATE - (90 - 1) AND '2023-08-31'::DATE
UNION 
SELECT DISTINCT user_id, date_trunc('month','2023-09-30'::date)  AS safra
FROM tmp_cliente_ativo_geramreceita_2311
WHERE created_at BETWEEN '2023-09-30'::DATE - (90 - 1) AND '2023-09-30'::DATE
UNION 
SELECT DISTINCT user_id, date_trunc('month','2023-10-31'::date) AS safra
FROM tmp_cliente_ativo_geramreceita_2311
WHERE created_at BETWEEN '2023-10-31'::DATE - (90 - 1) AND '2023-10-31'::DATE;


/*

grant all privileges
	on table business_analytics.tmp_cliente_ativo_geramreceita
	to business_analytics_role;
	
grant all privileges
	on table business_analytics.tmp_cliente_ativo_geramreceita_90
	to business_analytics_role;
	
*/

-- Possuí algum produto de crédito (Seguindo a regra de Cobrança, clientes com 31 dias de atraso não serão contabilizados – estes só entrarão novamente quando efetivarem a Renegociação);

CREATE TEMP TABLE base_ep AS 
SELECT DISTINCT a.user_id, a.safra_contratacao ::date AS data_inicio, (a.safra_contratacao ::date + concat(TEXT(a.numberofinstallments),' month') ::INTERVAL)::date AS data_final, 
CASE WHEN b.user_id IS NOT NULL THEN b.safra_inadimplencia ELSE NULL END AS safra_inadimplencia, 'EP' AS produto
  FROM business_analytics.tmp_base_ep AS a 
  LEFT JOIN business_analytics.tmp_base_ep_inadimplencia AS b ON a.user_id = b.user_id AND a.safra_contratacao ::date = b.safra_contratacao ::date;

CREATE TEMP TABLE base_reneg AS
SELECT DISTINCT a.user_id, a.safra_contratacao ::date AS data_inicio, (a.safra_contratacao ::date + concat(TEXT(a.numberofinstallments),' month') ::INTERVAL)::date AS data_final, 
CASE WHEN b.user_id IS NOT NULL THEN b.safra_inadimplencia ELSE NULL END AS safra_inadimplencia, 'RENEG' AS produto
  FROM business_analytics.tmp_base_reneg AS a 
  LEFT JOIN business_analytics.tmp_base_reneg_inadimplencia AS b ON a.user_id = b.user_id AND a.ccb_paradigma = b.ccb_paradigma;


CREATE TEMP TABLE tmp_cliente_ativo_possuicredito_2311 AS 
SELECT *
  FROM base_ep
 UNION 
SELECT *
  FROM base_reneg;	
 
DELETE FROM tmp_cliente_ativo_possuicredito_2311 WHERE safra_inadimplencia IS NOT NULL;

-- 30

CREATE TABLE business_analytics.tmp_cliente_ativo_possuicredito AS
SELECT DISTINCT user_id, date_trunc('month','2023-01-31'::date) AS safra
FROM tmp_cliente_ativo_possuicredito_2311 
WHERE '2023-01-31'::DATE - (30 - 1) BETWEEN data_inicio AND data_final 
UNION
SELECT DISTINCT user_id, date_trunc('month','2023-02-28'::date) AS safra
FROM tmp_cliente_ativo_possuicredito_2311 
WHERE '2023-02-28'::DATE - (30 - 1) BETWEEN data_inicio AND data_final
UNION
SELECT DISTINCT user_id, date_trunc('month','2023-03-31'::date) AS safra
FROM tmp_cliente_ativo_possuicredito_2311 
WHERE '2023-03-31'::DATE - (30 - 1) BETWEEN data_inicio AND data_final
UNION
SELECT DISTINCT user_id, date_trunc('month','2023-04-30'::date) AS safra
FROM tmp_cliente_ativo_possuicredito_2311 
WHERE '2023-04-30'::DATE - (30 - 1) BETWEEN data_inicio AND data_final
UNION
SELECT DISTINCT user_id, date_trunc('month','2023-05-31'::date) AS safra
FROM tmp_cliente_ativo_possuicredito_2311 
WHERE '2023-05-31'::DATE - (30 - 1) BETWEEN data_inicio AND data_final
UNION
SELECT DISTINCT user_id, date_trunc('month','2023-06-30'::date) AS safra
FROM tmp_cliente_ativo_possuicredito_2311 
WHERE '2023-06-30'::DATE - (30 - 1) BETWEEN data_inicio AND data_final
UNION
SELECT DISTINCT user_id, date_trunc('month','2023-07-31'::date) AS safra
FROM tmp_cliente_ativo_possuicredito_2311 
WHERE '2023-07-31'::DATE - (30 - 1) BETWEEN data_inicio AND data_final
UNION
SELECT DISTINCT .user_id, date_trunc('month','2023-08-31'::date) AS safra
FROM tmp_cliente_ativo_possuicredito_2311 
WHERE '2023-08-31'::DATE - (30 - 1) BETWEEN data_inicio AND data_final
UNION
SELECT DISTINCT user_id, date_trunc('month','2023-09-30'::date) AS safra
FROM tmp_cliente_ativo_possuicredito_2311
WHERE '2023-09-30'::DATE - (30 - 1) BETWEEN data_inicio AND data_final
UNION
SELECT DISTINCT user_id, date_trunc('month','2023-10-31'::date) AS safra
FROM tmp_cliente_ativo_possuicredito_2311 
WHERE '2023-10-31'::DATE - (30 - 1) BETWEEN data_inicio AND data_final;


-- 90

CREATE TABLE business_analytics.tmp_cliente_ativo_possuicredito_90 AS
SELECT DISTINCT a.user_id, date_trunc('month','2023-01-31'::date) AS safra
FROM tmp_cliente_ativo_possuicredito_2311 AS a
WHERE '2023-01-31'::DATE - (90 - 1) BETWEEN data_inicio AND data_final 
UNION
SELECT DISTINCT a.user_id, date_trunc('month','2023-02-28'::date) AS safra
FROM tmp_cliente_ativo_possuicredito_2311 AS a
WHERE '2023-02-28'::DATE - (90 - 1) BETWEEN data_inicio AND data_final
UNION
SELECT DISTINCT a.user_id, date_trunc('month','2023-03-31'::date) AS safra
FROM tmp_cliente_ativo_possuicredito_2311 AS a
WHERE '2023-03-31'::DATE - (90 - 1) BETWEEN data_inicio AND data_final
UNION
SELECT DISTINCT a.user_id, date_trunc('month','2023-04-30'::date) AS safra
FROM tmp_cliente_ativo_possuicredito_2311 AS a
WHERE '2023-04-30'::DATE - (90 - 1) BETWEEN data_inicio AND data_final
UNION
SELECT DISTINCT a.user_id, date_trunc('month','2023-05-31'::date) AS safra
FROM tmp_cliente_ativo_possuicredito_2311 AS a
WHERE '2023-05-31'::DATE - (90 - 1) BETWEEN data_inicio AND data_final
UNION
SELECT DISTINCT a.user_id, date_trunc('month','2023-06-30'::date) AS safra
FROM tmp_cliente_ativo_possuicredito_2311 AS a
WHERE '2023-06-30'::DATE - (90 - 1) BETWEEN data_inicio AND data_final
UNION
SELECT DISTINCT a.user_id, date_trunc('month','2023-07-31'::date) AS safra
FROM tmp_cliente_ativo_possuicredito_2311 AS a
WHERE '2023-07-31'::DATE - (90 - 1) BETWEEN data_inicio AND data_final
UNION
SELECT DISTINCT a.user_id, date_trunc('month','2023-08-31'::date) AS safra
FROM tmp_cliente_ativo_possuicredito_2311 AS a
WHERE '2023-08-31'::DATE - (90 - 1) BETWEEN data_inicio AND data_final
UNION
SELECT DISTINCT a.user_id, date_trunc('month','2023-09-30'::date) AS safra
FROM tmp_cliente_ativo_possuicredito_2311 AS a
WHERE '2023-09-30'::DATE - (90 - 1) BETWEEN data_inicio AND data_final
UNION
SELECT DISTINCT a.user_id, date_trunc('month','2023-10-31'::date) AS safra
FROM tmp_cliente_ativo_possuicredito_2311 AS a
WHERE '2023-10-31'::DATE - (90 - 1) BETWEEN data_inicio AND data_final;

/*

grant all privileges
	on table business_analytics.tmp_cliente_ativo_possuicredito
	to business_analytics_role;

grant all privileges
	on table business_analytics.tmp_cliente_ativo_possuicredito_90
	to business_analytics_role;	
*/



-- Clientes que efetuaram algum Login no App.

-- Clientes que efetuaram algum Login no App.

CREATE TEMP TABLE tmp_cliente_ativo_acessoapp_2311 AS 
SELECT DISTINCT a.fox_id  as user_id
     , original_timestamp::DATE AS date
  FROM ios_react.auth_user_login   a
 WHERE a.fox_id is not null
   AND original_timestamp::date BETWEEN '2022-11-01'::DATE AND '2023-10-31'::DATE;

-- 30

CREATE TABLE business_analytics.tmp_cliente_ativo_acessoapp AS 			
SELECT DISTINCT user_id, date_trunc('month','2023-01-31'::date)  AS safra
FROM tmp_cliente_ativo_acessoapp_2311
WHERE date BETWEEN '2023-01-31'::DATE - (30 - 1) AND '2023-01-31'::DATE
UNION 
SELECT DISTINCT user_id, date_trunc('month','2023-02-28'::date)  AS safra
FROM tmp_cliente_ativo_acessoapp_2311
WHERE date BETWEEN '2023-02-28'::DATE - (30 - 1) AND '2023-02-28'::DATE
UNION 
SELECT DISTINCT user_id, date_trunc('month','2023-03-31'::date)  AS safra
FROM tmp_cliente_ativo_acessoapp_2311
WHERE date BETWEEN '2023-03-31'::DATE - (30 - 1) AND '2023-03-31'::DATE
UNION 
SELECT DISTINCT user_id, date_trunc('month','2023-04-30'::date)  AS safra
FROM tmp_cliente_ativo_acessoapp_2311
WHERE date BETWEEN '2023-04-30'::DATE - (30 - 1) AND '2023-04-30'::DATE
UNION 
SELECT DISTINCT user_id, date_trunc('month','2023-05-31'::date)  AS safra
FROM tmp_cliente_ativo_acessoapp_2311
WHERE date BETWEEN '2023-05-31'::DATE - (30 - 1) AND '2023-05-31'::DATE
UNION 
SELECT DISTINCT user_id, date_trunc('month','2023-05-31'::date)  AS safra
FROM tmp_cliente_ativo_acessoapp_2311
WHERE date BETWEEN '2023-05-31'::DATE - (30 - 1) AND '2023-05-31'::DATE
UNION 
SELECT DISTINCT user_id, date_trunc('month','2023-06-30'::date)  AS safra
FROM tmp_cliente_ativo_acessoapp_2311
WHERE date BETWEEN '2023-06-30'::DATE - (30 - 1) AND '2023-06-30'::DATE
UNION 
SELECT DISTINCT user_id, date_trunc('month','2023-07-31'::date)  AS safra
FROM tmp_cliente_ativo_acessoapp_2311
WHERE date BETWEEN '2023-07-31'::DATE - (30 - 1) AND '2023-07-31'::DATE
UNION 
SELECT DISTINCT user_id, date_trunc('month','2023-08-31'::date)  AS safra
FROM tmp_cliente_ativo_acessoapp_2311
WHERE date BETWEEN '2023-08-31'::DATE - (30 - 1) AND '2023-08-31'::DATE
UNION 
SELECT DISTINCT user_id, date_trunc('month','2023-09-30'::date)  AS safra
FROM tmp_cliente_ativo_acessoapp_2311
WHERE date BETWEEN '2023-09-30'::DATE - (30 - 1) AND '2023-09-30'::DATE
UNION 
SELECT DISTINCT user_id, date_trunc('month','2023-10-31'::date)  AS safra
FROM tmp_cliente_ativo_acessoapp_2311
WHERE date BETWEEN '2023-10-31'::DATE - (30 - 1) AND '2023-10-31'::DATE;

-- 90
  
CREATE TABLE business_analytics.tmp_cliente_ativo_acessoapp_90 AS 			
SELECT DISTINCT user_id, date_trunc('month','2023-01-31'::date)  AS safra
FROM tmp_cliente_ativo_acessoapp_2311
WHERE date BETWEEN '2023-01-31'::DATE - (90 - 1) AND '2023-01-31'::DATE
UNION 
SELECT DISTINCT user_id, date_trunc('month','2023-02-28'::date)  AS safra
FROM tmp_cliente_ativo_acessoapp_2311
WHERE date BETWEEN '2023-02-28'::DATE - (90 - 1) AND '2023-02-28'::DATE
UNION 
SELECT DISTINCT user_id, date_trunc('month','2023-03-31'::date)  AS safra
FROM tmp_cliente_ativo_acessoapp_2311
WHERE date BETWEEN '2023-03-31'::DATE - (90 - 1) AND '2023-03-31'::DATE
UNION 
SELECT DISTINCT user_id, date_trunc('month','2023-04-30'::date)  AS safra
FROM tmp_cliente_ativo_acessoapp_2311
WHERE date BETWEEN '2023-04-30'::DATE - (90 - 1) AND '2023-04-30'::DATE
UNION 
SELECT DISTINCT user_id, date_trunc('month','2023-05-31'::date)  AS safra
FROM tmp_cliente_ativo_acessoapp_2311
WHERE date BETWEEN '2023-05-31'::DATE - (90 - 1) AND '2023-05-31'::DATE
UNION 
SELECT DISTINCT user_id, date_trunc('month','2023-05-31'::date)  AS safra
FROM tmp_cliente_ativo_acessoapp_2311
WHERE date BETWEEN '2023-05-31'::DATE - (90 - 1) AND '2023-05-31'::DATE
UNION 
SELECT DISTINCT user_id, date_trunc('month','2023-06-30'::date)  AS safra
FROM tmp_cliente_ativo_acessoapp_2311
WHERE date BETWEEN '2023-06-30'::DATE - (90 - 1) AND '2023-06-30'::DATE
UNION 
SELECT DISTINCT user_id, date_trunc('month','2023-07-31'::date)  AS safra
FROM tmp_cliente_ativo_acessoapp_2311
WHERE date BETWEEN '2023-07-31'::DATE - (90 - 1) AND '2023-07-31'::DATE
UNION 
SELECT DISTINCT user_id, date_trunc('month','2023-08-31'::date)  AS safra
FROM tmp_cliente_ativo_acessoapp_2311
WHERE date BETWEEN '2023-08-31'::DATE - (90 - 1) AND '2023-08-31'::DATE
UNION 
SELECT DISTINCT user_id, date_trunc('month','2023-09-30'::date)  AS safra
FROM tmp_cliente_ativo_acessoapp_2311
WHERE date BETWEEN '2023-09-30'::DATE - (90 - 1) AND '2023-09-30'::DATE
UNION 
SELECT DISTINCT user_id, date_trunc('month','2023-10-31'::date)  AS safra
FROM tmp_cliente_ativo_acessoapp_2311
WHERE date BETWEEN '2023-10-31'::DATE - (90 - 1) AND '2023-10-31'::DATE;


/*
grant all privileges
	on table business_analytics.tmp_cliente_ativo_acessoapp
	to business_analytics_role;
	
grant all privileges
	on table business_analytics.tmp_cliente_ativo_acessoapp_90
	to business_analytics_role;
*/

------------------------------------------------------------------------------------------------------------
-- 30

CREATE TABLE business_analytics.cliente_ativo AS 
SELECT *, 'saldo em conta' AS origem
FROM business_analytics.tmp_cliente_ativo_saldoemconta
UNION 
SELECT *, 'gerou receita' AS origem
FROM business_analytics.tmp_cliente_ativo_geramreceita
UNION 
SELECT *, 'possui crédito' AS origem
FROM business_analytics.tmp_cliente_ativo_possuicredito
UNION
SELECT *, 'acessou o app' AS origem
FROM business_analytics.tmp_cliente_ativo_acessoapp ;


grant all privileges
	on table business_analytics.cliente_ativo
	to business_analytics_role;
	

SELECT safra::date, count(DISTINCT user_id)
FROM business_analytics.cliente_ativo
GROUP BY 1

WITH 
cte AS (
SELECT safra, user_id, count(*)
FROM business_analytics.cliente_ativo
GROUP BY 1,2
HAVING count (*) = 4
)

SELECT safra, count(DISTINCT user_id)
FROM cte
GROUP BY 1

-- 90

CREATE TABLE business_analytics.cliente_ativo_90 AS 
SELECT *, 'saldo em conta' AS origem
FROM business_analytics.tmp_cliente_ativo_saldoemconta
UNION 
SELECT *, 'gerou receita' AS origem
FROM business_analytics.tmp_cliente_ativo_geramreceita_90
UNION 
SELECT *, 'possui crédito' AS origem
FROM business_analytics.tmp_cliente_ativo_possuicredito_90
UNION
SELECT *, 'acessou o app' AS origem
FROM business_analytics.tmp_cliente_ativo_acessoapp_90 ;


grant all privileges
	on table business_analytics.cliente_ativo_90
	to business_analytics_role;
	


SELECT safra::date, count(DISTINCT user_id)
FROM business_analytics.cliente_ativo_90
GROUP BY 1

WITH 
cte AS (
SELECT safra, user_id, count(*)
FROM business_analytics.cliente_ativo_90
GROUP BY 1,2
HAVING count (*) = 1
)

SELECT safra, count(DISTINCT user_id)
FROM cte
GROUP BY 1

----------------------------------------------------------------------------------------------
-- 30

CREATE TEMP TABLE tmp_analise_cliente_ativo_2411 AS
WITH 
saldoemconta AS (
SELECT DISTINCT  safra, user_id
FROM business_analytics.cliente_ativo
WHERE origem = 'saldo em conta' 
),
geroureceita AS (
SELECT DISTINCT  safra, user_id
FROM business_analytics.cliente_ativo
WHERE origem = 'gerou receita' 
),
possuicredito AS (
SELECT DISTINCT  safra, user_id
FROM business_analytics.cliente_ativo
WHERE origem = 'possui crédito' 
),
acessouapp AS (
SELECT DISTINCT  safra, user_id
FROM business_analytics.cliente_ativo
WHERE origem = 'acessou o app' 
),

distinto AS (
SELECT DISTINCT safra, user_id
FROM business_analytics.cliente_ativo
)

SELECT a.*,
CASE WHEN b.user_id IS NOT NULL THEN TRUE ELSE FALSE END "saldo em conta",
CASE WHEN c.user_id IS NOT NULL THEN TRUE ELSE FALSE END "gerou receita",
CASE WHEN d.user_id IS NOT NULL THEN TRUE ELSE FALSE END "possui crédito",
CASE WHEN e.user_id IS NOT NULL THEN TRUE ELSE FALSE END "acessou o app"
FROM distinto AS a 
LEFT JOIN saldoemconta AS b ON a.user_id = b.user_id AND a.safra = b.safra
LEFT JOIN geroureceita AS c ON a.user_id = c.user_id AND a.safra = c.safra
LEFT JOIN possuicredito AS d ON a.user_id = d.user_id AND a.safra = d.safra
LEFT JOIN acessouapp AS e ON a.user_id = e.user_id AND a.safra = e.safra;

SELECT safra::date,
"saldo em conta",
"gerou receita",
"possui crédito", 
"acessou o app",
count(DISTINCT user_id) AS usuarios
FROM tmp_analise_cliente_ativo_2411
GROUP BY 1,2,3,4,5

-- 90
CREATE TEMP TABLE tmp_analise_cliente_ativo_90_2411 AS
WITH 
saldoemconta AS (
SELECT DISTINCT  safra, user_id
FROM business_analytics.cliente_ativo_90
WHERE origem = 'saldo em conta' 
),
geroureceita AS (
SELECT DISTINCT  safra, user_id
FROM business_analytics.cliente_ativo_90
WHERE origem = 'gerou receita' 
),
possuicredito AS (
SELECT DISTINCT  safra, user_id
FROM business_analytics.cliente_ativo_90
WHERE origem = 'possui crédito' 
),
acessouapp AS (
SELECT DISTINCT  safra, user_id
FROM business_analytics.cliente_ativo_90
WHERE origem = 'acessou o app' 
),

distinto AS (
SELECT DISTINCT safra, user_id
FROM business_analytics.cliente_ativo_90
)

SELECT a.*,
CASE WHEN b.user_id IS NOT NULL THEN TRUE ELSE FALSE END "saldo em conta",
CASE WHEN c.user_id IS NOT NULL THEN TRUE ELSE FALSE END "gerou receita",
CASE WHEN d.user_id IS NOT NULL THEN TRUE ELSE FALSE END "possui crédito",
CASE WHEN e.user_id IS NOT NULL THEN TRUE ELSE FALSE END "acessou o app"
FROM distinto AS a 
LEFT JOIN saldoemconta AS b ON a.user_id = b.user_id AND a.safra = b.safra
LEFT JOIN geroureceita AS c ON a.user_id = c.user_id AND a.safra = c.safra
LEFT JOIN possuicredito AS d ON a.user_id = d.user_id AND a.safra = d.safra
LEFT JOIN acessouapp AS e ON a.user_id = e.user_id AND a.safra = e.safra;


SELECT safra::date,
"saldo em conta",
"gerou receita",
"possui crédito", 
"acessou o app",
count(DISTINCT user_id) AS usuarios
FROM tmp_analise_cliente_ativo_90_2411
GROUP BY 1,2,3,4,5;