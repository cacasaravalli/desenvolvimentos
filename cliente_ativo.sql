DELETE FROM sandbox.business_analytics.rps_cliente_ativo WHERE safra = date_trunc('MONTH',(current_date - '1 day'::INTERVAL));

INSERT INTO sandbox.business_analytics.rps_cliente_ativo
WITH 
tmp_cliente_ativo_saldoemconta AS (
SELECT DISTINCT ac.fox_id AS user_id,
       date_trunc('month',(current_date - '1 day'::INTERVAL)::date) as safra
  FROM processed.dock_service.balance_hist AS hb
  LEFT JOIN processed.account_service.account AS ac
    ON hb.id_account::varchar(126) = ac.provider_uid
 WHERE hb.balance > 0
   AND hb.balance_date = (SELECT MAX(balance_date) FROM processed.dock_service.balance_hist)
   AND (NOT EXISTS (SELECT 1 
                     FROM processed.dock_service.control_accounts AS dca
                    WHERE hb.id_account = dca.id_account) -- EXCLUINDO CONTAS CONTROLE DOCK
   AND NOT EXISTS (SELECT 1 
                     FROM processed.account_service.account as a
                    WHERE a.account_type = 'control'
                      AND hb.id_account::varchar(126) = a.provider_uid))  -- EXCLUINDO CONTAS CONTROLE BANQI
), 

tmp_cliente_ativo_geramreceita AS (
SELECT DISTINCT tr.user_id,
       date_trunc('month',(current_date - '1 day'::INTERVAL)::date) as safra
  FROM processed.transaction_list_service.transactions AS tr
 WHERE tr.status = 'Complete'
   AND tr.created_at BETWEEN (current_date - '1 day'::INTERVAL)::date  - (30 - 1) AND (current_date - '1 day'::INTERVAL)::date 
   AND tr.transaction_type IN ('CdcInstallmentPayment', 'VirtualCardTransaction', 'PhysicalCardTransaction', 'BoletoPayment', 'MobileRecharge', 'TransportationRecharge', 'Marketplace')
UNION ALL
SELECT DISTINCT oc.user_id,
       date_trunc('month',(current_date - '1 day'::INTERVAL)::date) as safra
  FROM processed.backend_events.order_created as oc
  JOIN (SELECT DISTINCT properties_order_id
          FROM processed.backend_events.order_tracking_updated
         WHERE properties_tracking_code = 'PAP' 
           AND properties_tracking_date IS NOT NULL) as apr ON oc.properties_order_id = apr.properties_order_id
 WHERE oc.received_at BETWEEN (current_date - '1 day'::INTERVAL)::date  - (30 - 1) AND (current_date - '1 day'::INTERVAL)::date 

),

tmp_cliente_ativo_acessoapp AS (

SELECT DISTINCT a.fox_id as user_id, 
       date_trunc('month',(current_date - '1 day'::INTERVAL)::date) as safra
  FROM processed.frontend_events.auth_user_login AS a
 WHERE a.fox_id IS NOT NULL 
   AND a.fox_id <> ''
   AND a.original_timestamp::date BETWEEN (current_date - '1 day'::INTERVAL)::date  - (30 - 1) AND (current_date - '1 day'::INTERVAL)::date

),

emprestimos_ativos as (
select id,
user_fox_id,
created_at ::date as data_inicio,
ccb,
type,
status, 
installment_plan_number_of_installments, 
(created_at ::date + (to_varchar(installment_plan_number_of_installments,99) || ' month')::interval)::date as data_final
from processed.loan_service.loan_service
where type = 'LOAN' 
and status = 'ACTIVE'  
),

parcelas_atrasadas_emprestimo as (
select id,
installment_number,
installment_value,
due_date,
paid_at,
status
from processed.loan_service.loan_service_installments 
where status = 'LATE'
), 

tmp_emprestimos_ativos as (

select *
from emprestimos_ativos as a 
where not exists (select 1 from parcelas_atrasadas_emprestimo as b where a.id = b.id)

),

reneg_ativa as (
SELECT
billing.type,
CASE WHEN billing.type = 'RENEGOTIATION_LOAN' THEN billing.CCB ELSE billing.loan_ccb END AS ccb,
billing.status,
billing.id,
billing.user_fox_id,
billing.number_of_installments,
billing.agreed_at::date as data_inicio,
(billing.agreed_at ::date + (to_varchar(billing.number_of_installments,99) || ' month')::interval)::date as data_final
FROM processed.billing_service.billing_service AS billing
WHERE billing.type IN ('AGREEMENT_LOAN', 'RENEGOTIATION_LOAN')
AND billing.status IN ('ACTIVE','WITH_AGREEMENT')
),

parcelas_atrasadas as (
SELECT id, 
amountPaid as installmentPaymentAmount,
date(installmentduedate) as installmentDueDate,
installmentnumber,
date(installmentpaidat) as installmentPaidAt,
installmentvalue as installmentValue,
installmentboletostatus as status_parcela
FROM analytics.billing.installments_agreement_type
WHERE installmentboletostatus = 'LATE'
UNION 
SELECT id,
amount_agreed as installmentPaymentAmount,
date(installmentduedate) as installmentDueDate,
installmentnumber,
date(installmentpaidat) as installmentPaidAt,
installmentvalue,
installmentstatus as status_parcela
FROM analytics.billing.installments_renagociation_loan_type
WHERE installmentstatus  = 'LATE'
),

tmp_renegs_ativas as (
select *
from reneg_ativa as a 
where not exists (select 1 from parcelas_atrasadas as b where a.id = b.id)
),

tmp_cliente_ativo_possuicredito AS (
SELECT DISTINCT user_fox_id AS user_id, 
       date_trunc('month',(current_date - '1 day'::INTERVAL)::date) as safra
  FROM tmp_emprestimos_ativos
 WHERE (current_date - '1 day'::INTERVAL)::date - (30 - 1) BETWEEN data_inicio AND data_final 
UNION
SELECT DISTINCT user_fox_id AS user_id,
       date_trunc('month',(current_date - '1 day'::INTERVAL)::date) as safra
  FROM tmp_renegs_ativas
  WHERE (current_date - '1 day'::INTERVAL)::date - (30 - 1) BETWEEN data_inicio AND data_final 
),

tmp_cliente_ativo AS (
SELECT *, 'saldo em conta' AS origem
  FROM tmp_cliente_ativo_saldoemconta
UNION   
SELECT *, 'gerou receita' AS origem
  FROM tmp_cliente_ativo_geramreceita
UNION 
SELECT *, 'acessou o app' AS origem
  FROM tmp_cliente_ativo_acessoapp
UNION 
SELECT *, 'possui crédito' AS origem
  FROM tmp_cliente_ativo_possuicredito
),

saldoemconta AS (
SELECT DISTINCT  safra, user_id
FROM tmp_cliente_ativo
WHERE origem = 'saldo em conta' 
),
geroureceita AS (
SELECT DISTINCT  safra, user_id
FROM tmp_cliente_ativo
WHERE origem = 'gerou receita' 
),
possuicredito AS (
SELECT DISTINCT  safra, user_id
FROM tmp_cliente_ativo
WHERE origem = 'possui crédito' 
),
acessouapp AS (
SELECT DISTINCT  safra, user_id
FROM tmp_cliente_ativo
WHERE origem = 'acessou o app' 
),

distinto AS (
SELECT DISTINCT safra, user_id
FROM tmp_cliente_ativo
)

SELECT DISTINCT a.user_id, 
a.safra::date, 
CASE WHEN b.user_id IS NOT NULL THEN TRUE ELSE FALSE END AS saldo_em_conta,
CASE WHEN c.user_id IS NOT NULL THEN TRUE ELSE FALSE END AS gerou_receita,
CASE WHEN d.user_id IS NOT NULL THEN TRUE ELSE FALSE END AS possui_credito,
CASE WHEN e.user_id IS NOT NULL THEN TRUE ELSE FALSE END AS acessou_o_app
FROM distinto AS a 
LEFT JOIN saldoemconta AS b ON a.user_id = b.user_id AND a.safra = b.safra
LEFT JOIN geroureceita AS c ON a.user_id = c.user_id AND a.safra = c.safra
LEFT JOIN possuicredito AS d ON a.user_id = d.user_id AND a.safra = d.safra
LEFT JOIN acessouapp AS e ON a.user_id = e.user_id AND a.safra = e.safra;
