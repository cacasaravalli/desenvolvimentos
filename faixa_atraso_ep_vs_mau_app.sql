/*
O intuito desta query é ajudar o time de produto a entender quantos clientes que estão inadimplentes no empréstimo e ainda fazem parte do MAU (Monthly active users) e quantos seguem acessando o aplicativo para que seja possível realizar 
uma comunicação direcionada à este público
*/

WITH base_ep AS (
SELECT
current_date AS ref,
'LOAN' AS produto,
loan.user_cpf as cpf,
loan.user_fox_id,
loan.ccb,
loan.ccb AS ccb_paradigma,
loan.amount_borrowed AS amountborrowed,
installment_plan_monthly_interest_percentage_rate AS monthlyinterestpercentagerate,
installment_plan_number_of_installments AS numberofinstallments,
installment_plan_amount_owed AS amountowed,
installment_plan_installment_value AS installmentvalue,
installment_number AS installmentNumber,
DATE(terms_accepted_by_user_at) AS createdat,
due_date AS duedate,
paid_at AS paidat,
loan.status,
'MOBILE' AS canal,
CASE WHEN installment_payment_amount IS NOT NULL THEN installment_payment_amount
     ELSE 0 end as valor_pago,
inst.status AS status_parcela,
provider_proposal_id AS providerproposalid
FROM processed.loan_service.loan_service AS loan
LEFT JOIN processed.loan_service.loan_service_installments AS inst on loan.id = inst.id
WHERE loan.type = 'LOAN'
AND loan.status IN ('ACTIVE')
),

ep_final_min AS (
  SELECT DISTINCT user_fox_id,
  status,
  status_parcela,
  min(duedate) installmentDueDate_min
  FROM base_ep
  WHERE status_parcela IN ('LATE')
  GROUP BY 1,2,3
  ),

ep_tempo_atraso AS (
SELECT *,
datediff(current_date,installmentDueDate_min) as tempo_atraso,
CASE WHEN datediff(current_date,installmentDueDate_min) BETWEEN 0 AND 15 THEN '0 A 15'
     WHEN datediff(current_date,installmentDueDate_min) BETWEEN 16 AND 30 THEN '16 A 30'
     WHEN datediff(current_date,installmentDueDate_min) BETWEEN 31 AND 60 THEN '31 A 60'
     WHEN datediff(current_date,installmentDueDate_min) BETWEEN 61 AND 90 THEN '61 A 90'
     WHEN datediff(current_date,installmentDueDate_min) BETWEEN 91 AND 180 THEN '91 A 180'
     ELSE '> 180'
END range_tempo_atraso
FROM ep_final_min
),

mau (
select distinct user_id
from analytics.active_users.active_users_30days_window
where date = current_date - '1 day'::interval
),

acessoapp AS (

SELECT DISTINCT a.fox_id as user_id
  FROM processed.frontend_events.auth_user_login AS a
 WHERE a.fox_id IS NOT NULL
   AND a.fox_id <> ''
   AND a.original_timestamp::date BETWEEN (current_date - '1 day'::INTERVAL)::date  - (30 - 1) AND (current_date - '1 day'::INTERVAL)::date

)

select range_tempo_atraso, count(distinct b.user_id) as mau,  count(distinct c.user_id) as acessou_app,  count(distinct a.user_fox_id) as inadimplentes
from ep_tempo_atraso as a
left join mau as b on a.user_fox_id = b.user_id
left join acessoapp as c on a.user_fox_id = c.user_id
group by 1
