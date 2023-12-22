/*
O intuito desta query é auxiliar o time de empréstimo a classificar de forma mais detalhada as politicas de contratação para que seja possível analisar
o comportamento do público após a contratação, como:
- quantidade de contratos por política
- valor dos contratos por política
- limite médio com e sem juros por política
- valor médio das parcelas por política
- prazo médio ponderado por política
- taxa média ponderada por política
- fpd (5,15,30) -  First Payment Default - métrica que aponta o percentual de inadimplência do primeiro pagamento - por política
*/

with 
contratacao_new_policy as (
      select
            from_iso8601_timestamp(loan.termsacceptedbyuserat) as createdat,
            loan.type,
            loan.status,
            loan.ccb,
            loan.amountborrowed,
            loan.installmentplan.amountowed,
            simulation.creditEngineDecisionId,
            simulation.conditions.allowedamountborrowed.max as allowedamountborrowed,
            simulation.conditions.allowedinstallmentvalues.max as allowedinstallmentvalues,
            loan.installmentplan.monthlyinterestpercentagerate,
            loan.installmentplan.numberofinstallments,
            simulation.conditions.policy as policy,
            loan.user.cpf as cpf,
            simulation.conditions.gh as gh,
            simulation.conditions.listCode as listcode,
            simulation.income,
            substring(cast(from_iso8601_timestamp(loan.termsAcceptedByUserAt) AT TIME ZONE 'America/Sao_Paulo' as varchar),1,7) as safra,
            max(i.installmentvalue) as installmentvalue
       from loan_service_athena_prod_796026647601_us_east_1.loan_service_data loan
       left join loan_service_athena_prod_796026647601_us_east_1.loan_service_data simulation on loan.simulationId = simulation.id
       cross join unnest(loan.installments) as t(i)
       where loan.type = 'LOAN' 
	   and loan.status <> 'CANCELLED'
       group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17
      ),
      motor as (
      select
            max(from_iso8601_timestamp(substring(decisiondatetime, 1, 19))) as decisiondatetime,
            pk,
            totalcommitted,
            restrictions,
            sk,
            approved,
            hasrestrictions,
            partition_0
       from credit_engine_service_athena_prod_796026647601_us_east_1.credit_engine_service_dynamodb
      where partition_0 = CAST((current_date - interval '1' day) AS VARCHAR)
      group by 2,3,4,5,6,7,8
      )
      select
      a.createdat,
      a.type,
      a.status,
      a.ccb,
      a.amountborrowed,
      a.amountowed,
      a.creditEngineDecisionId,
      a.policy,
      case when policy = 'PA_CDC_BANQI' then
        case when date(createdat) >= date '2023-11-30' and (listcode is null or listcode ='Motor') then
           case when substr(cpf,6,2) <= '34' then 'PA CDC c/ HBQ0 + CREDILINK'
                when substr(cpf,6,2) <= '44' then 'PA CDC c/ HBQ0 + HRP2'
                else 'PA CDC c/ HBQ0 + CREDILINK'
           end
        when date(createdat) >= date '2023-09-19' and (listcode is null or listcode ='Motor') then
           case when substr(cpf,6,2) = '15' then 'PA CDC c/ HVV8 + PH3A'
                when substr(cpf,6,2) = '14' then 'PA CDC c/ HVV8 + PH3A'
                when substr(cpf,6,2) <= '24' then 'PA CDC c/HVV8 + HRP2'
                when substr(cpf,6,2) <= '44' then 'PA CDC c/ HBQ0'
                when substr(cpf,6,2) <= '64' then 'PA CDC c/ BVS Dig'
                when substr(cpf,6,2) <= '99' then 'PA CDC c/HVV8 + HRP2'
                else 'PA CDC c/HVV8 + HRP2'
           end
        when date(createdat) >= date '2023-07-20' and (listcode is null or listcode ='Motor') then
           case when substr(cpf,6,2) <= '24' then 'PA CDC c/HVV8 + HRP2'
                when substr(cpf,6,2) <= '44' then 'PA CDC c/ HBQ0'
                when substr(cpf,6,2) <= '64' then 'PA CDC c/ BVS Dig'
                when substr(cpf,6,2) <= '99' then 'PA CDC c/HVV8 + HRP2'
                else 'PA CDC c/HVV8 + HRP2'
           end
        when date(createdat) >= date '2023-06-28' and (listcode is null or listcode ='Motor') then
           case when substr(cpf,6,2) <= '24' then 'PA CDC c/HVV8 + HRP2'
                when substr(cpf,6,2) <= '44' then 'PA CDC c/ HBQ0'
                when substr(cpf,6,2) <= '64' then 'PA CDC c/ BVS Dig'
                when substr(cpf,6,2) <= '84' then 'PA CDC c/HVV8 + HRP2'
                else 'PA CDC c/HVV8 + HRP2'
           end
        when date(createdat) >= date '2023-04-24' and (listcode is null or listcode ='Motor') then
           case when substr(cpf,6,2) <= '24' then 'PA CDC c/HVV8'
                when substr(cpf,6,2) <= '44' then 'PA CDC c/ HBQ0'
                when substr(cpf,6,2) <= '64' then 'PA CDC c/ BVS Dig'
                when substr(cpf,6,2) <= '84' then 'PA CDC c/HVV8 + HRP2'
                else 'PA CDC c/HVV8'
           end
        when substring(listcode,1,2)='PB' and listcode >= 'PB040' then
           case when substr(cpf,6,2) <= '24' then 'PA CDC c/HVV8 + HRP2'
                when substr(cpf,6,2) <= '44' then 'PA CDC c/ HBQ0'
                when substr(cpf,6,2) <= '64' then 'PA CDC c/ BVS Dig'
                when substr(cpf,6,2) <= '84' then 'PA CDC c/HVV8 + HRP2'
                else 'PA CDC c/HVV8 + HRP2'
           end
        when substring(listcode,1,2)='PB' and listcode >= 'PB034' then
           case when substr(cpf,6,2) <= '24' then 'PA CDC c/HVV8'
                when substr(cpf,6,2) <= '44' then 'PA CDC c/ HBQ0'
                when substr(cpf,6,2) <= '64' then 'PA CDC c/ BVS Dig'
                when substr(cpf,6,2) <= '84' then 'PA CDC c/HVV8 + HRP2'
                else 'PA CDC c/HVV8'
           end
        when substring(listcode,1,2)='PB' and listcode >= 'PB030' then
         case when substr(cpf,6,2) <= '24' then 'PA CDC c/HVV8'
              when substr(cpf,6,2) <= '64' then 'PA CDC s/HVV8'
              when substr(cpf,6,2) <= '84' then 'PA CDC c/HVV8 + HRP2'
              else 'PA CDC c/HVV8'
         end
        else
          case when date(createdat) >= date '2022-09-16' and listcode <> 'PB021' and (substr(cpf,6,2) <= '24' or substr(cpf,6,2) >= '85') then 'PA CDC c/HVV8'
               else 'PA CDC s/HVV8'
          end
        end
      when ${policy} = 'EP_MANUTENCAO' then
      
         case when date(createdat) >= date '2023-10-23' and (listcode is null or listcode = 'Motor') then
           case when substr(cpf,6,2) in ('40','41','42','43',
           '44','45','46','47',
           '48','49','50','51',
           '52','53','54','55',
           '56','57','58','59',
           '60','61','62','63',
           '64','65','66','67',
           '68','69','70','71',
           '72','73','74','75',
           '76','77','78','79',
           '80','81','82','83',
           '84','85','86','87',
           '88','89') then 'Manutencao C/ BHV 1.0 + HRP2' else 'Manutencao C/ BHV 1.0'
          end
        when substr(listcode,1,2)='MA' and listcode >= 'MA042' then
           case when substr(cpf,6,2) in ('40','41','42','43',
           '44','45','46','47',
           '48','49','50','51',
           '52','53','54','55',
           '56','57','58','59',
           '60','61','62','63',
           '64','65','66','67',
           '68','69','70','71',
           '72','73','74','75',
           '76','77','78','79',
           '80','81','82','83',
           '84','85','86','87',
           '88','89') then 'Manutencao C/ BHV 1.0 + HRP2' else 'Manutencao C/ BHV 1.0'
          end
        when substring(listcode,1,2)='MA' and listcode >= 'MA031' then 'Manutencao C/ BHV 1.0'
        when date(createdat) >= date '2022-06-28' and listcode <> 'MA015' and substr(cpf,6,2) not in (
           '00','01','02',
           '10','11','12',
           '20','21','22','23','24','25',
           '40','41','42','43',
           '55','56','57','58','59',
           '60','61','62','63','64','65','66', '67','68') then 'Manutencao C/ BHV 1.0'
        else 'Manutencao s/ BHV'
        end
        when policy in ('BANQI_MAR_ABERTO','NPA_CDC') then
           case when date(createdat) >= date '2023-07-18'and (listcode is null or listcode ='Motor') then
                case when substr(cpf,6,2) <= '59' then 'Mar Aberto C/ HVV8'
                else 'Mar Aberto C/ HBQ0'
                end
           when substring(listcode,1,2) ='NP' and listcode > 'NP012' then
                case when substr(cpf,6,2) <= '59' then 'Mar Aberto C/ HVV8'
                else 'Mar Aberto C/ HBQ0'
                end
           when date(createdat) >= date '2023-01-01' then 'Mar Aberto C/ HVV8'
           else 'Mar Aberto S/ HVV8'
           end
        else 'Demais Politicas'
      end  as new_policy,
      cpf,
      gh,
      listcode,
      allowedamountborrowed,
      allowedinstallmentvalues,
      monthlyinterestpercentagerate,
      numberofinstallments,
      installmentvalue,
      safra,
      income,
      b.decisiondatetime,
      b.pk,
      b.totalcommitted,
      b.restrictions,
      b.sk,
      b.approved,
      b.hasrestrictions,
      b.partition_0
 from contratacao_new_policy a
 left join motor b on creditEngineDecisionId = b.pk