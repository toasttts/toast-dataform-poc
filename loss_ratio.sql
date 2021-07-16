---------------------------------------- Contents ----------------------------------------
---- 01. loss_ratio_by_policy_and_dayend -- from -- report_loss_ratio_dayend
---- 02. loss_ratio_by_policy_and_accident -- from -- report_insured_port_loss_by_acc_month
---- 03. claim_by_policy_and_dayend -- from -- report_nci_view
---- 04. combined_loss_ratio_by_policy -- from -- report_combined_loss_ratio
------------------------------------------------------------------------------------------

------------------------------------------------------------------------------------------
---- 01. loss_ratio_by_policy_and_dayend
------------------------------------------------------------------------------------------
-----EXISTING ASSUMPTIONS-----
--1.This view split data at dayend date only, no first issue or effective
--2.This view assume subclass 90D (DTAC Dosmetic Travel) and 90T (Worldwide Individual Trave Insurance Policy (DTAC)) has only 1 customer always
--3.Add back number customer direct from gisweb to calc exposure unit
----REMARK----
--1.Policy Level (UW Year&Month --> First Issue Year&Month --> Dayend Year&Month --> Effective Year&Month (Endorsement Serial))
--2.Claim by Policy on Endorsement Serial.0 or is_endorsement = false 



------------------------------------------------------------------------------------------
---- 02. loss_ratio_by_policy_and_accident
------------------------------------------------------------------------------------------
----REMARK----
--1.Policy Level (Accident Year&Month (Earned Prem Year&Month and Claim Occur Year&Month))
--2.Earned Unit Exposure is full of each month.

--create as
with prem_data_range as
(
    select *,
           (case
                when total_covered_day = 0 then 0
                else net_premium / total_covered_day * days_of_month_range end)      as earned_premium_eff, --proportion of net premium each covered month
           (case
                when total_covered_day = 0 then 0
                else retained_premium / total_covered_day * days_of_month_range end) as retained_earned_premium_eff,
           (case
                when total_covered_day = 0 then 0
                else ceded_premium / total_covered_day * days_of_month_range end)    as ceded_earned_premium_eff
    from (
             select *,
                    date_part('day', date_trunc('day', expiry_dt) - date_trunc('day', effect_dt)) +
                    1       as total_covered_day,  --Number of covered days (Exp. - Eff. Date + 1)
                    case
                        when date_trunc('month', effect_dt::date) = date_trunc('month', expiry_dt::date) then
                                date_part('day', date_trunc('day', expiry_dt) - date_trunc('day', effect_dt)) + 1
                        when effect_dt::date > date_trunc('month', earned_premium_month)
                            then extract(day from earned_premium_month - effect_dt::date) + 1
                        when expiry_dt::date < earned_premium_month then
                                extract(day from expiry_dt::date - date_trunc('month', earned_premium_month)) + 1
                        else extract(day from earned_premium_month - date_trunc('month', earned_premium_month)) + 1
                        end as days_of_month_range --Number of days in covered month
             from (
                      select report_date,
                             policy_serial,
                             policy_no,
                             endorsement_serial,
                             case
                                 when first_issue_dt < '2017-01-01' then 'KIT Others - pre 2017'
                                 else channel end       as channel,
                             programme,
                             main_class,
                             subclass,
                             subclass_isb,
                             subclass_name_th,
                             subclass_name_en,
                             policy_package,
                             is_gisweb_policy,
                             agent_code,
                             agent_name,
                             cust_id,
                             uw_year,
                             is_endorsement,
                             number_policy,
                             number_policy_net_cancelation,
                             number_customer,
                             number_customer_net_cancelation,
                             create_dt,
                             propose_dt,
                             effect_dt,
                             expiry_dt,
                             issue_dt,
                             dayend_dt,
                             cancel_dt,
                             first_issue_dt,
                             first_effect_dt,
                             cancel_flag,
                             sum_insured,
                             net_premium,
                             ceded_premium,
                             retained_premium,
                             earned_premium_pcnt,
                             earned_premium,
                             earned_premium_retained,
                             unit_exposure_unnormalized,
                             unit_exposure,
                             earned_unit_exposure,
                             stamp_duty,
                             vat_amount,
                             total_premium_after_tax,
                             comm1_amount,
                             comm2_amount,
                             community_bonus,
                             reinsured_commission,
                             reinsured_prdep_amount,
                             dayend_year,
                             dayend_month,
                             effective_year,
                             effective_month,
                             first_issue_year,
                             first_issue_month,
                             renew_serial,
                             renew_policy,
                             renew_status,
                             generate_series(date_trunc('month', effect_dt::date), expiry_dt::date, '1 month')::date +
                             interval '1 month - 1 day' as earned_premium_month --last day of each month that endorsements effective.
                      from sunday.insured_port_earned_prem pol
                      ) a
         ) a
)
, prem as
(
    select  policy_serial,
            policy_no,
            channel,
            programme,
            main_class,
            subclass,
            subclass_name_en,
            subclass_name_th,
            policy_package,
            is_gisweb_policy,
            agent_code,
            is_endorsement,
            first_issue_year,
            first_issue_month,
            date_part('year', earned_premium_month)  earned_premium_year,
            date_part('month', earned_premium_month) earned_premium_month,
            sum(earned_premium_eff)                  earned_premium,
            sum(retained_earned_premium_eff)         retained_earned_premium,
            sum(ceded_earned_premium_eff)            ceded_earned_premium,
            max(days_of_month_range)                 days_of_month_range,
            max(total_covered_day)                   total_covered_day,
            sum(unit_exposure_unnormalized)          unit_exposure_unnormalized,
            sum(unit_exposure)                       unit_exposure,
            sum(case
                    when total_covered_day = 0 then 0
                    else unit_exposure * days_of_month_range / total_covered_day --365
                end) as                              earned_unit_exposure
    from prem_data_range
    group by    policy_serial,
                policy_no,
                channel,
                programme,
                main_class,
                subclass,
                subclass_name_en,
                subclass_name_th,
                policy_package,
                is_gisweb_policy,
                agent_code,
                first_issue_year,
                first_issue_month,
                is_endorsement,
                date_part('year', earned_premium_month),
                date_part('month', earned_premium_month)
)
-- Reserveadj
, claim_reserveadj as 
(
    select policy_no,
           channel,
           programme,
           main_class,
           subclass,
           policy_package,
           is_gisweb_policy,
           agent_code,
           date_part('year',policy_first_issue_dt) first_issue_year,
           date_part('month',policy_first_issue_dt) first_issue_month,
           case
               when date_part('year', claim_occur_dt) = 2 then 2002
               else date_part('year', claim_occur_dt) end as     claim_occur_year,
           date_part('month', claim_occur_dt)                    claim_occur_month,
           sum(coalesce(claim_base_amount, 0))                   claim_base_amount,
           sum(coalesce(claim_survey_amount, 0))                 claim_survey_amount,
           sum(coalesce(claim_net_loss, 0))                      claim_net_loss,
           sum(coalesce(claim_retained_base_amount, 0))          claim_retained_base_amount,
           sum(coalesce(claim_retained_survey_amount, 0))        claim_retained_survey_amount,
           sum(coalesce(claim_retained_net_loss, 0))             claim_retained_net_loss,
           sum(coalesce(claim_ceded_base_amount, 0))             claim_ceded_base_amount,
           sum(coalesce(claim_ceded_survey_amount, 0))           claim_ceded_survey_amount,
           sum(coalesce(claim_ceded_net_loss, 0))                claim_ceded_net_loss,
           sum(coalesce(claim_recovery_base_amount, 0))          claim_recovery_base_amount,
           sum(coalesce(claim_salvage_amount, 0))                claim_salvage_amount,
           sum(coalesce(claim_excess_amount, 0))                 claim_excess_amount,
           sum(coalesce(claim_recovery_net, 0))                  claim_recovery_net,
           sum(coalesce(claim_retained_recovery_base_amount, 0)) claim_retained_recovery_base_amount,
           sum(coalesce(claim_retained_salvage_amount, 0))       claim_retained_salvage_amount,
           sum(coalesce(claim_retained_excess_amount, 0))        claim_retained_excess_amount,
           sum(coalesce(claim_retained_recovery_net, 0))         claim_retained_recovery_net,
           sum(coalesce(claim_ceded_recovery_base_amount, 0))    claim_ceded_recovery_base_amount,
           sum(coalesce(claim_ceded_salvage_amount, 0))          claim_ceded_salvage_amount,
           sum(coalesce(claim_ceded_excess_amount, 0))           claim_ceded_excess_amount,
           sum(coalesce(claim_ceded_recovery_net, 0))            claim_ceded_recovery_net,
           count(distinct claim_no)                              number_claim
    from sunday.insured_port_claim_reserveadj
     --where claim_occur_dt > '2016-12-31'
    group by policy_no,
             channel,
             programme,
             main_class,
             subclass,
             agent_code,
             policy_package,
             is_gisweb_policy,
             date_part('year',policy_first_issue_dt),
             date_part('month',policy_first_issue_dt),
             date_part('year', claim_occur_dt),
             date_part('month', claim_occur_dt)
)
--Claim paid
, claim_paid as 
(
    select policy_no,
           channel,
           programme,
           main_class,
           subclass,
           policy_package,
           is_gisweb_policy,
           agent_code,
           case
               when date_part('year', claim_occur_dt) = 2 then 2002
               else date_part('year', claim_occur_dt) end as   claim_occur_year,
           date_part('month', claim_occur_dt)                  claim_occur_month,
           sum(coalesce(paid_claim_base_amount, 0))            paid_claim_base_amount,
           sum(coalesce(paid_claim_survey_amount, 0))          paid_claim_survey_amount,
           sum(coalesce(paid_claim_net_loss, 0))               paid_claim_net_loss,
           sum(coalesce(paid_claim_vat_amount, 0))             paid_claim_vat_amount,
           sum(coalesce(paid_claim_total, 0))                  paid_claim_total,
           sum(coalesce(paid_claim_retained_base_amount, 0))   paid_claim_retained_base_amount,
           sum(coalesce(paid_claim_retained_survey_amount, 0)) paid_claim_retained_survey_amount,
           sum(coalesce(paid_claim_retained_net_loss, 0))      paid_claim_retained_net_loss,
           sum(coalesce(paid_claim_ceded_base_amount, 0))      paid_claim_ceded_base_amount,
           sum(coalesce(paid_claim_ceded_survey_amount, 0))    paid_claim_ceded_survey_amount,
           sum(coalesce(paid_claim_ceded_net_loss, 0))         paid_claim_ceded_net_loss,
           count(distinct claim_no)                            number_claim
    from sunday.insured_port_claim_paid
         --where claim_occur_dt > '2016-12-31'
    group by policy_no,
             channel,
             programme,
             main_class,
             subclass,
             agent_code,
             policy_package,
             is_gisweb_policy,
             date_part('year', claim_occur_dt),
             date_part('month', claim_occur_dt)
)
--Recovery paid
, recovery_paid as 
(
    select policy_no,
           channel,
           programme,
           main_class,
           subclass,
           policy_package,
           is_gisweb_policy,
           agent_code,
           case
               when date_part('year', claim_occur_dt) = 2 then 2002
               else date_part('year', claim_occur_dt) end as    claim_occur_year,
           date_part('month', claim_occur_dt)                   claim_occur_month,
           sum(coalesce(paid_recovery_base_amount, 0))          paid_recovery_base_amount,
           sum(coalesce(paid_salvage_amount, 0))                paid_salvage_amount,
           sum(coalesce(paid_excess_amount, 0))                 paid_excess_amount,
           sum(coalesce(paid_recovery_net, 0))                  paid_recovery_net,
           sum(coalesce(paid_recovery_vat_amount, 0))           paid_recovery_vat_amount,
           sum(coalesce(paid_recovery_total, 0))                paid_recovery_total,
           sum(coalesce(paid_retained_recovery_base_amount, 0)) paid_retained_recovery_base_amount,
           sum(coalesce(paid_retained_salvage_amount, 0))       paid_retained_salvage_amount,
           sum(coalesce(paid_retained_excess_amount, 0))        paid_retained_excess_amount,
           sum(coalesce(paid_retained_recovery_net, 0))         paid_retained_recovery_net,
           sum(coalesce(paid_ceded_recovery_base_amount, 0))    paid_ceded_recovery_base_amount,
           sum(coalesce(paid_ceded_salvage_amount, 0))          paid_ceded_salvage_amount,
           sum(coalesce(paid_ceded_excess_amount, 0))           paid_ceded_excess_amount,
           sum(coalesce(paid_ceded_recovery_net, 0))            paid_ceded_recovery_net,
           count(distinct claim_no)                             number_claim
    from sunday.insured_port_recovery_paid
    --where claim_occur_dt > '2016-12-31'
    group by policy_no,
             channel,
             programme,
             main_class,
             subclass,
             agent_code,
             policy_package,
             is_gisweb_policy,
             date_part('year', claim_occur_dt),
             date_part('month', claim_occur_dt)
)
, claim as 
(
    select coalesce(rsv.policy_no, cp.policy_no, rec.policy_no)                         policy_no,
           coalesce(rsv.channel, cp.channel, rec.channel)                               channel,
           coalesce(rsv.programme, cp.programme, rec.programme)                         programme,
           coalesce(rsv.main_class, cp.main_class, rec.main_class)                      main_class,
           coalesce(rsv.subclass, cp.subclass, rec.subclass)                            subclass,
           coalesce(rsv.policy_package, cp.policy_package, rec.policy_package)          policy_package,
           coalesce(rsv.is_gisweb_policy, cp.is_gisweb_policy, rec.is_gisweb_policy)    is_gisweb_policy,
           coalesce(rsv.agent_code, cp.agent_code, rec.agent_code)                      agent_code,
           rsv.first_issue_year,
           rsv.first_issue_month,
           false::bool as                                                               is_endorsement,
           coalesce(rsv.claim_occur_year, cp.claim_occur_year, rec.claim_occur_year)    claim_occur_year,
           coalesce(rsv.claim_occur_month, cp.claim_occur_month, rec.claim_occur_month) claim_occur_month,
           coalesce(rsv.claim_base_amount, 0)                                           claim_base_amount,
           coalesce(rsv.claim_survey_amount, 0)                                         claim_survey_amount,
           coalesce(rsv.claim_net_loss, 0)                                              claim_net_loss,
           coalesce(rsv.claim_retained_base_amount, 0)                                  claim_retained_base_amount,
           coalesce(rsv.claim_retained_survey_amount, 0)                                claim_retained_survey_amount,
           coalesce(rsv.claim_retained_net_loss, 0)                                     claim_retained_net_loss,
           coalesce(rsv.claim_ceded_base_amount, 0)                                     claim_ceded_base_amount,
           coalesce(rsv.claim_ceded_survey_amount, 0)                                   claim_ceded_survey_amount,
           coalesce(rsv.claim_ceded_net_loss, 0)                                        claim_ceded_net_loss,
           coalesce(rsv.claim_recovery_base_amount, 0)                                  claim_recovery_base_amount,
           coalesce(rsv.claim_salvage_amount, 0)                                        claim_salvage_amount,
           coalesce(rsv.claim_excess_amount, 0)                                         claim_excess_amount,
           coalesce(rsv.claim_recovery_net, 0)                                          claim_recovery_net,
           coalesce(rsv.claim_retained_recovery_base_amount, 0)                         claim_retained_recovery_base_amount,
           coalesce(rsv.claim_retained_salvage_amount, 0)                               claim_retained_salvage_amount,
           coalesce(rsv.claim_retained_excess_amount, 0)                                claim_retained_excess_amount,
           coalesce(rsv.claim_retained_recovery_net, 0)                                 claim_retained_recovery_net,
           coalesce(rsv.claim_ceded_recovery_base_amount, 0)                            claim_ceded_recovery_base_amount,
           coalesce(rsv.claim_ceded_salvage_amount, 0)                                  claim_ceded_salvage_amount,
           coalesce(rsv.claim_ceded_excess_amount, 0)                                   claim_ceded_excess_amount,
           coalesce(rsv.claim_ceded_recovery_net, 0)                                    claim_ceded_recovery_net,
           coalesce(rsv.number_claim, 0)                                                number_claim_reserve,

           coalesce(cp.paid_claim_base_amount, 0)                                       paid_claim_base_amount,
           coalesce(cp.paid_claim_survey_amount, 0)                                     paid_claim_survey_amount,
           coalesce(cp.paid_claim_net_loss, 0)                                          paid_claim_net_loss,
           coalesce(cp.paid_claim_vat_amount, 0)                                        paid_claim_vat_amount,
           coalesce(cp.paid_claim_total, 0)                                             paid_claim_total,
           coalesce(cp.paid_claim_retained_base_amount, 0)                              paid_claim_retained_base_amount,
           coalesce(cp.paid_claim_retained_survey_amount, 0)                            paid_claim_retained_survey_amount,
           coalesce(cp.paid_claim_retained_net_loss, 0)                                 paid_claim_retained_net_loss,
           coalesce(cp.paid_claim_ceded_base_amount, 0)                                 paid_claim_ceded_base_amount,
           coalesce(cp.paid_claim_ceded_survey_amount, 0)                               paid_claim_ceded_survey_amount,
           coalesce(cp.paid_claim_ceded_net_loss, 0)                                    paid_claim_ceded_net_loss,
           coalesce(cp.number_claim, 0)                                                 number_claim_paid,

           coalesce(rec.paid_recovery_base_amount, 0)                                   paid_recovery_base_amount,
           coalesce(rec.paid_salvage_amount, 0)                                         paid_salvage_amount,
           coalesce(rec.paid_excess_amount, 0)                                          paid_excess_amount,
           coalesce(rec.paid_recovery_net, 0)                                           paid_recovery_net,
           coalesce(rec.paid_recovery_vat_amount, 0)                                    paid_recovery_vat_amount,
           coalesce(rec.paid_recovery_total, 0)                                         paid_recovery_total,
           coalesce(rec.paid_retained_recovery_base_amount, 0)                          paid_retained_recovery_base_amount,
           coalesce(rec.paid_retained_salvage_amount, 0)                                paid_retained_salvage_amount,
           coalesce(rec.paid_retained_excess_amount, 0)                                 paid_retained_excess_amount,
           coalesce(rec.paid_retained_recovery_net, 0)                                  paid_retained_recovery_net,
           coalesce(rec.paid_ceded_recovery_base_amount, 0)                             paid_ceded_recovery_base_amount,
           coalesce(rec.paid_ceded_salvage_amount, 0)                                   paid_ceded_salvage_amount,
           coalesce(rec.paid_ceded_excess_amount, 0)                                    paid_ceded_excess_amount,
           coalesce(rec.paid_ceded_recovery_net, 0)                                     paid_ceded_recovery_net,
           coalesce(rec.number_claim, 0)                                                number_recovery_paid
    from claim_reserveadj rsv
             full outer join claim_paid cp
                             on rsv.policy_no = cp.policy_no
                                 and rsv.channel = cp.channel
                                 and rsv.programme = cp.programme
                                 and rsv.main_class = cp.main_class
                                 and rsv.subclass = cp.subclass
                                 and rsv.agent_code = cp.agent_code
                                 and rsv.policy_package = cp.policy_package
                                 and rsv.is_gisweb_policy = cp.is_gisweb_policy
                                 and rsv.claim_occur_year = cp.claim_occur_year
                                 and rsv.claim_occur_month = cp.claim_occur_month
             full outer join recovery_paid rec
                             on coalesce(rsv.policy_no, cp.policy_no) = rec.policy_no
                                 and coalesce(rsv.channel, cp.channel) = rec.channel
                                 and coalesce(rsv.programme, cp.programme) = rec.programme
                                 and coalesce(rsv.main_class, cp.main_class) = rec.main_class
                                 and coalesce(rsv.subclass, cp.subclass) = rec.subclass
                                 and coalesce(rsv.agent_code, cp.agent_code) = rec.agent_code
                                 and coalesce(rsv.policy_package, cp.policy_package) = rec.policy_package
                                 and coalesce(rsv.is_gisweb_policy, cp.is_gisweb_policy) = rec.is_gisweb_policy
                                 and coalesce(rsv.claim_occur_year, cp.claim_occur_year) = rec.claim_occur_year
                                 and coalesce(rsv.claim_occur_month, cp.claim_occur_month) = rec.claim_occur_month
)
, loss_ratio_by_pol_and_acc_month as (
    select coalesce(prem.policy_no, claim.policy_no)                         policy_no,
           coalesce(cn.portfolio, 'KIT Legacy')                              portfolio,
           coalesce(prem.channel, claim.channel)                             channel,
           coalesce(prem.programme, claim.programme)                         programme,
           coalesce(cn.channel_code, '')                                     channel_code,
           coalesce(cn.channel_group, 'KIT Legacy')                          channel_group,
           scg.product_sort,
           scg.productgroup,
           scg.productgroup_main,
           coalesce(prem.main_class, claim.main_class)                       main_class,
           coalesce(prem.subclass, claim.subclass)                           subclass,
           coalesce(coalesce(prem.policy_package, claim.policy_package), '') policy_package,
           coalesce(prem.is_gisweb_policy, claim.is_gisweb_policy)           is_gisweb_policy,
           coalesce(prem.agent_code, claim.agent_code)                       agent_code,
           ag.agent_name,
           ag.agent_name_en,
           coalesce(prem.is_endorsement, claim.is_endorsement)               is_endorsement,
           coalesce(prem.first_issue_year, claim.first_issue_year)           first_issue_year,
           coalesce(prem.first_issue_month, claim.first_issue_month)         first_issue_month,
           coalesce(prem.earned_premium_year, claim.claim_occur_year)        accident_year,
           coalesce(prem.earned_premium_month, claim.claim_occur_month)      accident_month,
           coalesce(prem.earned_premium, 0)                                  earned_premium,
           coalesce(prem.retained_earned_premium, 0)                         retained_earned_premium,
           coalesce(prem.ceded_earned_premium, 0)                            ceded_earned_premium,

           coalesce(claim_base_amount, 0)                                    claim_base_amount,
           coalesce(claim_survey_amount, 0)                                  claim_survey_amount,
           coalesce(claim_net_loss, 0)                                       claim_net_loss,
           coalesce(claim_retained_base_amount, 0)                           claim_retained_base_amount,
           coalesce(claim_retained_survey_amount, 0)                         claim_retained_survey_amount,
           coalesce(claim_retained_net_loss, 0)                              claim_retained_net_loss,
           coalesce(claim_ceded_base_amount, 0)                              claim_ceded_base_amount,
           coalesce(claim_ceded_survey_amount, 0)                            claim_ceded_survey_amount,
           coalesce(claim_ceded_net_loss, 0)                                 claim_ceded_net_loss,
           coalesce(claim_recovery_base_amount, 0)                           claim_recovery_base_amount,
           coalesce(claim_salvage_amount, 0)                                 claim_salvage_amount,
           coalesce(claim_excess_amount, 0)                                  claim_excess_amount,
           coalesce(claim_recovery_net, 0)                                   claim_recovery_net,
           coalesce(claim_retained_recovery_base_amount, 0)                  claim_retained_recovery_base_amount,
           coalesce(claim_retained_salvage_amount, 0)                        claim_retained_salvage_amount,
           coalesce(claim_retained_excess_amount, 0)                         claim_retained_excess_amount,
           coalesce(claim_retained_recovery_net, 0)                          claim_retained_recovery_net,
           coalesce(claim_ceded_recovery_base_amount, 0)                     claim_ceded_recovery_base_amount,
           coalesce(claim_ceded_salvage_amount, 0)                           claim_ceded_salvage_amount,
           coalesce(claim_ceded_excess_amount, 0)                            claim_ceded_excess_amount,
           coalesce(number_claim_reserve, 0)                                 number_claim_reserve,

           coalesce(paid_claim_base_amount, 0)                               paid_claim_base_amount,
           coalesce(paid_claim_survey_amount, 0)                             paid_claim_survey_amount,
           coalesce(paid_claim_net_loss, 0)                                  paid_claim_net_loss,
           coalesce(paid_claim_vat_amount, 0)                                paid_claim_vat_amount,
           coalesce(paid_claim_total, 0)                                     paid_claim_total,
           coalesce(paid_claim_retained_base_amount, 0)                      paid_claim_retained_base_amount,
           coalesce(paid_claim_retained_survey_amount, 0)                    paid_claim_retained_survey_amount,
           coalesce(paid_claim_retained_net_loss, 0)                         paid_claim_retained_net_loss,
           coalesce(paid_claim_ceded_base_amount, 0)                         paid_claim_ceded_base_amount,
           coalesce(paid_claim_ceded_survey_amount, 0)                       paid_claim_ceded_survey_amount,
           coalesce(paid_claim_ceded_net_loss, 0)                            paid_claim_ceded_net_loss,
           coalesce(number_claim_paid, 0)                                    number_claim_paid,

           coalesce(paid_recovery_base_amount, 0)                            paid_recovery_base_amount,
           coalesce(paid_salvage_amount, 0)                                  paid_salvage_amount,
           coalesce(paid_excess_amount, 0)                                   paid_excess_amount,
           coalesce(paid_recovery_net, 0)                                    paid_recovery_net,
           coalesce(paid_recovery_vat_amount, 0)                             paid_recovery_vat_amount,
           coalesce(paid_recovery_total, 0)                                  paid_recovery_total,
           coalesce(paid_retained_recovery_base_amount, 0)                   paid_retained_recovery_base_amount,
           coalesce(paid_retained_salvage_amount, 0)                         paid_retained_salvage_amount,
           coalesce(paid_retained_excess_amount, 0)                          paid_retained_excess_amount,
           coalesce(paid_retained_recovery_net, 0)                           paid_retained_recovery_net,
           coalesce(paid_ceded_recovery_base_amount, 0)                      paid_ceded_recovery_base_amount,
           coalesce(paid_ceded_salvage_amount, 0)                            paid_ceded_salvage_amount,
           coalesce(paid_ceded_excess_amount, 0)                             paid_ceded_excess_amount,
           coalesce(paid_ceded_recovery_net, 0)                              paid_ceded_recovery_net,
           coalesce(number_recovery_paid, 0)                                 number_recovery_paid,

           days_of_month_range,
           total_covered_day,
           earned_unit_exposure,
           (select min(report_date) from prem_data_range)                    report_date
    from prem
             full outer join claim
                             on prem.policy_no = claim.policy_no
                                 and prem.channel = claim.channel
                                 and prem.programme = claim.programme
                                 and prem.main_class = claim.main_class
                                 and prem.subclass = claim.subclass
                                 and prem.policy_package = claim.policy_package
                                 and prem.is_gisweb_policy = claim.is_gisweb_policy
                                 and prem.agent_code = claim.agent_code
                                 and prem.first_issue_year = claim.first_issue_year
                                 and prem.first_issue_month = claim.first_issue_month
                                 and prem.earned_premium_year = claim.claim_occur_year
                                 and prem.earned_premium_month = claim.claim_occur_month
                                 and prem.is_endorsement = claim.is_endorsement
             left join sunday_mapping.sundaychannelcode cn on coalesce(prem.channel, claim.channel) = cn.channel
             left join sunday_mapping.sundaysubclassgroup scg
                       on coalesce(prem.main_class, claim.main_class) = scg.main_class and
                          coalesce(prem.subclass, claim.subclass) = scg.subclass
             left join sunday_mapping.sundayagentcode ag on coalesce(prem.agent_code, claim.agent_code) = ag.agentcode
)
select *
from loss_ratio_by_pol_and_acc_month
where accident_year >= 2017
  and make_date(accident_year::int, accident_month::int, 1) < (select min(report_date) from loss_ratio_by_pol_and_acc_month)
order by main_class, subclass, policy_no, accident_year, accident_month, is_endorsement;

------------------------------------------------------------------------------------------
---- 03. claim_by_policy_and_dayend
------------------------------------------------------------------------------------------
----REMARK----
--1.Claim by Policy and Dayend Year&Month

--create as
--Claim reserve dayend


------------------------------------------------------------------------------------------
---- 04. combined_loss_ratio_by_policy
------------------------------------------------------------------------------------------
----REMARK----
--1.Policy Level with Transaction Year&Month (UW/Accident/Dayend)

--create as
