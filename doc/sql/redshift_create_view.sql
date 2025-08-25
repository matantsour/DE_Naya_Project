--CREATE OR REPLACE VIEW campaign_daily_performance AS
WITH clicks_agg AS (
    SELECT
        c.campaign_id,
        c.company_name,
        c.state,
        c.cpa_target,
        j.job_id,
        j.title,
        j.city,
        cl.date,
        SUM(cl.cost) AS spend,
        COUNT(*) AS clicks,
        SUM(cl.is_applicant) AS applicants
    FROM "dev"."publishing_schema"."campaigns" c
    JOIN "dev"."publishing_schema"."jobs" j
      ON c.company_name = j.company_name
    JOIN "dev"."publishing_schema"."clicks" cl
      ON j.job_id = cl.job_id
    GROUP BY c.campaign_id,c.company_name,c.cpa_target, c.state, j.job_id, j.title, j.city, cl.date
),
campaign_stats AS (
    SELECT
        ca.campaign_id,
        ca.company_name,
        ca.state,
        ca.cpa_target,
        ca.budget AS campaign_budget,
        ca.margin AS campaign_margin,
        cl.date,
        COUNT(DISTINCT cl.job_id) AS job_count,
        SUM(cl.spend) AS total_spend
    FROM clicks_agg cl
    JOIN "dev"."publishing_schema"."campaigns" ca
      ON cl.campaign_id = ca.campaign_id
    GROUP BY ca.campaign_id, ca.company_name, ca.state, ca.cpa_target, ca.budget, ca.margin, cl.date
),
-- cumulative spend until yesterday per campaign/job
cumulative_spend AS (
    SELECT
        cl.campaign_id,
        cl.job_id,
        cl.date,
        SUM(cl.spend) OVER (
            PARTITION BY cl.campaign_id, cl.job_id
            ORDER BY cl.date
            ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
        ) AS spend_until_yesterday
    FROM clicks_agg cl
),
final AS (
    SELECT
        cl.campaign_id,
        cl.company_name,
        cl.state,
        cl.cpa_target,
        cs.campaign_budget,
        cs.campaign_margin,
        cl.job_id,
        cl.title,
        cl.city,
        cl.date,
        (cs.campaign_budget::double precision / NULLIF(cs.job_count,0)) AS budget_per_job,
        cl.spend,
        cs.total_spend,
        cum.spend_until_yesterday,
        -- remainder days in month
        DATEDIFF(day, cl.date, ADD_MONTHS(DATE_TRUNC('month', cl.date), 1)) AS remaining_days_in_month,
        -- job_daily_budget calculation
        CASE 
            WHEN DATEDIFF(day, cl.date, ADD_MONTHS(DATE_TRUNC('month', cl.date), 1)) > 0
            THEN ( (cs.campaign_budget::double precision / NULLIF(cs.job_count,0)) - COALESCE(cum.spend_until_yesterday,0) )
                / DATEDIFF(day, cl.date, ADD_MONTHS(DATE_TRUNC('month', cl.date), 1))
        END AS job_daily_budget,

        (cs.total_spend::double precision / NULLIF(cs.campaign_budget,0)) AS campaign_budget_util_rate,
        cl.clicks,
        CASE WHEN cl.clicks > 0 THEN cl.spend/cl.clicks ELSE NULL END AS cpc,
        cl.applicants,
        CASE WHEN cl.clicks > 0 THEN cl.applicants::double precision/cl.clicks ELSE NULL END AS conversion_rate,
        CASE WHEN cl.applicants > 0 THEN cl.spend/cl.applicants ELSE NULL END AS cost_per_applicant,
        CASE WHEN cl.applicants > 0 THEN (cl.spend/cl.applicants)/NULLIF(cl.cpa_target,0) ELSE NULL END AS cpa_target_rate,
        
        -- daily budget utilization
        CASE 
            WHEN (
                ( (cs.campaign_budget::double precision / NULLIF(cs.job_count,0)) - COALESCE(cum.spend_until_yesterday,0) )
                / NULLIF(DATEDIFF(day, cl.date, ADD_MONTHS(DATE_TRUNC('month', cl.date), 1)),0)
            ) > 0
            THEN cl.spend / (
                ( (cs.campaign_budget::double precision / NULLIF(cs.job_count,0)) - COALESCE(cum.spend_until_yesterday,0) )
                / NULLIF(DATEDIFF(day, cl.date, ADD_MONTHS(DATE_TRUNC('month', cl.date), 1)),0)
            )
        END AS daily_budget_util_rate
    FROM clicks_agg cl
    JOIN campaign_stats cs 
      ON cl.campaign_id = cs.campaign_id AND cl.date = cs.date
    LEFT JOIN cumulative_spend cum
      ON cl.campaign_id = cum.campaign_id AND cl.job_id = cum.job_id AND cl.date = cum.date
)
SELECT * FROM final
--WITH NO SCHEMA BINDING;
