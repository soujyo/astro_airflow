--Calculates total visits,bounces and bounce rate per source of traffic
CREATE OR REPLACE VIEW {{ params.agg_db }}.{{ params.agg_schema }}.BOUNCE_RATE_PER_SOURCE
AS
SELECT
source,
total_visits,
total_no_of_bounces,
ROUND(( ( total_no_of_bounces / total_visits ) * 100 ),2) AS bounce_rate
FROM (
SELECT
trafficSource_source AS source,
COALESCE(COUNT ( TOTALS_VISITS ),0) AS total_visits,
COALESCE(SUM ( TOTALS_BOUNCES ),0) AS total_no_of_bounces
FROM
{{ params.stage_db }}.{{ params.stage_schema }}.{{ params.stage_table_compact }}
WHERE DATE BETWEEN DATE AND dateadd(day, -30, current_date())
GROUP BY source )
ORDER BY
total_visits DESC;

--Calculates total sessions,bounces and bounce rate,AVERAGE_SESSION_DURATION_SECS from each country
CREATE OR REPLACE VIEW {{ params.agg_db }}.{{ params.agg_schema }}.SESSION_BOUNCE_RATE_PER_COUNTRY
AS
select
geonetwork_country as country,
COUNT(distinct concat(FULLVISITORID,cast(VISITID as STRING))) as sessions,
sum(case when totals_bounces = 1 then totals_visits else 0 end) as total_bounces,
ROUND(sum(case when totals_bounces = 1 then totals_visits else 0 end)/COUNT(distinct concat(FULLVISITORID,cast(VISITID as STRING))),3) as bounce_rate,
ROUND(sum(totals_timeonsite) / COUNT(distinct concat(FULLVISITORID,cast(VISITID as STRING))),2) as average_session_duration_secs
from
{{ params.stage_db }}.{{ params.stage_schema }}.{{ params.stage_table_compact }}
WHERE DATE BETWEEN DATE AND dateadd(day, -30, current_date()) AND totals_visits = 1
group by
country
order by COUNTRY;

--Calculates traffic source metrics for transactions,revenue,bounce_rate and conversion rate
CREATE OR REPLACE VIEW {{ params.agg_db }}.{{ params.agg_schema }}.TRAFFIC_SOURCE_METRICS_CONVERSION
AS
SELECT
trafficSource_medium AS medium,
COUNT(distinct concat(FULLVISITORID,cast(VISITID as STRING))) AS sessions,
SUM(totals_transactions) AS transactions,
ROUND(SUM(totals_totalTransactionRevenue)/1000000,3) AS total_revenue,
ROUND(sum(totals_bounces)/COUNT(distinct concat(FULLVISITORID,cast(VISITID as STRING))),3) as bounce_rate,
ROUND(SUM(totals_transactions)/COUNT(distinct concat(FULLVISITORID,cast(VISITID as STRING))),3) AS conversion_rate
FROM
{{ params.stage_db }}.{{ params.stage_schema }}.{{ params.stage_table_compact }} x
WHERE
    DATE BETWEEN DATE AND dateadd(day, -30, current_date()) AND totals_visits = 1
GROUP BY medium
ORDER BY sessions DESC;


--Calculates total visits,pageviews,bounces,transactions and revenues per quarter

CREATE OR REPLACE VIEW {{ params.agg_db }}.{{ params.agg_schema }}.TOTAL_METRICES_PER_QUARTER
AS
SELECT
  year(date) as year,
  quarter(date) AS quarter,
  COUNT(DISTINCT fullVisitorId) AS Users,
  SUM(totals_visits) AS visits,
  SUM(totals_pageviews) AS pageviews,
  SUM(totals_bounces) AS bounces,
  SUM(totals_transactions) AS transactions,
  round(SUM(totals_transactionRevenue)/1000000,2) AS revenue,
  ROUND((SUM(totals_totalTransactionRevenue)/1000000)/(SUM(totals_visits)),2) AS revenue_per_visit
FROM
  {{ params.stage_db }}.{{ params.stage_schema }}.{{ params.stage_table_compact }}
WHERE DATE BETWEEN DATE AND dateadd(day, -30, current_date()) AND totals_visits = 1
GROUP BY
  year,quarter
ORDER BY
  year desc,quarter desc;


--Calculates user's conversion path through the site in the conversion process where users are retained or lost

CREATE OR REPLACE VIEW {{ params.agg_db }}.{{ params.agg_schema }}.CONVERSION_PATH_FOR_USER
AS
SELECT user_action,users,coalesce(ROUND((100-((LAG(CTE.users,1) OVER(order by users DESC))-users)/(LAG(CTE.users,1) OVER(order by users DESC))*100),2),100) AS CONVERSION_PERCENT_RATE_FROM_PREVIOUS_STAGE
FROM (SELECT
    CASE WHEN hits.value:eCommerceAction:action_type = '1' THEN 'Click through of product lists'
         WHEN hits.value:eCommerceAction:action_type = '2' THEN 'Product detail views'
         WHEN hits.value:eCommerceAction:action_type = '5' THEN 'Check out'
         WHEN hits.value:eCommerceAction:action_type = '6' THEN 'Completed purchase'
         WHEN hits.value:eCommerceAction:action_type = '7' THEN 'Refund of purchase'
    END AS user_action,
    COUNT(fullVisitorID) AS users
FROM
    {{ params.stage_db }}.{{ params.stage_schema }}.{{ params.stage_table_compact }} x ,
     lateral flatten(input => x.hits) as hits,
     lateral flatten( input => hits.value:product) as hits_product

WHERE
    DATE BETWEEN DATE AND dateadd(day, -30, current_date())
    AND
    hits.value:eCommerceAction:action_type Not in ('0','3','4')

GROUP BY user_action
ORDER BY users DESC) cte;

--Calculates user's promotion click through rate as per promotion dimensions

CREATE OR REPLACE VIEW {{ params.agg_db }}.{{ params.agg_schema }}.PROMOTION_CLICK_THROUGH_RATE
AS
SELECT
  hits_promotion.value:promoName AS Promotion_Name,
  hits_promotion.value:promoCreative AS Promotion_Type,
  hits_promotion.value:promoPosition as Promotion_Position,
  COUNT(hits.value:promotionActionInfo:promoIsView) AS Promotion_Views,
  COUNT(hits.value:promotionActionInfo:promoIsClick) AS Promotion_Clicks,
  ROUND((COUNT(hits.value:promotionActionInfo:promoIsClick)
        /COUNT(hits.value:promotionActionInfo:promoIsView))*100,2) AS Promotion_Click_Through_Rate
FROM
  {{ params.stage_db }}.{{ params.stage_schema }}.{{ params.stage_table_compact }} x,
  lateral flatten( input => x.hits) as hits,
  lateral flatten( input => hits.value:promotion) as hits_promotion
WHERE
  DATE BETWEEN DATE AND dateadd(day, -30, current_date())
GROUP BY
  Promotion_Name,  Promotion_Type, Promotion_Position
HAVING
  Promotion_Views > Promotion_Clicks
ORDER BY
  Promotion_Click_Through_Rate DESC;


--Calculates unique page views per page
CREATE OR REPLACE VIEW {{ params.agg_db }}.{{ params.agg_schema }}.UNIQUE_PAGE_VIEWS
AS
select
  page,
  sum(unique_pageviews) as unique_pageviews
from (
  select
    hits.value:page:pagePath as page,
    concat(hits.value:page:pagePath,hits.value:page:pagetitle) as page_concat,
    count(distinct concat(FULLVISITORID , cast(VISITID as string))) as unique_pageviews
  from
    {{ params.stage_db }}.{{ params.stage_schema }}.{{ params.stage_table_compact }} x,
    lateral flatten( input => x.hits) as hits
  where
    DATE BETWEEN DATE AND dateadd(day, -30, current_date())
  group by
    page,
    page_concat)
group by
  page
order by
  unique_pageviews desc;
