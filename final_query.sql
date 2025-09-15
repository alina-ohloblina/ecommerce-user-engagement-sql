WITH
  -- 1) Accounts by ts/country/...
  account_base AS (
    SELECT
      TIMESTAMP(s.date)                                AS ts,
      sp.country,
      a.send_interval,
      a.is_verified,
      a.is_unsubscribed,
      COUNT(DISTINCT a.id)                             AS account_cnt
    FROM `data-analytics-mate.DA.account`         AS a
    JOIN `data-analytics-mate.DA.account_session` AS ac
      ON a.id = ac.account_id
    JOIN `data-analytics-mate.DA.session`         AS s
      ON ac.ga_session_id = s.ga_session_id
    JOIN `data-analytics-mate.DA.session_params`  AS sp
      ON s.ga_session_id = sp.ga_session_id
    GROUP BY ts, sp.country, a.send_interval, a.is_verified, a.is_unsubscribed
  ),


  -- 2) Country totals + rank on accounts
  account_country_totals AS (
    SELECT
      country,
      SUM(account_cnt)                                    AS total_country_account_cnt,
      RANK() OVER (ORDER BY SUM(account_cnt) DESC)        AS rank_total_country_account_cnt
    FROM account_base
    GROUP BY country
  ),


  -- 3) Emails by ts/country/...
  email_base AS (
    SELECT
      TIMESTAMP_ADD(
        TIMESTAMP(s.date),
        INTERVAL e.sent_date DAY
      )                                                  AS ts,
      sp.country,
      a.send_interval,
      a.is_verified,
      a.is_unsubscribed,
      COUNT(DISTINCT e.id_message)                      AS sent_msg,
      COUNT(DISTINCT eo.id_message)                     AS open_msg,
      COUNT(DISTINCT ev.id_message)                     AS visit_msg
    FROM `data-analytics-mate.DA.email_sent`         AS e
    JOIN `data-analytics-mate.DA.account`            AS a
      ON e.id_account = a.id
    JOIN `data-analytics-mate.DA.account_session`    AS ac
      ON a.id = ac.account_id
    JOIN `data-analytics-mate.DA.session`            AS s
      ON ac.ga_session_id = s.ga_session_id
    JOIN `data-analytics-mate.DA.session_params`     AS sp
      ON s.ga_session_id = sp.ga_session_id
    LEFT JOIN `data-analytics-mate.DA.email_open`    AS eo
      ON e.id_message = eo.id_message
    LEFT JOIN `data-analytics-mate.DA.email_visit`   AS ev
      ON e.id_message = ev.id_message
    GROUP BY ts, sp.country, a.send_interval, a.is_verified, a.is_unsubscribed
  ),


  -- 4) Country totals + rank on emails
  email_country_totals AS (
    SELECT
      country,
      SUM(sent_msg)                                       AS total_country_sent_cnt,
      RANK() OVER (ORDER BY SUM(sent_msg) DESC)           AS rank_total_country_sent_cnt
    FROM email_base
    GROUP BY country
  ),


  -- 5) Union aligned columns
  combined AS (
    SELECT
      ab.ts,
      ab.country,
      ab.send_interval,
      ab.is_verified,
      ab.is_unsubscribed,
      ab.account_cnt,
      NULL               AS sent_msg,
      NULL               AS open_msg,
      NULL               AS visit_msg,
      act.total_country_account_cnt,
      NULL               AS total_country_sent_cnt,
      act.rank_total_country_account_cnt,
      NULL               AS rank_total_country_sent_cnt
    FROM account_base AS ab
    JOIN account_country_totals AS act
      ON ab.country = act.country


    UNION ALL


    SELECT
      eb.ts,
      eb.country,
      eb.send_interval,
      eb.is_verified,
      eb.is_unsubscribed,
      NULL               AS account_cnt,
      eb.sent_msg,
      eb.open_msg,
      eb.visit_msg,
      NULL               AS total_country_account_cnt,
      ect.total_country_sent_cnt,
      NULL               AS rank_total_country_account_cnt,
      ect.rank_total_country_sent_cnt
    FROM email_base AS eb
    JOIN email_country_totals AS ect
      ON eb.country = ect.country
  )


-- 6) Final filter: keep only top-10 by either country-rank
SELECT
  ts,
  country,
  send_interval,
  is_verified,
  is_unsubscribed,
  account_cnt,
  sent_msg,
  open_msg,
  visit_msg,
  total_country_account_cnt,
  total_country_sent_cnt,
  rank_total_country_account_cnt,
  rank_total_country_sent_cnt
FROM combined
WHERE
  rank_total_country_account_cnt <= 10
  OR rank_total_country_sent_cnt    <= 10
ORDER BY ts, country;
