CREATE OR REPLACE TABLE
  keepcoding.ivr_summary AS (
  SELECT
    calls_ivr_id AS ivr_id,
    calls_phone_number AS phone_number,
    calls_ivr_result AS ivr_result,
    CASE
      WHEN calls_vdn_label LIKE "ATC%" THEN "FRONT"
      WHEN calls_vdn_label LIKE "TECH%" THEN "TECH"
      WHEN calls_vdn_label = "ABSORPTION" THEN "ABSORPTION"
    ELSE
    "RESTO"
  END
    AS vdn_aggregation,
    calls_start_date AS start_date,
    calls_end_date AS end_date,
    calls_total_duration AS total_duration,
    calls_customer_segment AS customer_segment,
    calls_ivr_language AS ivr_language,
    document_type,
    document_identification,
    customer_phone,
    billing_account_id,
    calls_steps_module AS steps_module,
    calls_module_aggregation AS module_aggregation,
  IF
    (module_name = 'AVERIA_MASIVA',1,0) AS masiva_lg,
  IF
    (step_name = 'CUSTOMERINFOBYDNI.TX'
      AND step_description_error = NULLIF(step_description_error, "NULL"),1,0) AS info_by_dni_lg,
    CASE
      WHEN TIMESTAMP_DIFF(calls_start_date, LAG(calls_start_date) OVER (PARTITION BY calls_phone_number ORDER BY calls_start_date), HOUR) <= 24 THEN 1
    ELSE
    0
  END
    AS repeated_phone_24H,
    CASE
      WHEN COUNT(*) OVER (PARTITION BY calls_phone_number ORDER BY calls_end_date ASC ROWS BETWEEN 1 FOLLOWING AND UNBOUNDED FOLLOWING) > 0 THEN 1
    ELSE
    0
  END
    AS cause_recall_phone_24H
  FROM
    keepcoding.ivr_detail
  WHERE
    document_type <> 'NULL'
    AND document_type <> 'DESCONOCIDO'
    AND document_identification <> 'DESCONOCIDO'
    AND document_identification <> 'NULL'
    AND customer_phone <> 'DESCONOCIDO'
    AND customer_phone <> 'NULL'
    AND billing_account_id <> 'DESCONOCIDO'
    AND billing_account_id <> 'NULL' QUALIFY ROW_NUMBER() OVER(PARTITION BY SAFE_CAST(calls_ivr_id AS string)) = 1 );