CREATE OR REPLACE TABLE
  keepcoding.ivr_summary AS
WITH
  doc AS(
  SELECT
    calls_ivr_id AS doc_ivr_id,
    document_identification AS doc_document_identification,
    document_type AS doc_document_type,
    module_sequece AS doc_module_sequence,
    step_sequence AS doc_step_sequence
  FROM
    keepcoding.ivr_detail
  WHERE
    document_identification NOT IN ('NULL',
      'DESCONOCIDO') QUALIFY ROW_NUMBER() OVER(PARTITION BY SAFE_CAST(doc_ivr_id AS string)
    ORDER BY
      module_sequece DESC,
      step_sequence DESC) = 1)
SELECT
  ivr_detail.calls_ivr_id AS ivr_id,
  ivr_detail.calls_phone_number AS phone_number,
  ivr_detail.calls_ivr_result AS ivr_result,
  CASE
    WHEN ivr_detail.calls_vdn_label LIKE "ATC%" THEN "FRONT"
    WHEN ivr_detail.calls_vdn_label LIKE "TECH%" THEN "TECH"
    WHEN ivr_detail.calls_vdn_label = "ABSORPTION" THEN "ABSORPTION"
  ELSE
  "RESTO"
END
  AS vdn_aggregation,
  ivr_detail.calls_start_date AS start_date,
  ivr_detail.calls_end_date AS end_date,
  ivr_detail.calls_total_duration AS total_duration,
  ivr_detail.calls_customer_segment AS customer_segment,
  ivr_detail.calls_ivr_language AS ivr_language,
  ivr_detail.document_type,
  ivr_detail.document_identification,
  IFNULL(MAX(NULLIF(ivr_detail.customer_phone, 'NULL')), 'DESCONOCIDO') AS customer_phone,
  IFNULL(MAX(NULLIF(ivr_detail.billing_account_id, 'NULL')), 'DESCONOCIDO') AS billing_account_id,
  ivr_detail.calls_steps_module AS steps_module,
  ivr_detail.calls_module_aggregation AS module_aggregation,
  MAX(
  IF
    (ivr_detail.module_name = 'AVERIA_MASIVA',1,0)) AS masiva_lg,
  MAX(
  IF
    (ivr_detail.step_name = 'CUSTOMERINFOBYPHONE.TX'
      AND ivr_detail.step_description_error = "NULL",1,0)) AS info_by_phone_lg,
  MAX(
  IF
    (ivr_detail.step_name = 'CUSTOMERINFOBYDNI.TX'
      AND ivr_detail.step_description_error = "NULL",1,0)) AS info_by_dni_lg,
  CASE
    WHEN TIMESTAMP_DIFF(ivr_detail.calls_start_date, LAG(ivr_detail.calls_start_date) OVER (PARTITION BY ivr_detail.calls_phone_number ORDER BY ivr_detail.calls_start_date), HOUR) <= 24 THEN 1
  ELSE
  0
END
  AS repeated_phone_24H,
  CASE
    WHEN TIMESTAMP_DIFF(ivr_detail.calls_start_date, LEAD(ivr_detail.calls_start_date) OVER (PARTITION BY ivr_detail.calls_phone_number ORDER BY ivr_detail.calls_start_date), HOUR) <= 24 THEN 1
  ELSE
  0
END
  AS cause_recall_phone_24H
FROM
  keepcoding.ivr_detail
LEFT JOIN
  doc
ON
  ivr_detail.calls_ivr_id = doc.doc_ivr_id
LEFT JOIN
  keepcoding.ivr_detail recalls
ON
  ivr_detail.calls_phone_number <> 'NULL'
  AND ivr_detail.calls_phone_number = recalls.calls_phone_number
  AND ivr_detail.calls_ivr_id <> recalls.calls_ivr_id
GROUP BY
  ivr_id,
  phone_number,
  ivr_result,
  vdn_aggregation,
  start_date,
  end_date,
  total_duration,
  customer_segment,
  ivr_language,
  steps_module,
  module_aggregation,
  document_type,
  document_identification;