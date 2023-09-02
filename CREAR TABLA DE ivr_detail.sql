CREATE OR REPLACE TABLE
  keepcoding.ivr_detail AS
WITH
  ivr_calls AS (
  SELECT
    ivr_id calls_ivr_id,
    phone_number calls_phone_number,
    ivr_result calls_ivr_result,
    vdn_label calls_vdn_label,
    start_date calls_start_date,
    end_date calls_end_date,
    total_duration calls_total_duration,
    customer_segment calls_customer_segment,
    ivr_language calls_ivr_language,
    steps_module calls_steps_module,
    module_aggregation calls_module_aggregation,
  FROM
    keepcoding.ivr_calls),
  ivr_modules AS (
  SELECT
    ivr_id,
    module_sequece,
    module_name,
    module_duration,
    module_result
  FROM
    keepcoding.ivr_modules ),
  ivr_steps AS (
  SELECT
    ivr_id,
    module_sequece,
    step_sequence,
    step_name,
    step_result,
    step_description_error,
    document_type,
    document_identification,
    customer_phone,
    billing_account_id
  FROM
    keepcoding.ivr_steps )
SELECT
  ivr_calls.calls_ivr_id,
  ivr_calls.calls_phone_number,
  ivr_calls.calls_ivr_result,
  ivr_calls.calls_vdn_label,
  ivr_calls.calls_start_date,
  FORMAT_DATE('%Y%m%d', ivr_calls.calls_start_date) AS calls_start_date_id,
  ivr_calls.calls_end_date,
  FORMAT_DATE('%Y%m%d', ivr_calls.calls_end_date) AS calls_end_date_id,
  ivr_calls.calls_total_duration,
  ivr_calls.calls_customer_segment,
  ivr_calls.calls_ivr_language,
  ivr_calls.calls_steps_module,
  ivr_calls.calls_module_aggregation,
  ivr_modules.module_sequece,
  ivr_modules.module_name,
  ivr_modules.module_duration,
  ivr_modules.module_result,
  ivr_steps.step_sequence,
  ivr_steps.step_result,
  ivr_steps.step_description_error,
  ivr_steps.step_name,
  ivr_steps.document_type,
  ivr_steps.document_identification,
  ivr_steps.customer_phone,
  ivr_steps.billing_account_id
FROM
  ivr_calls
LEFT JOIN
  ivr_modules
ON
  ivr_calls.calls_ivr_id = ivr_modules.ivr_id
LEFT JOIN
  ivr_steps
ON
  ivr_modules.ivr_id = ivr_steps.ivr_id
  AND ivr_modules.module_sequece = ivr_steps.module_sequece;