{{ config(
    materialized='incremental',
    unique_key='external_id',
    incremental_strategy='delete+insert',
) }}

SELECT 
    DISTINCT "{{ var("table_prefix") }}_users".id as external_id,
    NOW() as created,
    NOW() as modified,
    md5(
      '{{ var("integration_id") }}' ||
      "{{ var("table_prefix") }}_users".id ||
      'user' ||
      'lucca'
    )  as id,
    'lucca' as source,
    '{{ var("integration_id") }}'::uuid  as integration_id,
    _airbyte_raw_{{ var("table_prefix") }}_users._airbyte_data as last_raw_data, 
    '{{ var("timestamp") }}' as sync_timestamp,
    "{{ var("table_prefix") }}_users".mail as email,
    "{{ var("table_prefix") }}_users".firstname as firstname,
    "{{ var("table_prefix") }}_users".birthdate as birth_date,
    "{{ var("table_prefix") }}_users".lastname as lastname,
    NULL as phone_number,
    "{{ var("table_prefix") }}_users_culture".name as language,
    "{{ var("table_prefix") }}_users_legalentity".name as legal_entity_name,
    "{{ var("table_prefix") }}_users_manager".name as manager_email,
    "{{ var("table_prefix") }}_users".jobtitle as job_title,
    NULL as team,
    NULL as country,
    "{{ var("table_prefix") }}_users".dtcontractstart as contract_start_date,
    "{{ var("table_prefix") }}_users".dtcontractend as contract_end_date,
    "{{ var("table_prefix") }}_users".gender as gender,
    "{{ var("table_prefix") }}_users_department".name as business_unit,
    NULL as employee_number,
    "{{ var("table_prefix") }}_work_contracts_type".name as contract_type,
    "{{ var("table_prefix") }}_work_contracts_spc".name as socio_professional_category,
    NULL as state
FROM "{{ var("table_prefix") }}_users"
LEFT JOIN _airbyte_raw_{{ var("table_prefix") }}_users
    ON "{{ var("table_prefix") }}_users"._airbyte_ab_id = "_airbyte_raw_{{ var("table_prefix") }}_users"._airbyte_ab_id
LEFT JOIN "{{ var("table_prefix") }}_users_culture"
    ON "{{ var("table_prefix") }}_users_culture"."_airbyte_{{ var("table_prefix") }}_users_hashid" = "{{ var("table_prefix") }}_users"."_airbyte_{{ var("table_prefix") }}_users_hashid"
LEFT JOIN "{{ var("table_prefix") }}_users_manager"
    ON  "{{ var("table_prefix") }}_users_manager"."_airbyte_{{ var("table_prefix") }}_users_hashid" = "{{ var("table_prefix") }}_users"."_airbyte_{{ var("table_prefix") }}_users_hashid"
LEFT JOIN "{{ var("table_prefix") }}_users_legalentity"
    ON  "{{ var("table_prefix") }}_users_legalentity"."_airbyte_{{ var("table_prefix") }}_users_hashid" = "{{ var("table_prefix") }}_users"."_airbyte_{{ var("table_prefix") }}_users_hashid"
LEFT JOIN "{{ var("table_prefix") }}_users_department"
    ON "{{ var("table_prefix") }}_users_department"."_airbyte_{{ var("table_prefix") }}_users_hashid" = "{{ var("table_prefix") }}_users"."_airbyte_{{ var("table_prefix") }}_users_hashid"
LEFT JOIN "{{ var("table_prefix") }}_work_contracts"
    ON "{{ var("table_prefix") }}_work_contracts"."ownerid" = "{{ var("table_prefix") }}_users"."id"::int
LEFT JOIN "{{ var("table_prefix") }}_work_contracts_type"
    ON "{{ var("table_prefix") }}_work_contracts_type"."_airbyte_{{ var("table_prefix") }}_work_contracts_hashid" = "{{ var("table_prefix") }}_work_contracts"."_airbyte_{{ var("table_prefix") }}_work_contracts_hashid"
LEFT JOIN "{{ var("table_prefix") }}_work_contracts_spc"
    ON "{{ var("table_prefix") }}_work_contracts_spc"."_airbyte_{{ var("table_prefix") }}_work_contracts_hashid" = "{{ var("table_prefix") }}_work_contracts"."_airbyte_{{ var("table_prefix") }}_work_contracts_hashid"
WHERE "{{ var("table_prefix") }}_work_contracts"."isapplicable" = true
