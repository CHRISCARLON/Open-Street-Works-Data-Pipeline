{% set table_alias = 'uprn_usrn_counts_latest' %}
{{ config(materialized='table', alias=table_alias) }}

SELECT
    usrn.usrn,
    COUNT(uprn.CORRELATION_ID) as uprn_count
FROM
    os_open_usrns.open_usrns_latest usrn
    JOIN os_open_linked_identifiers.os_open_linked_identifiers_latest uprn ON usrn.usrn = uprn.IDENTIFIER_2_USRN
GROUP BY
    usrn.usrn
