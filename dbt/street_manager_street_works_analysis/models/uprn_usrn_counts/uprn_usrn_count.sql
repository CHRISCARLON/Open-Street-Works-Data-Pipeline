{% set table_alias = 'uprn_usrn_counts_latest' %}
{{ config(materialized='table', alias=table_alias) }}

SELECT
    s.usrn,
    COUNT(l.CORRELATION_ID) as uprn_count
FROM
    os_open_usrns.open_usrns_latest s
    JOIN os_open_linked_identifiers.os_open_linked_identifiers_latest l ON s.usrn = l.IDENTIFIER_2_USRN
GROUP BY
    s.usrn
