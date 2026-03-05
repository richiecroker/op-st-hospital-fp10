SELECT
  PARSE_DATE('%Y%m', CAST(period AS STRING)) AS month,
  BNF_NAME AS bnf_name,
  BNF_CODE AS bnf_code,
  HOSPITAL_TRUST_CODE AS hospital,
  sum(TOTAL_QUANTITY) AS quantity,
  sum(TOTAL_ITEMS) AS items,
  sum(TOTAL_ACTUAL_COST) AS actual_cost
FROM ebmdatalab.hospitalcommunityprescribing.hospital_community_rx
GROUP BY
  month,
  bnf_name,
  bnf_code,
  hospital