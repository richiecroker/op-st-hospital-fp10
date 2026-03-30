SELECT
  PARSE_DATE('%Y%m', CAST(period AS STRING)) AS month,
  BNF_NAME AS bnf_name,
  rx.BNF_CODE AS bnf_code,
  COALESCE(vmp.controlinfo_cat, "No Controlled Drug Status") AS cd_category,
  COALESCE(bnf.chapter, "Unknown chapter") AS bnf_chapter,
  HOSPITAL_TRUST_CODE AS hospital,
  SUM(TOTAL_QUANTITY) AS quantity,
  SUM(TOTAL_ITEMS) AS items,
  SUM(TOTAL_ACTUAL_COST) AS actual_cost

FROM ebmdatalab.hospitalcommunityprescribing.hospital_community_rx AS rx

LEFT JOIN dmd.vmp_full AS vmp
  ON CONCAT(
       SUBSTR(rx.BNF_CODE, 0, 9),
       "AA",
       SUBSTR(rx.BNF_CODE, -2),
       SUBSTR(rx.BNF_CODE, -2)
     ) = vmp.bnf_code

LEFT JOIN hscic.bnf AS bnf
  ON rx.BNF_CODE = bnf.presentation_code

WHERE PARSE_DATE('%Y%m', CAST(period AS STRING)) >= '2019-01-01'

GROUP BY
  month,
  bnf_name,
  bnf_code,
  hospital,
  vmp.controlinfo_cat,
  bnf_chapter
