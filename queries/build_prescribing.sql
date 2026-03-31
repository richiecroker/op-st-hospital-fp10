WITH bnf_chapter AS
(SELECT DISTINCT chapter, chapter_code FROM hscic.bnf)

SELECT
  PARSE_DATE('%Y%m', CAST(period AS STRING)) AS month,
  COALESCE(bnf.presentation, rx.bnf_name) AS bnf_name, --- use name from BNF map for consistency, then use rx data name if that doesn't exist
  TRIM(COALESCE(map.current_bnf_code, rx.bnf_code)) AS bnf_code, --- map to current BNF code if it exists in the BNF normalisation map
  COALESCE(vmp.controlinfo_cat, "No Controlled Drug Status") AS cd_category, --- gives CD status, with "no status" if it doesn't exist
  COALESCE(bnf_chapter.chapter, "Unknown chapter") AS bnf_chapter, --- gives BNF chapter, with "unknown" if there's issue with drug (e.g. discontinued), mapped to the old code if the BNF map hasn't been updated
  HOSPITAL_TRUST_CODE AS hospital,
  SUM(TOTAL_QUANTITY) AS quantity,
  SUM(TOTAL_ITEMS) AS items,
  SUM(TOTAL_ACTUAL_COST) AS actual_cost

FROM ebmdatalab.hospitalcommunityprescribing.hospital_community_rx AS rx

LEFT JOIN ebmdatalab.hscic.bnf_map AS map
  ON map.former_bnf_code = rx.bnf_code

LEFT JOIN dmd.vmp_full AS vmp
ON CONCAT(
      SUBSTR(rx.BNF_CODE, 0, 9),
      "AA",
      SUBSTR(rx.BNF_CODE, -2),
      SUBSTR(rx.BNF_CODE, -2)
    ) = vmp.bnf_code

INNER JOIN bnf_chapter
  ON bnf_chapter.chapter_code = left(rx.bnf_code,2)

LEFT JOIN hscic.bnf AS bnf
  ON rx.bnf_code = bnf.presentation_code

WHERE PARSE_DATE('%Y%m', CAST(period AS STRING)) = '2019-01-01'

GROUP BY
  month,
  bnf_name,
  bnf_code,
  hospital,
  vmp.controlinfo_cat,
  bnf_chapter
