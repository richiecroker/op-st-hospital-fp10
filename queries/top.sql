SELECT bnf_name, rx.hospital, cd_category, sum(actual_cost) AS actual_cost, sum(items) AS items
FROM prescribing AS rx
WHERE EXISTS (
    SELECT 1 FROM unnest($1::VARCHAR[]) AS t(code)
    WHERE LEFT(rx.hospital, LENGTH(code)) = code
)
AND CAST(month AS DATE) BETWEEN $2 AND $3
GROUP BY bnf_name, rx.hospital,cd_category,
