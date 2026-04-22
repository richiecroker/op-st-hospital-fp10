SELECT month, sum(items) AS items, sum(actual_cost) AS actual_cost
FROM prescribing AS rx
WHERE EXISTS (
    SELECT 1 FROM unnest($1::VARCHAR[]) AS t(code)
    WHERE LEFT(rx.hospital, LENGTH(code)) = code
)
AND ($2::VARCHAR[] IS NULL OR bnf_name = ANY($2::VARCHAR[]))
GROUP BY month
ORDER BY month
