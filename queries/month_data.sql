SELECT month, sum(items) AS items, sum(actual_cost) AS actual_cost
FROM prescribing AS rx
WHERE EXISTS (
    SELECT 1 FROM unnest($1::VARCHAR[]) AS t(code)
    WHERE LEFT(rx.hospital, LENGTH(code)) = code
)
GROUP BY month
ORDER BY month