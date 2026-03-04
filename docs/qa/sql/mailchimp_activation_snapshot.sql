-- Optional QA snapshot for mailchimp activation implementation.
-- Run only after activation migration/mode exists.

SELECT status, COUNT(*) AS n
FROM mailchimp_activation_run
GROUP BY 1
ORDER BY 1;

SELECT
    COUNT(*) AS rows_total,
    COUNT(DISTINCT email_normalized) AS distinct_emails,
    COUNT(*) FILTER (WHERE is_suppressed) AS suppressed_rows
FROM mailchimp_activation_row;
