SELECT
    start_time,
    filename,
    location,
    lead_id
FROM
    recording_log
WHERE
     (start_time between CURRENT_TIMESTAMP() - INTERVAL 1 DAY and CURRENT_TIMESTAMP()) 