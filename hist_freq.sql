-- Most common tdiff by history_id
DROP FUNCTION arprecip.hist_freq(history_id int);

CREATE OR REPLACE FUNCTION arprecip.hist_freq(history_id int)
RETURNS interval AS
$$
SELECT tdiff
FROM
(
    SELECT history_id, count(tdiff) as cnt, tdiff 
    FROM 
    (
        SELECT history_id,
            obs_raw_id,
            obs_time,
            obs_time - lag(obs_time) over (order by obs_time) as tdiff
        FROM obs_raw
        WHERE history_id = $1 AND
            vars_id in (SELECT vars_id FROM arprecip.precip_vars_v)
        ORDER BY obs_time
    ) as foo
    GROUP BY history_id, tdiff
    ORDER BY cnt DESC
    LIMIT 1
) as bar
$$
LANGUAGE SQL;
