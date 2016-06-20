-- Threshold functions

CREATE OR REPLACE FUNCTION arprecip.daily_thresh(history_id int, event_years int)
RETURNS double precision AS
$$
SELECT min(datum)
FROM
(
    SELECT datum
    FROM arprecip.daily_precip_mv
    WHERE history_id = $1
    ORDER BY datum DESC
    LIMIT (SELECT floor(stn_years/$2) FROM arprecip.station_stats_v WHERE history_id = $1)
) as foo
$$
LANGUAGE SQL;

CREATE OR REPLACE FUNCTION arprecip.n_day_sum(history_id int, obs_day timestamp, range interval)
RETURNS double precision AS
$$
SELECT sum(datum)
FROM arprecip.daily_precip_mv
WHERE history_id = $1
AND obs_day BETWEEN $2 AND $2 + $3
$$
LANGUAGE SQL;


CREATE OR REPLACE FUNCTION arprecip.three_daily_thresh(history_id int, event_years int)
RETURNS double precision AS
$$
SELECT min(datum)
FROM
(
    SELECT history_id, obs_day, arprecip.n_day_sum(history_id, obs_day, '2 days'::interval) as datum
    FROM arprecip.daily_precip_mv
    WHERE history_id = $1
    ORDER BY datum DESC
    LIMIT (SELECT floor(stn_years/$2) FROM arprecip.station_stats_v WHERE history_id = $1)
) as foo
$$
LANGUAGE SQL;


CREATE OR REPLACE FUNCTION arprecip.five_daily_thresh(history_id int, event_years int)
RETURNS double precision AS
$$
SELECT min(datum)
FROM
(
    SELECT history_id, obs_day, arprecip.n_day_sum(history_id, obs_day, '4 days'::interval) as datum
    FROM arprecip.daily_precip_mv
    WHERE history_id = $1
    ORDER BY datum DESC
    LIMIT (SELECT floor(stn_years/$2) FROM arprecip.station_stats_v WHERE history_id = $1)
) as foo
$$
LANGUAGE SQL;

CREATE OR REPLACE FUNCTION arprecip.obs_count(history_id int)
RETURNS bigint AS
$$
SELECT count(*)
FROM arprecip.daily_precip_mv
WHERE history_id = $1
$$
LANGUAGE SQL;