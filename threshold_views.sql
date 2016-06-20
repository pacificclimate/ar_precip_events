CREATE OR REPLACE VIEW arprecip.daily_thresh_v AS
(
    SELECT station_id, 
        history_id,
        arprecip.daily_thresh(history_id, 1) as "1-year",
        arprecip.daily_thresh(history_id, 5) as "5-year",
        arprecip.daily_thresh(history_id, 10) as "10-year",
        arprecip.daily_thresh(history_id, 25) as "25-year",
        arprecip.daily_thresh(history_id, 50) as "50-year",
        arprecip.obs_count(history_id)/stn_days as daily_avail
    FROM arprecip.station_stats_v
);

CREATE OR REPLACE VIEW arprecip.three_daily_thresh_v AS
(
    SELECT station_id, 
        history_id,
        arprecip.three_daily_thresh(history_id, 1) as "1-year",
        arprecip.three_daily_thresh(history_id, 5) as "5-year",
        arprecip.three_daily_thresh(history_id, 10) as "10-year",
        arprecip.three_daily_thresh(history_id, 25) as "25-year",
        arprecip.three_daily_thresh(history_id, 50) as "50-year",
        arprecip.obs_count(history_id)/stn_days as daily_avail
    FROM arprecip.station_stats_v
);

CREATE OR REPLACE VIEW arprecip.five_daily_thresh_v AS
(
    SELECT station_id, 
        history_id,
        arprecip.five_daily_thresh(history_id, 1) as "1-year",
        arprecip.five_daily_thresh(history_id, 5) as "5-year",
        arprecip.five_daily_thresh(history_id, 10) as "10-year",
        arprecip.five_daily_thresh(history_id, 25) as "25-year",
        arprecip.five_daily_thresh(history_id, 50) as "50-year",
        arprecip.obs_count(history_id)/stn_days as daily_avail
    FROM arprecip.station_stats_v
);