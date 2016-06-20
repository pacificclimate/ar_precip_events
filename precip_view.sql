CREATE OR REPLACE VIEW arprecip.three_day_precip_v AS
(
    SELECT history_id, obs_day, arprecip.n_day_sum(history_id, obs_day, '2 days'::interval) as datum
    FROM arprecip.daily_precip_mv
);

CREATE OR REPLACE VIEW arprecip.five_day_precip_v AS
(
    SELECT history_id, obs_day, arprecip.n_day_sum(history_id, obs_day, '4 days'::interval) as datum
    FROM arprecip.daily_precip_mv
);

CREATE OR REPLACE VIEW arprecip.multiday_precip_v AS
(
    SELECT history_id, obs_day, 
        datum as one_day_precip, 
        arprecip.n_day_sum(history_id, obs_day, '2 days'::interval) as three_day_precip,
        arprecip.n_day_sum(history_id, obs_day, '4 days'::interval) as five_day_precip,
        vars_id,
        daily_avail
    FROM arprecip.daily_precip_mv
);
