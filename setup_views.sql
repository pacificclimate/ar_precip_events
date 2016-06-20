-- Creates the necessary views

-- All precip vars
CREATE OR REPLACE VIEW arprecip.precip_vars_v AS
(
    SELECT vars_id
    FROM meta_vars
    WHERE short_name like '%lwe_thickness_of_precipitation_amount%' 
    AND cell_method='time: sum'
    AND vars_id <> 441
);

CREATE OR REPLACE VIEW arprecip.valid_networks as
(
    SELECT network_id, network_name
    FROM meta_network
    WHERE network_name IN ('ENV-ASP','AGRI','ENV-AQN','EC',
                'MoTIe','MoTIm','BCH','RTA','MoTI',
                'FRBC','EC_raw','EC_AHCCD',
                'EC_Buoy','MVan','ARDA')
);


-- Station_id and history_id stats
CREATE OR REPLACE VIEW arprecip.station_stats_v as
(
    SELECT history_id, 
           station_id, 
           count(*) as obs_count, 
           arprecip.hist_freq(history_id) as freq,
           min(obs_time) as sdate,
           max(obs_time) as edate,
           date_part('year', max(obs_time))- date_part('year', min(obs_time)) as stn_years,
           extract (days FROM( max(obs_time) - min(obs_time))) as stn_days
    FROM obs_raw NATURAL JOIN meta_history NATURAL JOIN meta_station
    WHERE network_id IN (SELECT network_id FROM arprecip.valid_networks)
    AND vars_id IN (SELECT vars_id FROM arprecip.precip_vars_v)
    GROUP BY history_id, station_id
    HAVING count(*) > 1000
);

CREATE OR REPLACE VIEW arprecip.valid_precip_obs_v as
(
    SELECT history_id, obs_raw_id, obs_time, datum, vars_id, mod_time
    FROM obs_raw
    WHERE history_id IN (SELECT history_id FROM arprecip.station_stats_v)
);

CREATE OR REPLACE VIEW arprecip.daily_view AS
( 
SELECT obs_raw.history_id, 
    date_trunc('day', obs_raw.obs_time) AS obs_day, 
    obs_raw.datum, 
    obs_raw.vars_id,
    1::float as daily_avail
FROM 
( 
    SELECT obs_raw.history_id, 
           obs_raw.obs_raw_id, 
           obs_raw.obs_time, 
           obs_raw.datum, 
           obs_raw.vars_id
    FROM obs_raw
    NATURAL JOIN meta_history
    WHERE (obs_raw.vars_id IN ( SELECT precip_vars_v.vars_id FROM arprecip.precip_vars_v)) 
        AND (obs_raw.history_id IN (SELECT history_id FROM arprecip.station_stats_v)) 
        AND meta_history.freq = 'daily'
) AS obs_raw
NATURAL JOIN meta_vars
LEFT JOIN obs_raw_native_flags USING (obs_raw_id)
LEFT JOIN meta_native_flag USING (native_flag_id)
LEFT JOIN obs_raw_pcic_flags USING (obs_raw_id)
LEFT JOIN meta_pcic_flag USING (pcic_flag_id)
WHERE meta_native_flag.discard IS NOT TRUE 
    AND meta_pcic_flag.discard IS NOT TRUE
);

CREATE OR REPLACE VIEW arprecip.hourly_view AS
( 
SELECT obs_raw.history_id, 
    date_trunc('day', obs_raw.obs_time) AS obs_day, 
    avg(obs_raw.datum) as datum,
    obs_raw.vars_id,
    count(datum) / 24.0 as daily_avail
FROM 
( 
    SELECT obs_raw.history_id, 
           obs_raw.obs_raw_id, 
           obs_raw.obs_time,
           obs_raw.datum, 
           obs_raw.vars_id
    FROM obs_raw
    NATURAL JOIN meta_history
    WHERE (obs_raw.vars_id IN (SELECT precip_vars_v.vars_id FROM arprecip.precip_vars_v)) 
        AND (obs_raw.history_id IN (SELECT history_id FROM arprecip.station_stats_v)) 
        AND meta_history.freq = '1-hourly'
) AS obs_raw
NATURAL JOIN meta_vars
LEFT JOIN obs_raw_native_flags USING (obs_raw_id)
LEFT JOIN meta_native_flag USING (native_flag_id)
LEFT JOIN obs_raw_pcic_flags USING (obs_raw_id)
LEFT JOIN meta_pcic_flag USING (pcic_flag_id)
WHERE meta_native_flag.discard IS NOT TRUE 
    AND meta_pcic_flag.discard IS NOT TRUE
GROUP BY obs_day, obs_raw.history_id, obs_raw.vars_id
);

CREATE OR REPLACE VIEW arprecip.daily_precip_v AS
(
    SELECT *
    FROM arprecip.hourly_view
    WHERE daily_avail > 0.90
    UNION
    SELECT *
    FROM arprecip.daily_view
);

-- daily_precip_v must be stored as a matview daily_precip_mv for the remaining functions/views