1) set up schema

create schema arprecip authorization arprecip;
grant all on schema arprecip to arprecip;
grant all on schema arprecip to steward;

2) import hist_freq.sql

3) import setup_views.sql

4) create daily_precip_v into matview daily_precip_mv

select arprecip.create_matview('arprecip.daily_precip_mv', 'arprecip.daily_precip_v');
create index daily_precip_comp_idx on arprecip.daily_precip_mv using btree (history_id, obs_day); 
select arprecip.create_matview('arprecip.station_stats_mv', 'arprecip.station_stats_v');
create index station_stats_hist_idx on arprecip.station_stats_mv using btree (history_id);


5) import functions.sql

6) import threshold_views.sql, precip_views.sql

7) create matviews:

select arprecip.create_matview('arprecip.daily_thresh_mv', 'arprecip.daily_thresh_v');
select arprecip.create_matview('arprecip.three_daily_thresh_mv', 'arprecip.three_daily_thresh_v');
select arprecip.create_matview('arprecip.five_daily_thresh_mv', 'arprecip.five_daily_thresh_v');