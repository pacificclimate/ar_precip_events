library(RPostgreSQL)
library(crmp)

get.daily.thresholds <- function(con){
  query = sprintf('SELECT history_id, "1-year", "5-year", "10-year", "25-year", "50-year", daily_avail, st_x(the_geom) as x, st_y(the_geom) as y
    FROM arprecip.daily_thresh_mv JOIN meta_history USING (history_id)
    WHERE daily_avail > 0.95')
  rs <- dbSendQuery(con, statement=query)
  rs <- fetch(rs, n=-1)
  rs
}

get.three.day.thresholds <- function(con){
  query = sprintf('SELECT history_id, "1-year", "5-year", "10-year", "25-year", "50-year", daily_avail, st_x(the_geom) as x, st_y(the_geom) as y
    FROM arprecip.three_daily_thresh_mv JOIN meta_history USING (history_id)
    WHERE daily_avail > 0.95')
  rs <- dbSendQuery(con, statement=query)
  rs <- fetch(rs, n=-1)
  rs
}

get.five.day.thresholds <- function(con){
  query = sprintf('SELECT history_id, "1-year", "5-year", "10-year", "25-year", "50-year", daily_avail, st_x(the_geom) as x, st_y(the_geom) as y
    FROM arprecip.five_daily_thresh_mv JOIN meta_history USING (history_id)
    WHERE daily_avail > 0.95')
  rs <- dbSendQuery(con, statement=query)
  rs <- fetch(rs, n=-1)
  rs
}

get.extreme.events <- function(con, hist.id, days, recurrence) {
  d <- ifelse(days==1, '',
       ifelse(days==3, 'three_',
       ifelse(days==5, 'five_',
       NA )))
  query = sprintf('SELECT "%s-year" FROM arprecip.%sdaily_thresh_mv WHERE history_id = %s',recurrence, d, hist.id)
  rs <- dbSendQuery(con, statement=query)
  thresh <- fetch(rs, n=-1)
  
  d <- ifelse(days==1, 'one',
       ifelse(days==3, 'three',
       ifelse(days==5, 'five',
       NA )))
  query = sprintf('SELECT obs_day, %s_day_precip FROM arprecip.multiday_precip_mv WHERE history_id = %s AND %s_day_precip > %s ORDER BY obs_day', d, hist.id, d, thresh)
  rs <- dbSendQuery(con, statement=query)
  events <- fetch(rs, n=-1)
  events
}

get.stn.ids <- function(con) {
  query = sprintf('SELECT station_id FROM arprecip.station_stats_mv')
  rs <- dbSendQuery(con, statement=query)
  stn.ids <- fetch(rs, n=-1)
  stn.ids[,1]
}

get.hist.ids <- function(con) {
  query = sprintf('SELECT history_id FROM arprecip.station_stats_mv')
  rs <- dbSendQuery(con, statement=query)
  hist.ids <- fetch(rs, n=-1)
  hist.ids[,1]
}

get.stn.stats <- function(con) {
  query = sprintf('SELECT * FROM arprecip.station_stats_mv')
  rs <- dbSendQuery(con, statement=query)
  stats <- fetch(rs, n=-1)
  stats
}

stn.id.to.hist.id <- function(con, stn.id) {
  query = sprintf('SELECT history_id FROM arprecip.station_stats_mv WHERE station_id = %s', stn.id)
  rs <- dbSendQuery(con, statement=query)
  hist.id <- fetch(rs, n=-1)
  hist.id[,]
}

hist.id.to.stn.id <- function(con, hist.id) {
  query = sprintf('SELECT station_id FROM arprecip.station_stats_mv WHERE history_id = %s',hist.id)
  rs <- dbSendQuery(con, statement=query)
  stn.id <- fetch(rs, n=-1)
  stn.id[,]
}

test <- function(con) {
  print('Get all daily thresholds')
  dt <- get.daily.thresholds(con)
  print(head(dt))

  print('Get extreme events for history id 11454 for one day yearly recurrence events')
  events <- get.extreme.events(con, hist.id=11454, days=1, recurrence=1)
  print(head(events))
}
# open database connection
con <- dbConnect(PostgreSQL(), user='httpd', dbname='crmp')

test(con)
