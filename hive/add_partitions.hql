
ALTER TABLE event ADD PARTITION(pd="2018-11-26") LOCATION "/user/cloudera/events/18/11/26";
ALTER TABLE event ADD PARTITION(pd="2018-11-27") LOCATION "/user/cloudera/events/18/11/27";
ALTER TABLE event ADD PARTITION(pd="2018-11-28") LOCATION "/user/cloudera/events/18/11/28";
ALTER TABLE event ADD PARTITION(pd="2018-11-29") LOCATION "/user/cloudera/events/18/11/29";
ALTER TABLE event ADD PARTITION(pd="2018-11-30") LOCATION "/user/cloudera/events/18/11/30";
ALTER TABLE event ADD PARTITION(pd="2018-12-01") LOCATION "/user/cloudera/events/18/12/01";
ALTER TABLE event ADD PARTITION(pd="2018-12-02") LOCATION "/user/cloudera/events/18/12/02";
ALTER TABLE event ADD PARTITION(pd="2018-12-03") LOCATION "/user/cloudera/events/18/12/03";
