# big_data_challenge
## Kafka create topic
kafka-topics --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic iot-device-log

## Hive
CREATE EXTERNAL TABLE iot_device_log (id BIGINT, deviceId STRING, temperature INT, latitude FLOAT, longitude FLOAT, time STRING)
STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
WITH SERDEPROPERTIES ("hbase.columns.mapping" = ":key,device:id,metric:temperature,location:latitude,location:longitude,time:time")
TBLPROPERTIES("hbase.table.name" = "iot_device_log");

invalidate metadata iot_device_log;

## impala
select deviceid, max(temperature)
    from iot_device_log
    group by deviceid;

select deviceid, count(*)
    from iot_device_log
    group by deviceid;

select max(temperature)
    from iot_device_log
    where day = '';