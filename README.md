# big_data_challenge
## Kafka
```
kafka-topics --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic iot-device-log
```

## Hive
```
CREATE EXTERNAL TABLE iot_device_log (id BIGINT, device_id STRING, temperature INT, latitude FLOAT, longitude FLOAT, time STRING)
STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
WITH SERDEPROPERTIES ("hbase.columns.mapping" = ":key,device:id,metric:temperature,location:latitude,location:longitude,time:time")
TBLPROPERTIES("hbase.table.name" = "iot_device_log");
```

## HBase
Table: iot_device_log

Key: deviceId + timestamp

device | metric | location | time
------ | ------ | -------- | ----
id | temperature | latitude, longitude | time

## Impala
```
invalidate metadata iot_device_log;
```
```sql
select device_id, max(temperature)
    from iot_device_log
    group by device_id;
```
![Max temperature](https://github.com/okabanov18/big_data_challenge/blob/master/images/temperature_by_deviceId.png?raw=true)
```sql
select device_id, count(*)
    from iot_device_log
    group by device_id;
```
![Count rows by device](https://github.com/okabanov18/big_data_challenge/blob/master/images/count_by_deviceId.png?raw=true)
```sql
select device_id, max(temperature)
    from iot_device_log
    where split_part(time, 'T', 1) = '2018-09-19'
    group by device_id;
```
![Max temperature with filter](https://github.com/okabanov18/big_data_challenge/blob/master/images/temperature_by_deviceId_filtered.png?raw=true)
