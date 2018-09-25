# Big data challenge

## Usage
- In order to start the emulator: ru.okabanov.challenge.emulator.DeviceLogEmulatorApp.scala
- For starting spark application use: ./submit_local.sh
- Impala queries see below

## Improvements:
- Default spark version (1.6.0) was used from latest Cloudera image. It's old version and need to upgrade it.
- There are no integration tests with Kafka and HBase.
- Need to separate project to 3 modules: model, emulator, log-processing. It can reduce jars size for spark application.

## Kafka
./create_topics.sh
```
kafka-topics --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic iot-device-log
```

## HBase
Table: iot_device_log

Key: deviceId + timestamp

device | metric | location | time
------ | ------ | -------- | ----
id | temperature | latitude, longitude | time

![Table view](https://github.com/okabanov18/big_data_challenge/blob/master/images/HBase_table.png?raw=true)

## Hive
```
CREATE EXTERNAL TABLE iot_device_log (id BIGINT, device_id STRING, temperature INT, latitude FLOAT, longitude FLOAT, time STRING)
STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
WITH SERDEPROPERTIES ("hbase.columns.mapping" = ":key,device:id,metric:temperature,location:latitude,location:longitude,time:time")
TBLPROPERTIES("hbase.table.name" = "iot_device_log");
```

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
===
```sql
select device_id, count(*)
    from iot_device_log
    group by device_id;
```
![Count rows by device](https://github.com/okabanov18/big_data_challenge/blob/master/images/count_by_deviceId.png?raw=true)
===
```sql
select device_id, max(temperature)
    from iot_device_log
    where split_part(time, 'T', 1) = '2018-09-19'
    group by device_id;
```
![Max temperature with filter](https://github.com/okabanov18/big_data_challenge/blob/master/images/temperature_by_deviceId_filtered.png?raw=true)
===
