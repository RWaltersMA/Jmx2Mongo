# JMX2Mongo

This tool will sample JMX MBeans and copy them to a time series collection in MongoDB.

## Usage

jmx2mongo [OPTIONS]

OPTIONS

| Parameter | Default Value | Description |
|:------------- |-------------|------------|
| -h     |   |  Shows option list|
| -service     |  service:jmx:rmi:///jndi/rmi://127.0.0.1:9999/jmxrmi  |  JMX Service URL |
| -objectname      | REQUIRED |  Object Name of the MBean or an Object Name pattern for a MBean type that contains the data you want to collect.  |
| -mongo    | mongodb://localhost |  MongoDB Connection String   |
| -database      | jmx2mongo |  Write metrics to this Database Name |
| -collection      | metrics_ts |  Write metrics to this time series collection.  Creates one if doesn't exist. |

## Examples

Write MongoDB Kafka Connector MBeans to a MongoDB Atlas Cluster

```
java -jar jmx2mongo.jar -mongo "mongodb+srv://<USERNAME>:<PASSWORD>@demo.lkyil.mongodb.net/?retryWrites=true&w=majority" -objectname "com.mongodb:name=*,type=MongoDBKafkaConnector"
```

## Contribute

This project was primarily tested using the JMX MBeans exposed for the MongoDB Connector for Apache Kafka.  Chances are there may be bugs with other MBeans if you try other ObjectNames.  I don't claim to be a Java developer, I don't even play one on TV.  I appreciate any PRs to make this code more robust/usable with this JMX to MongoDB scenario.

Thanks,
Rob
