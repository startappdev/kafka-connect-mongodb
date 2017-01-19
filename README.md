# kafka-connect-mongodb

The MongoDB sink connector for Kafka Connect provide a simple, continuous link from a Kafka topic or set of topics to MonogoDB collection or collections.

The connector consumes Kafka messages, rename message fields, selecting specific fields and insert or update thous fields to the MongoDB collection.

The Connector is available as source at Github or as binary HERE

### Connector Configurations:
| Parameter              |     Description                                                              | Possible values                         |
| ---------------------- |------------------------------------------------------------------------------| ----------------------------------------|
| db.host                | host name of the database                                                    | "localhost"                             |
| db.port                | port number of the database                                                  | "27017"                                 |
| db.name                | name of the database                                                         | "myDataBase"                            |
| db.collections         | name of the collection                                                       | "myCollection                           |       
| write.batch.enabled    | use batch writing when a task is getting more records than write.batch.size  | "true"/"false"                          |           
| write.batch.size       | the batch size when using batch writing                                      | "200"                                   |           
| connect.use_schema     | true if the data in the topic contains schema                                | "true"/"false"                          |           
| record.fields.rename   | rename fields name from the data in the topic                                | "field1=>newField1, field2=>newField2"  |               
| record.keys            | keys in the db to update by                                                  | "key1,key2"                             |   
| record.fields          | specific fields from the record to insert the db                             | "field1,field2"                         |       

### Quick Start
In the following example we will produce json data to a Kafka Topic with out schema, and insert it to a test collection in our MongoDB database with the connector in distributed mode.
##### Pre start
* Download Kafka 0.9.0.0 or later.
* Create new database in your MongoDB named "testdb" and in that database, create new collection named "testcollection".
##### Start Kafka
* Start Zookeeper:
	```
  	$./bin/zookeeper-server-start.sh config/zookeeper.properties
  	```
* Start Kafka Broker:
	```
   	$./bin/kafka-server-start.sh config/server.properties
   	```
* Create a test topic:
    ```
   $./bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 5 --topic testTopic
    ```
##### Start Kafka Connect worker
* Copy the jar file of the connector to your workspace folder:
    ```
   $cp /your-jar-location/kafka-connect-mongodb-assembly-1.0.jar /tmp/
    ```
* Copy worker configuration file to your workspace directory:
    ```
   $cp config/connect-distributed.properties /tmp/
    ```
* Modify the properties file to:
    ```
    bootstrap.servers=localhost:9092
    
    group.id=testGroup
    
    key.converter=org.apache.kafka.connect.json.JsonConverter
    value.converter=org.apache.kafka.connect.json.JsonConverter
    
    key.converter.schemas.enable=false
    value.converter.schemas.enable=false
    
    internal.key.converter=org.apache.kafka.connect.json.JsonConverter
    internal.value.converter=org.apache.kafka.connect.json.JsonConverter
    internal.key.converter.schemas.enable=false
    internal.value.converter.schemas.enable=false
    
    offset.storage.topic=connectoffsets
    
    offset.flush.interval.ms=10000
    config.storage.topic=connectconfigs
    ```

    Notice that if your topic has a lot of data, you may suffer from timeouts and rebalance issues because of pulling too much records at once, for handle ityou can override the maximum number of records returned by polling by putting consumer.max.partition.fetch.bytes=N in your worker configs, where N is a number that is small enough not to trigger the timeout, but large enough that you don't suffer from effectively synchronous message processing.
* Create topics for connector offsets and configs:
    ```
   $./bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 5 --partitions 5 --topic connectoffsets
   $./bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 5 --partitions 1 --topic connectconfigs
    ```

