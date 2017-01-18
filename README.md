# kafka-connect-mongodb

The MongoDB sink connector for Kafka Connect provide a simple, continuous link from a Kafka topic or set of topics to MonogoDB collection or collections.

The connector consumes Kafka messages, rename message fields, selecting specific fields and insert or update thous fields to the MongoDB collection.

The Connector is available as source at Github or as binary HERE

#### Connector Configurations:
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