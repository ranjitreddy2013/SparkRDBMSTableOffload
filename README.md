#Spark Sample Maven Project to Load data from MySQL database

Make sure to keep the following MySQL Maven dependency to avoid MySQL driver related issues.


```
<dependency>
    <groupId>mysql</groupId>
    <artifactId>mysql-connector-java</artifactId>
    <version>5.1.6</version>
</dependency>
```

Submit the job using spark-submit on yarn as follows......

```
/opt/mapr/spark/spark-*/./bin/spark-submit \
--class com.sparkdboffload.SparkDBOffloadApp \
--master yarn \
--deploy-mode cluster \
--executor-memory 1G \
--num-executors 2 \
target/SparkDatabaseOffload-1.0-SNAPSHOT.jar
```
