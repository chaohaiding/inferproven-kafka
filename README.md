# inferproven-kafka
InferProven Kafka Project based on Kafka Node JS


# Prerequisites
- install docker
- install nodejs v:14.2.0
- Neo4j

# How to generate provenance graph

## clone the project
``` 
git clone https://github.com/chaohaiding/inferproven-kafka.git
```
## start the kafka and zookeeper docker container
``` shell
docker-compose up -d
```

## start sensor 
The sensor script is used to read data from CSV file and post to the producor, INTERVAL is the time interval in ms to send each record 
``` shell
cd sensor && INTERVAL=200 node index.js
```

## start producor
The producor server is used to simulator kafka producor

``` shell
cd producor && KAFKA_BOOTSTRAP_SERVER="localhost:9092" TOPIC="inferproven-kafka-traffic-topic" node server.js
```

## start consumer
The consumer server is used to simulator kafka consumer

``` shell
cd consumer && KAFKA_BOOTSTRAP_SERVER="localhost:9092" TOPIC="inferproven-kafka-traffic-topic" GROUP_ID="group-id" node index.js
```
## start Kafka Stream to generate provenance graph

cars-more-than-5.js is used to generate provenance graph and push the data into local neo4j database.

``` shell
cd kafka-streams && npm install
node cars-more-than-5.js
```


