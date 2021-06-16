const {KafkaStreams} = require("kafka-streams");

const config = require("./config.json");
const factory = new KafkaStreams(config);
