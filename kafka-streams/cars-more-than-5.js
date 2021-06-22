const { KafkaStreams } = require("kafka-streams");

// Kafka stream config
const config = require("./config.json");
const factory = new KafkaStreams(config);

factory.on("error", (error) => {
  console.log("Error occured:", error.message);
});

// get original Stream (S1)
const kstream = factory.getKStream("inferproven-kafka-traffic-topic");

// generate modified Stream (S2)
kstream
  .mapJSONConvenience()
  .filter((kv) => kv.vehicleCount > 5) //Data Reduction
  .map((kv) => {
    //provenance generating function
  })
  .tap((kv) => console.log(kv)) // for debugging
  .to("inferproven-kafka-traffic-topic-morethan5");
