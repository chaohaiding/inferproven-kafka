const kafka = require("./kafka");
const { CompressionTypes, CompressionCodecs } = require("kafkajs");
const SnappyCodec = require("kafkajs-snappy");

CompressionCodecs[CompressionTypes.Snappy] = SnappyCodec;
const consumer = kafka.consumer({
  groupId: process.env.GROUP_ID || "inferproven-kafka-traffic-consumers",
});

const main = async () => {
  await consumer.connect();
  await consumer.subscribe({
    topic: process.env.TOPIC || "inferproven-kafka-traffic-topic-morethan5",
    //fromBeginning: true,
  });

  await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      console.log("Received message", {
        topic,
        partition,
        key: message.key.toString(),
        value: message.value.toString(),
      });
    },
  });
};

main().catch(async (error) => {
  console.error(error);
  try {
    await consumer.disconnect();
  } catch (e) {
    console.error("Failed to gracefully disconnect consumer", e);
  }
  process.exit(1);
});
