const express = require("express");
const bodyParser = require("body-parser");
const kafka = require("./kafka");

// Initialize express and define a port
const app = express();
const PORT = 3000;

// kafka producer;
const producer = kafka.producer();
const topic = process.env.TOPIC || "inferproven-kafka-traffic-topic";
const main = async () => {
  const admin = kafka.admin();
  await admin.connect();
  await producer.connect();
  await admin.createTopics({
    waitForLeaders: true,
    topics: [
      {
        topic: topic,
      },
    ],
  });

  // Tell express to use body-parser's JSON parsing
  app.use(bodyParser.json());
  //let preNode = {};

  // Set up a hook router for traffic data notification
  app.post("/hook", async (req, res) => {
    //console.log(req.body) // sensor data
    const event = req.body;
    const value = {
      id: preNode._id,
      vehicleCount: preNode.vehicleCount,
      avgMeasuredTime: preNode.avgMeasuredTime,
      medianMeasuredTime: preNode.medianMeasuredTime,
      extID: preNode.extID,
      avgSpeed: preNode.avgSpeed,
      REPORT_ID: preNode.REPORT_ID,
      TIMESTAMP: preNode.TIMESTAMP,
    };

    try {
      const responses = await producer.send({
        topic: topic,
        messages: [
          {
            // _id as key, to make sure that we process events in order
            key: event._id,
            // The message value is just bytes to Kafka, so we need to serialize our JavaScript
            // object to a JSON string. Other serialization methods like Avro are available.
            value: JSON.stringify(value),
          },
        ],
      });
      console.log("Published message to topic:" + topic, {
        responses,
      });
    } catch (error) {
      console.error("Error publishing message to topic:" + topic, error);
    }

    //preNode = event;
    res.status(200).end(); // Responding is important
    /*
      {
        status: 'OK',
        avgMeasuredTime: '66',
        avgSpeed: '56',
        extID: '668',
        medianMeasuredTime: '66',
        TIMESTAMP: '2014-02-13T11:30:00',
        vehicleCount: '7',
        _id: '190000',
        REPORT_ID: '158324'
      }
      */
    // pre node exist?
    /*if (Object.keys(preNode).length !== 0) {
    
      const node1 = {
        id: preNode._id,
        vehicleCount: preNode.vehicleCount,
        avgMeasuredTime: preNode.avgMeasuredTime,
        medianMeasuredTime: preNode.medianMeasuredTime,
        extID: preNode.extID,
        avgSpeed: preNode.avgSpeed,
        REPORT_ID: preNode.REPORT_ID,
        TIMESTAMP: preNode.TIMESTAMP,
      };

      const node2 = {
        id: event._id,
        vehicleCount: event.vehicleCount,
        avgMeasuredTime: event.avgMeasuredTime,
        medianMeasuredTime: event.medianMeasuredTime,
        extID: event.extID,
        avgSpeed: event.avgSpeed,
        REPORT_ID: event.REPORT_ID,
        TIMESTAMP: event.TIMESTAMP,
      };

      const value = {
        node1: node1,
        edge: event.TIMESTAMP,
        node2: node2,
      };*/
    /*
      const value = {
        "nodes":[node1, node2],
        "edge": event.TIMESTAMP,
      }
    } */
  });

  // Start express on the defined port
  app.listen(PORT, () =>
    console.log(`🚀 Traffic Producer server is running on port ${PORT}`)
  );
};

main().catch((error) => {
  console.error(error);
  process.exit(1);
});
