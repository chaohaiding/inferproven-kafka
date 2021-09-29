const { KafkaStreams } = require("kafka-streams");
const { CompressionTypes, CompressionCodecs } = require("kafkajs");
const SnappyCodec = require("kafkajs-snappy");

CompressionCodecs[CompressionTypes.Snappy] = SnappyCodec;

const neo4j = require("neo4j-driver");
const kafka = require("../consumer/kafka");
// Kafka stream config
const config = require("./config.json");
const factory = new KafkaStreams(config);

var driver = neo4j.driver(
  "bolt://localhost:7687",
  neo4j.auth.basic("neo4j", "Southampton11")
);

/**
 * createNode: create a single Node
 * @param {*} node
 */

const createNode = (node) => {
  var session = driver.session();
  session
    .run(
      `CREATE (n:Node $node)
          RETURN n
    `,
      {
        node: node,
      }
    )
    .then(function (result) {
      result.records.forEach(function (record) {
        console.log(record);
      });
      session.close();
    })
    .catch(function (error) {
      console.log(error);
    });
};

/*
 *createNodesBlank
 */
const createNodesBlank = (node_s1, node_s2, node_s3, rel) => {
  var session = driver.session({ database: "leftjoin" });
  session
    .run(
      `CREATE (n1:Node $node_1), (n2:Node $node_2)
          MERGE (n3:Node {key:$node_3_key})
          MERGE (fn:Function {name:$fn_name})
          MERGE (n2)-[:GENERATED_BY]->(fn)
          MERGE (fn)-[:USED_BY]->(n1)
          RETURN fn
    `,
      {
        node_1: node_s1,
        node_2: node_s2,
        fn_name: rel,
        node_3_key: node_s3.key,
      }
    )
    .then(function (result) {
      result.records.forEach(function (record) {
        console.log(record);
      });
      session.close();
    })
    .catch(function (error) {
      console.log(error);
    });
};

/*
 *createNodes
 */
const createNodes = (node_s1, node_s2, node_s3, rel) => {
  var session = driver.session({ database: "leftjoin" });
  session
    .run(
      `CREATE (n1:Node $node_1),(n2:Node $node_2),(n3:Node $node_3)
          MERGE (fn:Function {name:$fn_name})
          MERGE (n2)-[:GENERATED_BY]->(fn)
          MERGE (fn)-[:USED_BY]->(n1)
          RETURN fn
    `,
      {
        node_1: node_s1,
        node_2: node_s2,
        node_3: node_s3,
        fn_name: rel,
      }
    )
    .then(function (result) {
      result.records.forEach(function (record) {
        console.log(record);
      });
      session.close();
    })
    .catch(function (error) {
      console.log(error);
    });
};

factory.on("error", (error) => {
  console.log("Error occured:", error.message);
});

// get original Stream (S1)
const kstream1 = factory.getKStream("inferproven-kafka-traffic-topic");

kstream1
  .mapJSONConvenience()
  .map((kv) => {
    if (kv) {
      kv.otherKey = kv.value.id;
    }
    return kv;
  })
  .tap((kv) => {
    //console.log(JSON.stringify(kv));
  });

// get original Stream (S2)
const kstream2 = factory.getKStream("inferproven-kafka-traffic-topic-2");

kstream2
  .mapJSONConvenience()
  .map((kv) => {
    if (kv) {
      kv.otherKey = kv.value.id;
    }
    return kv;
  })
  .tap((kv) => {
    //console.log(JSON.stringify(kv));
  });

// generate modified Stream (S3)
const fn_Object = {
  id: "1",
  name: "JoinOperation",
};
const joinedStream = kstream1.innerJoin(kstream2);
joinedStream
  .mapJSONConvenience() //deserialise to JSON object
  .tap((kv) => {
    /*kv.left.value = kv.left.value.toString();
    kv.left.key = kv.left.key.toString();
    kv.right.value = kv.right.value.toString();
    kv.right.key = kv.right.key.toString();*/
    //console.log(JSON.stringify(kv));
    console.log(kv);
  })
  .filter((kv) => {
    /*const kvLeft = kv.left;
    const kvRight = kv.right;
    if (kvLeft.key === kvRight.key) {
      const node_s3 = JSON.parse(JSON.stringify(kvLeft));
      node_s3.value.vehicleCount =
        kvLeft.value.vehicleCount + kvRight.value.vehicleCount;
      node_s3.fnName = fn_Object.name;
      node_s3.id = fn_Object.id;
      createNodes(kvLeft, kvRight, node_s3, fn_Object);
      return true;
    } else {
      const node_s3 = {
        key: 0,
        value: "",
      };
      createNodesBlank(kvLeft, kvRight, node_s3, fn_Object);
      return false;
    }*/
  })
  //.tap((kv) => console.log(kv)) // for debugging
  .wrapAsKafkaValue()
  .to("inferproven-kafka-traffic-topic-carsjoin");

kstream1.start();
kstream2.start();
