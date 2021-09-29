const { KafkaStreams } = require("kafka-streams");
const { CompressionTypes, CompressionCodecs } = require("kafkajs");
const SnappyCodec = require("kafkajs-snappy");

CompressionCodecs[CompressionTypes.Snappy] = SnappyCodec;

const neo4j = require("neo4j-driver");
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
      { node: node }
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
const createNodesBlank = (node_s1, node_s2, rel) => {
  var session = driver.session({
    database: "morethanfive",
    defaultAccessMode: neo4j.session.WRITE,
  });
  session
    .run(
      `CREATE (n1:Node $node_1)
        MERGE (n2:Node {key:$node_2_key})
          MERGE (fn:Function {id:$fn_id, name:$fn_name}) 
          MERGE (fn)-[:USED_BY]->(n1)
          MERGE (n2)-[:GENERATED_BY]->(fn)
          RETURN fn
    `,
      {
        node_1: node_s1,
        fn_name: rel.name,
        fn_id: rel.id,
        node_2_key: node_s2.key,
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
const createNodes = (node_s1, node_s2, rel) => {
  var session = driver.session({
    database: "morethanfive",
    defaultAccessMode: neo4j.session.WRITE,
  });
  session
    .run(
      `CREATE (n1:Node $node_1), (n2:Node $node_2)
          MERGE (fn:Function {id:$fn_id, name:$fn_name}) 
          MERGE (fn)-[:USED_BY]->(n1)
          MERGE (n2)-[:GENERATED_BY]->(fn)
          RETURN fn
    `,
      {
        node_1: node_s1,
        fn_name: rel.name,
        fn_id: rel.id,
        node_2: node_s2,
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
const _createNodes = (node_s1, node_s2, rel) => {
  console.log(node_s1, node_s2);
  var session = driver.session();
  session
    /*.run(
      `CREATE (n:Node ${JSON.stringify(node_s1)}), (m:Node ${JSON.stringify(node_s2)}) 
        RETURN n, m`
    )*/
    /*.run(`CREATE p = (n1:Node ${JSON.stringify(node_s1)})-[:USED_BY]<-(fn:Function {name:$fn_name})<-[:GENERATED_BY]-(n2:Node ${JSON.stringify(node_s2)})
    RETURN p`, { fn_name: rel })*/
    /*.run(`MERGE (fn:Function {name:$fn_name})
    ON CREATE p = (n1:Node $node_1)<-[:USED_BY]-(fn:Function {name:$fn_name})<-[:GENERATED_BY]-(n2:Node $node_2)
    RETURN p`, { node_1:node_s1, fn_name:rel, node_2:node_s2 })*/
    .run(
      `CREATE (n1:Node $node_1), (n2:Node $node_2)
          MERGE (fn:Function {name:$fn_name}) 
          MERGE (n1)<-[:USED_BY]-(fn)
          MERGE (fn)<-[:GENERATED_BY]-(n2)
          RETURN fn
    `,
      { node_1: node_s1, fn_name: rel, node_2: node_s2 }
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
const kstream = factory.getKStream("inferproven-kafka-traffic-topic");

// generate modified Stream (S2)
let node_s1;
let node_s2;

const fn_Object = {
  id: "1",
  name: "MoreThanFive",
};

kstream
  .mapJSONConvenience() //deserialise to JSON object
  .tap((kv) => {
    // get node_s1
    node_s1 = JSON.parse(JSON.stringify(kv.value));
    node_s1.key = kv.key.toString();
    //node_s1._id = node_s1._id.toString();
    node_s1.offset = kv.offset.toString();
  })
  .filter((kv) => {
    const kvValue = JSON.parse(JSON.stringify(kv.value));
    if (kvValue.vehicleCount > 5) {
      node_s2 = kvValue;
      node_s2.key = kv.key.toString();
      node_s2.fnName = fn_Object.fnName;
      node_s2.fnId = fn_Object.id;
      node_s2.offset = kv.offset.toString();
      createNodes(node_s1, node_s2, fn_Object);
      return true;
    } else {
      node_s2 = { key: "0", id: "0", offset: kv.offset.toString() };
      createNodesBlank(node_s1, node_s2, fn_Object);
      return false;
    }
  })
  .tap((kv) => {
    console.log(kv);
    /*node_s2 = kv;
    //provenance generating function
    if (node_s2.value.id === node_s1.value.id) {
      // go to neo4j
      let nodes_s2_val=JSON.parse(JSON.stringify(node_s2.value));
      nodes_s2_val.vehicleCount="MoreThanFive";
      nodes_s2_val.fn_name="Fn_MoreThanFive";
      createNodes(node_s1.value, nodes_s2_val, "Fn_MoreThanFive");
    }*/
  })
  //.tap((kv) => console.log(kv)) // for debugging
  .wrapAsKafkaValue()
  .to("inferproven-kafka-traffic-topic-morethan5");

kstream.start();
