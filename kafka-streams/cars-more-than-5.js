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

/* 
*createNodes
*/
const createNodes = (node_s1, node_s2, rel) => {
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
    .run(`CREATE (n1:Node $node_1), (n2:Node $node_2)
          MERGE (fn:Function {name:$fn_name}) 
          MERGE (n1)<-[:USED_BY]-(fn)
          MERGE (fn)<-[:GENERATED_BY]-(n2)
          RETURN fn
    `, {node_1:node_s1, fn_name:rel, node_2:node_s2})
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

kstream
  .mapJSONConvenience() //deserialise to JSON object
  .tap((kv) => {
    // get node_s1
    node_s1 = kv;
  })
  .filter((kv) => kv.value.vehicleCount > 5) //Data Reduction
  .tap((kv) => {
    //console.log(kv);
    node_s2 = kv;
    //provenance generating function
    if (node_s2.value.id === node_s1.value.id) {
      // go to neo4j
      let nodes_s2_val=JSON.parse(JSON.stringify(node_s2.value));
      nodes_s2_val.vehicleCount="MoreThanFive";
      nodes_s2_val.fn_name="Fn_MoreThanFive";
      createNodes(node_s1.value, nodes_s2_val, "Fn_MoreThanFive");
    }
  })
  //.tap((kv) => console.log(kv)) // for debugging
  .wrapAsKafkaValue()
  .to("inferproven-kafka-traffic-topic-morethan5");

kstream.start();
