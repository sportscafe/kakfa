import kafka from "kafka-node";
import queue from "async/queue";



function createConsumer(partitionId) {
  const client = new kafka.Client("localhost:2181");
  const topics = [
    {
      topic: "webevents",
      partition: partitionId
    }
  ];

  const options = {
    autoCommit: true,
    fetchMaxWaitMs: 1000,
    fetchMaxBytes: 1024 * 1024,
    encoding: "buffer"
  };

  const q = queue((message, callback) => {
    setImmediate(() => {
      console.log('STARTING');
      console.log(message);
      setTimeout(() => callback(), 7000);
    });
  }, 1);

  q.drain = () => {
    console.log(`all items in queue ${partitionId} got processed`);
  }

  const consumer = new kafka.Consumer(client, topics, options);
  consumer.on('message', (message) => {
    var buf = new Buffer(message.value, "binary");
    var decodedMessage = JSON.parse(buf.toString());
    q.push(decodedMessage, (err) => console.log('all done'));
  });

  consumer.on("error", function(err) {
    console.log("error", err);
  });

  return consumer;
}

let consumer = [];

consumer.push(createConsumer(0));
consumer.push(createConsumer(1));
consumer.push(createConsumer(2));

process.on("SIGINT", function() {
  consumer.forEach((c) => {
    c.close(true);
  });

  setTimeout(() => {
    process.exit();
  }, 2000);
});
