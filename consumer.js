import kafka from "kafka-node"
 
const client = new kafka.Client("localhost:2181");
 
const topics = [
    {
        topic: "webevents",
        partition: 0
    }
];

const options = {
    autoCommit: true,
    highWaterMark: 1
    //fetchMaxWaitMs: 1000,
    //fetchMaxBytes: 1024 * 1024,
    //encoding: "buffer"
};

 
const consumer = new kafka.Consumer(client, topics, options);
 
consumer.on("message", function(message) {
    
    console.log(message);
    //consumer.pause();
    // Read string into a buffer.
    //var buf = new Buffer(message.value, "binary"); 
    //var decodedMessage = JSON.parse(buf.toString());
 
    //Events is a Sequelize Model Object. 
    console.log('Received Msg .. printing .............');
    //console.log(decodedMessage);
    setTimeout(() => {
        console.log('job finished');
        //consumer.resume();
    }, 5000);
    
});
 
consumer.on("error", function(err) {
    console.log("error", err);
});
 
process.on("SIGINT", function() {
    consumer.close(true, function() {
        process.exit();
    });
});