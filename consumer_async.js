import kafka from "kafka-node"
import queue from 'async/queue';
 
const client = new kafka.Client("localhost:2181");
 
const topics =  (i) => [
    {
        topic: "webevents",
        partition: 1
    }
];

const options = {
    autoCommit: true,
    fetchMaxWaitMs: 1000,
    fetchMaxBytes: 1024 * 1024,
    encoding: "buffer"
};
 
// create a queue object with concurrency 1
let q = []
for(let i=0; i<3; i++){
    q.push(
        queue(function(message, callback) {
            console.log('STARTING');
            console.log(message);
            setTimeout(() => callback(), 7000);
            //callback();
            }, 1)
    )
    // assign a callback
    q[i].drain = function() {
        console.log('all items have been processed');
    };
}


let consumers = [];

for(let i=0; i<3; i++){
    let consumer = new kafka.Consumer(client, topics(i), options);
    
    ((c, i) => {

        consumers.push(consumer, i);

        c.on("message", function(message) {
 
        // Read string into a buffer.
        var buf = new Buffer(message.value, "binary"); 
        var decodedMessage = JSON.parse(buf.toString());

        q[i].push(decodedMessage, (err) => console.log('all done'));
        
        });
         
        c.on("error", function(err) {
            console.log("error", err);
        });
    })(consumer, i);
    
}

console.log(JSON.stringify(consumers));
console.log(JSON.stringify(q));

process.on("SIGINT", function() {
    consumer.close(true, function() {
        process.exit();
    });
});