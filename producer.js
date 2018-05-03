import kafka from "kafka-node";
import uuid from "uuid";
 
const client = new kafka.Client("localhost:2181", "my-client-id", {
    sessionTimeout: 300,
    spinDelay: 100,
    retries: 2
});
 
const producer = new kafka.Producer(client);
producer.on("ready", function() {
    console.log("Kafka Producer is connected and ready.");
});
 
// For this demo we just log producer errors to the console.
producer.on("error", function(error) {
    console.error(error);
});
 
const KafkaService = {
    sendRecord: ({ type, userId, sessionId, data, partitionId }, callback = () => {}) => {
        if (!userId) {
            return callback(new Error(`A userId must be provided.`));
        }
 
        const event = {
            id: uuid.v4(),
            timestamp: Date.now(),
            userId,
            sessionId,
            type,
            data,
            partitionId
        };
 
        const buffer = new Buffer.from(JSON.stringify(event));
 
        // Create a new payload
        const record = [
            {
                topic: "webevents",
                messages: buffer,
                partition: partitionId || 0,
                attributes: 1 /* Use GZip compression for the payload */
            }
        ];
 
        //Send record to Kafka and log result/error
        producer.send(record,  (err, data ) => {
            if(err) console.log(err);
            console.log(data);
        });
    }
};
 
export default KafkaService;