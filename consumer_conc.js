import TopicConsumer from 'kafka-node-topic-consumer';
 
// create a new TopicConsumer
const options = {
    autoCommit: true,
    fetchMaxWaitMs: 1000,
    fetchMaxBytes: 1024 * 1024,
    encoding: "buffer"
};

const consumer = new TopicConsumer({
  host: "localhost:2181",
  consumer: { groupId: 'my-consumer-group' },
  topic: 'webevents',
  //options
});
 
consumer.registerWorker((msg) => {
	console.log('STARTONG>>>>>>>>>>>>>>>>>>>>>>>>>>');
	let p = new Promise((resolve) => {
		setTimeout(() => {
			console.log(msg);
			resolve();
		}, 5000);
	});

	return p;
});
 
consumer.on('message:error', (err, msg) => {
  console.error(err, msg);
});
 
consumer.connect()