const { Kafka } = require('kafkajs');
const util = require('util');

// - intantiating kafkajs client
const kafka = new Kafka({
    clientId: 'anuj',
    brokers: ['localhost:9092'],
});

async function listTopics() {
    const admin = kafka.admin();
    admin.connect();

    const aTopics = await admin.listTopics();
    console.log(`Topics :: ${aTopics}`);

    admin.disconnect();
}

async function createTopics(aTopic) {
    const admin = kafka.admin();
    admin.connect();

    const bIsCreated = await admin.createTopics({
        validateOnly: false,
        waitForLeaders: true,
        timeout: 5000,
        topics: aTopic,
    });
    console.log(`Topics creation :: ${bIsCreated}`);

    admin.disconnect();
}

async function describeTopics(aTopic = []) {
    const admin = kafka.admin();
    admin.connect();

    const aTopicMetaData = await admin.fetchTopicMetadata({ topics: aTopic });
    console.log(`Describe Topics: ${util.inspect(aTopicMetaData, { depth: null, colors: true })}`);

    admin.disconnect();
}

async function deleteTopics(aTopic = []) {
    const admin = kafka.admin();
    admin.connect();

    await admin.deleteTopics({ topics: aTopic });

    admin.disconnect();
}

async function test() {
    /* uncomment to create topic */
    // await createTopics([
    //     {
    //         topic: 'topic_1',
    //     },
    //     {
    //         topic: 'topic_2',
    //     },
    // ]);

    /* uncomment to describe topic */
    // await describeTopics();

    /* uncomment to delete topic */
    // await deleteTopics(['topic_1']);
    
    /* uncomment to list topic */
    // await listTopics();
}

test();
