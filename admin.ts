import { Consumer, Kafka, logLevel } from 'kafkajs';
import chalk from 'chalk';

const kafka = new Kafka({
    logLevel: logLevel.DEBUG,
    brokers: ['localhost:9092'],
    clientId: 'example-admin'
})
const admin = kafka.admin()

// remember to connect and disconnect when you are done
const run = async () => {
    await admin.connect()
    console.log('before create topic');
    // const topics = await admin.listTopics()
    const hasCreatedTopic = await admin.createTopics({
        validateOnly: false,
        waitForLeaders: true,
        topics: [
            {
                topic: 'zero-test-topic',
                numPartitions: 2
            }
        ],
    });

    console.log('topics is created', hasCreatedTopic);
    await admin.disconnect()
}

run().catch(e => console.error('admin run failed', e));