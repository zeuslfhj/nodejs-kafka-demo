import { CompressionTypes, Kafka, logLevel } from 'kafkajs';
import chalk from 'chalk';

const kafka = new Kafka({
    logLevel: logLevel.DEBUG,
    brokers: ['localhost:9092'],
    clientId: 'example-producer'
});

const TOPIC = 'zero-test-topic';

let seq = 0;
const getNumber = () => {
    return seq++;
};

// create a producer
const producer = kafka.producer();

// send a message
const sendMessage = () => {
    return producer.send({
        topic: TOPIC,
        compression: CompressionTypes.GZIP,
        messages: [{
            key: `${TOPIC}-message`,
            value: `currentSeq: ${getNumber()}`
        }]
    }).then(console.log, console.error);
};

const run = async () => {
    try {
        await producer.connect();
    } catch (e) {
        console.error(chalk.red('connect producer failed with error', e));
    }
    setInterval(sendMessage, 3000);
};

run();