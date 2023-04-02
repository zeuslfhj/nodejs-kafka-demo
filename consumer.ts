import { Consumer, Kafka, logLevel } from 'kafkajs';
import * as process from 'process';
import * as chalk from 'chalk';
import * as cluster from 'cluster';
import * as pm2 from 'pm2';

const isMaster = () => {
    if (cluster.isMaster) {
        return true;
    }

    
    if (process.env && process.env.pm_id) {
        console.log(chalk.yellow(`current pm2id: ${parseInt(process.env.NODE_APP_INSTANCE)}`));
        //Is run with PM2
        if (parseInt(process.env.NODE_APP_INSTANCE) === 0) {
            return true;
        }
    }

    return false;
};

console.log(chalk.red(`current is ${isMaster()}`));

// 只有主线程进行kafka的线程
if (isMaster()) {
    const clustersPid: number[] = [];
    const kafka = new Kafka({
        logLevel: logLevel.ERROR,
        brokers: ['localhost:9092'],
        clientId: 'example-consumer'
    });
    
    const TOPIC = 'zero-test-topic';
    
    const consumer = kafka.consumer({ groupId: 'test-group' });

    // 刷新cluster进程的列表
    const refreshClusters = () => {
        console.log('start refresh clusters');
        pm2.connect((err) => {
            if (err) {
                console.error('connect pm2 list failed', err);
                return;
            }
    
            console.log(chalk.blue('pm2 connect has success'));
            pm2.list((err, data) => {
                console.log(chalk.blue(`pm2 loaded list: ${JSON.stringify(data)}`));
                if (err) {
                    console.error('connect pm2 list failed', err);
                    pm2.disconnect();
                    return;
                }
    
                for (let i = 0; i < data.length; i++) {
                    const processInfo = data[i];
                    const { pm_id } = processInfo;
                    if (pm_id && pm_id !== process.env.pm_id) {
                        clustersPid.push(pm_id);
                    }
                }
                console.log(chalk.blue(`pm2 list result: ${JSON.stringify(clustersPid)}`));

                run().catch(e => console.error(`[example/consumer] ${e.message}`, e));
                pm2.disconnect();
            });
        });
    };
    
    // 监听kafka的消息
    const run = async () => {
        await consumer.connect();
        await consumer.subscribe({
            topic: TOPIC,
            fromBeginning: true
        });

        // 当前index的索引
        let index = 0;
    
        await consumer.run({
            eachMessage: async ({ topic, partition, message }) => {
                const prefix = `${topic}[${partition} | ${message.offset}] / ${message.timestamp}`
                console.log(chalk.green(`- ${prefix} ${message.key}#${message.value}`));

                if (clustersPid.length === 0) {
                    return;
                }

                // 计算当前使用哪个pid
                const curClusterIndex = ++index % clustersPid.length;
                const pid = clustersPid[curClusterIndex];
                const data = {
                    key: message.key.toString(),
                    value: message.value.toString(),
                    constant: '1111'
                };

                console.log(`send data key is ${data.key}, and value is ${data.value}`);
                
                pm2.sendDataToProcessId(pid, {
                    type: 'process:msg',
                    data,
                    id: pid,
                    topic: true
                }, function (err, res) {
                    if (err) {
                        console.log(chalk.red('refresh cluster list because of send data failed'));
                        refreshClusters();
                    }
                });
            },
        });
    };

    refreshClusters();
} else {
    console.log(chalk.yellow(`this is cluster process: ${process.env.pm_id}`));
    process.on('message', function(packet: object) {
        const { data, type } = packet;
        if (!data || type != 'process:msg') {
            console.error(chalk.red('receive data from message failed with empty'));
            return;
        }

        const { key, value, constant } = data;

        console.log(`process ${process.env.pm_id} received content from master: key[${JSON.stringify(data)}]: value[${value}], and constant: ${constant}`);
    });
}

