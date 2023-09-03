import { Consumer, Kafka, KafkaMessage, Message, logLevel } from 'kafkajs';
import * as process from 'node:process';
import * as chalk from 'chalk';
import * as cluster from 'node:cluster';
import * as pm2 from 'pm2';
import { Socket } from 'node:net';
import { createUnixSocketClient, onBuffer } from './consumer/socket-consumer-client';
import { ClusterSocketInfo, createUnixSocketServer, sendDataBatch } from './consumer/socket-consumer-server';

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
    const servers: { [key: string]: ClusterSocketInfo } = {};
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

                const serverCreators: Promise<ClusterSocketInfo>[] = [];
    
                for (let i = 0; i < data.length; i++) {
                    const processInfo = data[i];
                    const { pm_id } = processInfo;
                    if (pm_id && pm_id !== process.env.pm_id) {
                        const socketInfoPromise = createUnixSocketServer(pm_id);
                        serverCreators.push(socketInfoPromise);
                        // servers.push(server);
                        clustersPid.push(pm_id);
                    }
                }
                console.log(chalk.blue(`pm2 list result: ${JSON.stringify(clustersPid)}`));

                Promise.all(serverCreators).then((rets: ClusterSocketInfo[]) => {
                    console.log('pm2 started all socket server');
                    for (const ret of rets) {
                        const { pid } = ret;
                        servers[pid] = ret;
                    }

                    run().catch(e => console.error(`[example/consumer] ${e.message}`, e));
                });

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
            eachBatch: async ({
                batch,
                resolveOffset,
                heartbeat,
                commitOffsetsIfNecessary,
                uncommittedOffsets,
                isRunning,
                isStale,
                pause,
            }) => {
                const prefix = `${batch.topic}[${batch.partition} | ${batch.firstOffset()} | ${batch.lastOffset()}]`
                console.log(chalk.green(`- ${prefix}`));

                if (clustersPid.length === 0) {
                    return;
                }

                // 计算当前使用哪个pid, 由于0是主进程，子进程是从1开始的
                const curClusterIndex = (++index % clustersPid.length);
                const pid = clustersPid[curClusterIndex];
                console.log('current cluster index', pid);
                const socketInfo: ClusterSocketInfo = servers[pid];
                if (socketInfo) {
                    sendDataBatch(socketInfo, batch.messages);
                }
                await heartbeat();
            }
        });
    };

    refreshClusters();
} else {
    console.log(chalk.yellow(`this is cluster process: ${process.env.pm_id}`));
    // FIXME: 这里需要等主进构建完socket server的文件以后才可以开始建立连接
    // 因此使用setTimeout进行一定时间的等待，这个实现有点粗糙
    // 可以使用process来进行消息传递，等收到主进程的消息以后再开始建立连接
    setTimeout(() => {
        createUnixSocketClient(parseInt(process.env.pm_id!)).then((client: Socket) => {
            client.on("data", onBuffer);
        });
    }, 1000);
}

