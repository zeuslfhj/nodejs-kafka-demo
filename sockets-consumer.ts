import { Consumer, Kafka, KafkaMessage, Message, logLevel } from 'kafkajs';
import process from 'node:process';
import * as chalk from 'chalk';
import cluster from 'node:cluster';
import * as pm2 from 'pm2';
import * as fs from 'fs';
import * as net from 'net';
import * as Buffer from 'buffer';
import { INT32_SIZE } from './common/buffer-conf';
import { onBuffer } from './consumer/socket-consumer-client';
import { ClusterSocketInfo, sendDataBatch } from './consumer/socket-consumer-server';
import { createSocketPath } from './common/socket-path';

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
                        const serverCreator = createUnixSocketServer(pm_id);
                        serverCreators.push(serverCreator);
                        // servers.push(server);
                        // clustersPid.push(pm_id);
                    }
                }
                console.log(chalk.blue(`pm2 list result: ${JSON.stringify(clustersPid)}`));

                Promise.all(serverCreators).then((rets: ClusterSocketInfo[]) => {
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

                // 计算当前使用哪个pid  
                const curClusterIndex = ++index % clustersPid.length;
                const socketInfo: ClusterSocketInfo = servers[curClusterIndex];
                sendDataBatch(socketInfo, batch.messages);
                return heartbeat();
            }
        });
    };

    refreshClusters();
} else {
    console.log(chalk.yellow(`this is cluster process: ${process.env.pm_id}`));
    // for consume each message

    const socketPath = createSocketPath(parseInt(process.env.pm_id!));
    const client = net.createConnection({ path: socketPath });

    client
        .on("connect", () => {
            client.on("data", onBuffer)
        .on("end", (data: Buffer) => {
            console.log('received data on end', data && data.toString());
        }).on("close", () => {
            console.log('client connect has been ended');
        }).on("error", (e) => {
            console.log('client connect has error', e);
        });
    });
}

