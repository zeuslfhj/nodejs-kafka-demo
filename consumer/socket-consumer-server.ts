import { Message } from 'kafkajs';
import { Buffer } from 'node:buffer';
import * as net from 'net';
import { INT32_SIZE } from '../common/buffer-conf';
import { createSocketPath } from '../common/socket-path';

export type ClusterSocketInfo = {
    pid: string|number;
    socket: net.Socket;
    server: net.Server;
};

/**
 * 构建Unix Socket，并根据传入的pid构建出
 * @param pid 线程ID
 * @returns 线程Socket
 */
export function createUnixSocketServer(pid: number) : Promise<ClusterSocketInfo>{
    return new Promise((resolve, reject) => {
        const socketPath = createSocketPath(pid);
        
        const server = net.createServer((socket: net.Socket) => {
            socket.on("end", () => {
                console.log('master process ended');
            }).on('drain', () => {
                console.log('server socket is drain');
            }).on('error', (e) => {
                console.log('server socket is error');
                reject(e);
            }).on('data', () => {
                console.log('server socket is data');
            }).on('close', () => {
                console.log('server socket is close');
            }).on('timeout', () => {
                console.log('server socket is timeout');
            }).on('connect', () => {
                console.log('server socket is connect');
            });

            resolve({
                pid,
                socket,
                server
            });
        });
    
        try {
            server.listen(socketPath);
        } catch (e) {
            reject(e);
        }
    });
}

/**
 * 构建一个buffer，并写入制定的number类型值
 * @param num number for writing
 * @returns Buffer
 */
function createInt32Buff(num: number) {
    const buff = Buffer.alloc(INT32_SIZE);
    buff.writeInt32BE(num);
    return buff;
}

/**
 * 发送Batch数据到指定的pid上
 * 
 * 流结构设计(+并未真实写入)
 * Int32 + Int32 + data + Int32 + data
 * 总长 + key长 + key的数据包 + value长 + value的数据包
 * @param socketInfo socket information of pid
 * @param batchMessage content for sending
 */
export function sendDataBatch(socketInfo: ClusterSocketInfo, batchMessage: Message[]) {
    let totalLength = INT32_SIZE;
    const lengthBuff = Buffer.alloc(INT32_SIZE);

    // 缓存住所有需要写入的buffer内容，由于数字在写入时会尝试复用对象
    const bufferArray: (Buffer|number|string)[] = [lengthBuff];

    // 将batch message中的所有消息进行过滤，并添加到对应的列表中
    for (const message of batchMessage) {
        const { key, value } = message;

        if (!(key && value)) {
            continue;
        }

        // int32
        const keyLen = Buffer.byteLength(key);
        const valueLen = Buffer.byteLength(value);

        if (keyLen === 0 && valueLen === 0) {
            continue;
        }

        totalLength += INT32_SIZE + keyLen + INT32_SIZE + valueLen;
        bufferArray.push(keyLen, key, valueLen, value);
    }

    lengthBuff.writeInt32BE(totalLength);
    
    const { socket } = socketInfo;
    const totalBuff = Buffer.alloc(totalLength);

    // 将上面过滤出来的内容写入到socket流里
    let offset = 0;
    let byteLen;
    for (const message of bufferArray) {
        if (typeof message === 'number') {
            // int32
            offset = totalBuff.writeInt32BE(message, offset);
        } else if (message instanceof Buffer) {
            byteLen = message.copy(totalBuff, offset);
            offset += byteLen;
        } else {
            byteLen = totalBuff.write(message, offset, 'utf-8');
            offset += byteLen;
        }
    }

    socket.write(totalBuff);
}