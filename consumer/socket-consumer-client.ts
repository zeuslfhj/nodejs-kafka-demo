import { INT32_SIZE } from '../common/buffer-conf';
import { createSocketPath } from '../common/socket-path';
import { processMessages } from './message-process';
import net, { Socket } from 'node:net';

let totalLen: number = -1;
let chunksBuffer: Buffer | undefined;

function convertBuffToMessages(buffer: Buffer) {
    let offset = INT32_SIZE;
    const messages = [];

    while (offset < buffer.length) {
        const keyLen = buffer.readInt32BE(offset);
        offset += INT32_SIZE;

        const key = buffer.toString('utf-8', offset, offset + keyLen);
        offset += keyLen;
        
        const msgLen = buffer.readInt32BE(offset);
        offset += INT32_SIZE;
        
        const msg = buffer.toString('utf-8', offset, offset + msgLen);
        offset += msgLen

        messages.push({
            key,
            msg
        });
    }

    processMessages(messages);
}

export function onBuffer(chunk: Buffer){
    // 获取总长度
    if (chunksBuffer && chunksBuffer.length >= 1) {
        chunksBuffer = Buffer.concat([chunksBuffer, chunk]);
    } else {
        chunksBuffer = chunk;
    }
    
    let count = 0;

    // 不断循环，直至当前buffer的长度不满足渲染的需要
    while(
        chunksBuffer && chunksBuffer.length >= INT32_SIZE &&
        (totalLen === -1 || chunksBuffer.length >= totalLen) &&
        count < 5
    ) {
        count++;
        // 获取总长度
        if (totalLen === -1 && chunksBuffer.length >= INT32_SIZE) {
            // total中是把自己算进去的，因此如果小于一个32位的数量，那这个total是不正确的
            // 因此不正确的情况下会使用int32的size来进行写入
            totalLen = Math.max(chunksBuffer.readInt32BE(), INT32_SIZE);
            // console.log('read totallength', totalLen);
        }

        // console.log(`before totalLen: ${totalLen}, content len: ${chunksBuffer.length}, `);
        // chunk长度不够
        if (totalLen > chunksBuffer.length) {
            break;
        }

        const packBuff = chunksBuffer.subarray(0, totalLen);

        // console.log(`before totalLen: ${totalLen}, content len: ${chunksBuffer.length}, `);
        
        if (chunksBuffer.length === totalLen) {
            chunksBuffer = undefined;
        } else {
            chunksBuffer = chunksBuffer.subarray(totalLen, chunksBuffer.length);
            // console.log(`after length: ${chunksBuffer.byteLength}, content is:`, chunksBuffer.toString());
        }
    
        totalLen = -1;
        convertBuffToMessages(packBuff);
    }
}

export function createUnixSocketClient(pid: number): Promise<Socket> {
    return new Promise((resolve, reject) => {
        const client = net.createConnection({ path: createSocketPath(pid) });

        client
            .on("connect", () => {
                resolve(client);
            })
            .on("end", (data: Buffer) => {
                console.log('received data on end', data && data.toString());
            }).on("close", () => {
                console.log('client connect has been ended');
            }).on("error", (e) => {
                reject(e);
                console.log('client connect has error', e);
            });
    });
}