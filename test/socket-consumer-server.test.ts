import { INT32_SIZE } from '../common/buffer-conf';
import { createSocketPath } from '../common/socket-path';
import { createUnixSocketClient } from '../consumer/socket-consumer-client';
import { ClusterSocketInfo, createUnixSocketServer, sendDataBatch } from '../consumer/socket-consumer-server';
import { Server, Socket } from 'node:net';

describe('test write socket', () => {
    describe('test writing socket', () => {
        let socket: ClusterSocketInfo;
        let client: Socket;
        const port = Math.round(Math.random() * 1000);
        let socketClosePromise: Promise<void>;
        let serverClosePromise: Promise<void>;
        let clientClosePromise: Promise<void>;

        function createSocketClosePromise(socket: Socket|Server): Promise<void> {
            return new Promise<void>((resolve, reject) => {
                socket.on('close', () => resolve()).on('error', e => reject(e));
            });
        }
        
        beforeEach(() => {
            return Promise.all([
                createUnixSocketServer(port),
                createUnixSocketClient(port)
            ]).then(([socketRet, clientRet]) => {
                socket = socketRet;
                client = clientRet;

                socketClosePromise = createSocketClosePromise(socket.socket);
                serverClosePromise = createSocketClosePromise(socket.server);
                clientClosePromise = createSocketClosePromise(client);
            }, (e) => {
                console.error('before all ended by error', e);
            });
        });

        afterEach(() => {
            if (socket) {
                if (socket.socket.destroyed) {
                    socket.socket.end();
                    socket.socket.destroy();
                }

                if (socket.server) {
                    socket.server.close();
                }
            }

            if (client && !client.destroyed) {
                client.destroy();
            }

            return Promise.all([socketClosePromise, clientClosePromise, serverClosePromise]).then(() => {
                console.log('server and client has been ended');
            }, (e) => {
                console.error('close server and client with error', e);
            });
        });
        
        it('test write buffer by socket', (done) => {
            const chunks: Buffer[] = [];
            const clientListener = (chunk: Buffer) => {
                chunks.push(chunk);
            };

            const keyBuff = Buffer.from('customKey1');
            const valueBuff = Buffer.from('hello batch');
            const messages = [
                { key: keyBuff, value: valueBuff }
            ];

            client
                .on('data', clientListener)
                .on('end', () => {
                    try {
                        const finalBuffer = Buffer.concat(chunks);
                        const total = keyBuff.length + valueBuff.length + INT32_SIZE * 3;
                        
                        expect(finalBuffer.length).toBe(total);
                        expect(finalBuffer.readInt32BE()).toBe(total);
                        
                        const keyLen = finalBuffer.readInt32BE(INT32_SIZE);
                        expect(keyLen).toBe(keyBuff.length);
                        let offset = 2 * INT32_SIZE;
    
                        expect(finalBuffer.toString('utf-8', offset, offset + keyBuff.length)).toBe('customKey1');
                        offset += keyBuff.length;
                        
                        expect(finalBuffer.readInt32BE(offset)).toBe(valueBuff.length);
                        offset += INT32_SIZE;
    
                        expect(finalBuffer.toString('utf-8', offset, offset + valueBuff.length)).toBe('hello batch');
                        client.off('data', clientListener);
                        done();
                    } catch (error) {
                        done(error);
                    }
                });

            sendDataBatch(socket, messages);
            socket.socket.end();
        });

        function collectChunk(client: Socket): Promise<Buffer> {
            const chunks: Buffer[] = [];
            return new Promise((resolve, reject) => {
                client.on('data', (chunk: Buffer) => {
                    chunks.push(chunk);
                }).on('end', () => {
                    resolve(Buffer.concat(chunks));
                }).on('error', e => reject(e));
            });
        }

        /**
         * 测试socket.write进行编写的时候，如果将buffer的值进行覆盖，是否会影响最终的结果
         * 
         * 结论是会影响，所以buffer不能被重用
         */
        it('test override buffer after invoke socket.write', () => {
            const result = collectChunk(client).then((buffer: Buffer) => {
                expect(buffer.toString().indexOf('0')).toBeGreaterThan(-1);
            }, (e) => {
                expect(e).toBe(undefined);
            });

            const randomStr = new Array(50).fill(0).map(() => Math.round(Math.random() * 10)).join(';');
            const buffer = Buffer.from(randomStr);
            socket.socket.write(buffer);
            buffer.fill(0);
            socket.socket.end();
            return result;
        });
    });
});