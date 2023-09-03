import { INT32_SIZE } from '../common/buffer-conf';
import { Message, processMessages } from '../consumer/message-process';
import { onBuffer } from '../consumer/socket-consumer-client';
import { Buffer } from 'node:buffer';
jest.mock('../consumer/message-process.ts');

describe('test client of socket consumer', () => {
    describe('test onBuffer function', () => {

        afterEach(() => {
            (processMessages as jest.Mock).mockClear();
        })

        it('should process single buffer', () => {
            let length = 0;
            const totalBuff = Buffer.alloc(INT32_SIZE);
            let buffArr = [totalBuff];
            const result: Message[] = [];
            new Array(10).fill(0).forEach((_, i) => {
                const key = `customKey${i}`;
                const keyBuff = Buffer.from(key);
                const msg = `hello world robot${i}`;
                const valueBuff = Buffer.from(msg);

                result.push({ key, msg });

                const keyCount = Buffer.alloc(INT32_SIZE);
                keyCount.writeInt32BE(keyBuff.length);

                const valueCount = Buffer.alloc(INT32_SIZE);
                valueCount.writeInt32BE(valueBuff.length);

                buffArr.push(keyCount, keyBuff, valueCount, valueBuff);
                length += keyBuff.length + valueBuff.length + INT32_SIZE * 2;
            });
            
            totalBuff.writeInt32BE(length + INT32_SIZE);

            const targetBuff = Buffer.concat(buffArr);
            
            onBuffer(targetBuff);
            expect(result.length >= 10);
            expect(processMessages).toBeCalledWith(result);
        });

        it('should process split a buffer to multi parts', () => {
            let length = 0;
            const totalBuff = Buffer.alloc(INT32_SIZE);
            let buffArr = [totalBuff];
            const result: Message[] = [];
            new Array(10).forEach((_, i) => {
                const key = `customKey${i}`;
                const keyBuff = Buffer.from(key);
                const msg = `hello world robot${i}`;
                const valueBuff = Buffer.from(msg);

                result.push({ key, msg });

                const keyCount = Buffer.alloc(INT32_SIZE);
                keyCount.writeInt32BE(keyBuff.length);
    
                const valueCount = Buffer.alloc(INT32_SIZE);
                valueCount.writeInt32BE(valueBuff.length);

                buffArr.push(keyCount, keyBuff, valueCount, valueBuff);
                length += keyBuff.length + valueBuff.length + INT32_SIZE * 2;
            });
            
            totalBuff.writeInt32BE(length + INT32_SIZE);

            const targetBuff = Buffer.concat(buffArr);
            let offset = 0;
            let partCount = 3;
            let partLen = Math.ceil(targetBuff.length / partCount);
            
            while(offset < targetBuff.length) {
                onBuffer(targetBuff.subarray(offset, Math.min(offset + partLen, targetBuff.length)));
                offset += partLen;
            }
            
            expect(result.length >= 10);
            expect(processMessages).toBeCalledWith(result);
        });

        it('should process a buffer with two pack', () => {
            // create buffer 1
            const createBuff = (suffix: string) => {
                const key = `customKey${suffix}`;
                const keyBuff = Buffer.from(key);
                const msg = `hello world robot${suffix}`;
                const valueBuff = Buffer.from(msg);
                const keyCount = Buffer.alloc(INT32_SIZE);
                keyCount.writeInt32BE(keyBuff.length);
                const valueCount = Buffer.alloc(INT32_SIZE);
                valueCount.writeInt32BE(valueBuff.length);
                
                const totalBuff = Buffer.alloc(INT32_SIZE);
    
                totalBuff.writeInt32BE(keyBuff.length + valueBuff.length + INT32_SIZE * 3);
                return Buffer.concat([totalBuff, keyCount, keyBuff, valueCount, valueBuff]);
            }

            const firstBuff = createBuff('1');
            const secondBuff = createBuff('2');

            const finalBuff = Buffer.concat([firstBuff, secondBuff]);
            onBuffer(finalBuff);
            expect(processMessages).toBeCalledTimes(2);
            expect(processMessages).toHaveBeenNthCalledWith(1, [{
                key: 'customKey1',
                msg: 'hello world robot1'
            }]);
            expect(processMessages).toHaveBeenNthCalledWith(2, [{
                key: 'customKey2',
                msg: 'hello world robot2'
            }]);
        });

        it('should process empty correct buffer correct', () => {
            const buff = Buffer.alloc(INT32_SIZE);
            buff.writeInt32BE(INT32_SIZE);
            onBuffer(buff);
            expect(processMessages).toBeCalledTimes(1);
            expect(processMessages).toBeCalledWith([]);
        });

        it('should process empty buffer correct', () => {
            const buff = Buffer.alloc(INT32_SIZE);
            buff.writeInt32BE(0);
            onBuffer(buff);
            expect(processMessages).toBeCalledTimes(1);
            expect(processMessages).toBeCalledWith([]);
        });
    });
});