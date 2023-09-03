import { INT32_SIZE } from '../common/buffer-conf';

export type Message = {
    key: string;
    msg: string;
};

export function processMessages(messages: Message[]) {
    console.log('messages', messages);
}
