package com.sjr.msg.pg;

import com.sjr.msg.message.MessageType;
import com.sjr.msg.message.PgOutMessage;
import lombok.extern.slf4j.Slf4j;

import java.nio.ByteBuffer;

/**
 * pgoutput 解析插件
 *
 * @author TMW
 * @date 2020/8/28 17:36
 */
@Slf4j
public class OutPutPlugin {
    private static OutPutPlugin OUT_PUT_PLUGIN;

    public static synchronized OutPutPlugin getInstance() {
        if (OUT_PUT_PLUGIN == null) {
            OUT_PUT_PLUGIN = new OutPutPlugin();
        }
        return OUT_PUT_PLUGIN;
    }

    public PgOutMessage process(ByteBuffer buffer) {
        MessageType messageType = MessageType.forType((char) buffer.get());
        switch (messageType) {
            case BEGIN:
                return handBegin(buffer);
            case RELATION:
                return handRelation(buffer);
            case INSERT:
                return handInsert(buffer);
            case UPDATE:
                return handUpdate(buffer);
            case DELETE:
                return handDelete(buffer);
            case COMMIT:
                return handCommit(buffer);
            default:
                log.warn("Message Type {} skipped, not processed.", messageType);
                break;
        }
    }

}
