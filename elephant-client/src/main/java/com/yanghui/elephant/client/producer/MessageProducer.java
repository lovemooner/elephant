package com.yanghui.elephant.client.producer;

import com.yanghui.elephant.client.exception.MQClientException;
import com.yanghui.elephant.client.impl.MessageProducerImpl;
import com.yanghui.elephant.client.pojo.SendResult;
import com.yanghui.elephant.client.pojo.TransactionSendResult;
import com.yanghui.elephant.common.message.Message;
import lombok.Data;

/**
 * 普通消息生产者
 *
 * @author --小灰灰--
 */
@Data
public class MessageProducer implements IProducer {

    /**
     * 消息的最大长度（4M）
     */
    protected int maxMessageSize = 1024 * 1024 * 4;
    /**
     * 生产者分组
     */
    protected String producerGroup;
    /**
     * 发送消息的超时时间
     */
    protected long sendMsgTimeout = 3000;
    /**
     * 发送失败的超时次数
     */
    protected int retryTimesWhenSendFailed = 2;
    /**
     * 注册中心地址
     */
    protected String registerCenter;

    protected MessageProducerImpl producerImpl;

    public MessageProducer() {
        this(null);
    }

    public MessageProducer(String producerGroup) {
        this.producerGroup = producerGroup;
        this.producerImpl = new MessageProducerImpl(this);
    }

    @Override
    public void start() throws MQClientException {
        this.producerImpl.start();
    }

    @Override
    public void shutdown() {
        this.producerImpl.shutdown();
    }

    @Override
    public SendResult send(Message msg) throws MQClientException {
        return this.producerImpl.send(msg);
    }

    @Override
    public TransactionSendResult sendMessageTransaction(Message msg,
                                                        LocalTransactionExecuter excuter, Object arg) throws MQClientException {
        throw new RuntimeException("sendMessageInTransaction not implement, please use TransactionMQProducer class");
    }
}
