package love;

import com.yanghui.elephant.client.exception.MQClientException;
import com.yanghui.elephant.client.pojo.SendResult;
import com.yanghui.elephant.client.producer.MessageProducer;
import com.yanghui.elephant.common.message.Message;
import org.junit.jupiter.api.Test;
import org.springframework.test.annotation.Rollback;
import org.springframework.transaction.annotation.Transactional;

public class CommonMessageProducer {

    @Test
    @Transactional
    @Rollback(value = false)
    public void test() throws MQClientException {
        MessageProducer producer = new MessageProducer("love/moon/test");
        producer.setRegisterCenter("120.77.152.143:2181");
        producer.start();

        try {
            for(int i=0;i<1;i++){
                Message msg = new Message("queue://yanghui.queue.test1", ("我是消息" + i).getBytes());
                SendResult sendResult = producer.send(msg);
                System.out.println(sendResult);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        producer.shutdown();
    }
}
