package love.moon.test;

import com.yanghui.elephant.client.exception.MQClientException;
import com.yanghui.elephant.client.impl.MessageProducerImpl;
import com.yanghui.elephant.client.pojo.SendResult;
import com.yanghui.elephant.client.pojo.TransactionSendResult;
import com.yanghui.elephant.client.producer.LocalTransactionExecuter;
import com.yanghui.elephant.common.constant.LocalTransactionState;
import com.yanghui.elephant.common.message.Message;
import com.yanghui.elephant.remoting.exception.RemotingConnectException;
import com.yanghui.elephant.remoting.exception.RemotingSendRequestException;
import com.yanghui.elephant.remoting.exception.RemotingTimeoutException;
import lombok.extern.log4j.Log4j2;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.annotation.Rollback;
import org.springframework.transaction.annotation.Transactional;

@Log4j2
public class ApplicationTest {

    @Autowired
    private MessageProducerImpl producer;

//    @Test
//    @Transactional
//    @Rollback(value = false)
//    public void applyAppTest() throws MQClientException {
//
//    }


    @Test
    public void sendTransactionMsg() throws MQClientException, InterruptedException, RemotingTimeoutException, RemotingSendRequestException, RemotingConnectException {
        TransactionSendResult transactionSendResult = new TransactionSendResult();
        Message msg = new Message("queue://yanghui.queue.test1", ("我是事务消息").getBytes());
        //step 1 : prepare
        SendResult result = producer.send(msg);
        //step 2 execute local trans
        Throwable localException = null;
        LocalTransactionState localState=null;
        try{
             localState = executeLocalTrans(result,msg);
        }catch (Exception e){
            log.error(e.getMessage(),e);
            localException=e;
        }
      //3 confirm
        producer.confirm(result, localState, localException);
        //Assert
        transactionSendResult.setMsgId(result.getMsgId());
        transactionSendResult.setSendStatus(result.getSendStatus());
        transactionSendResult.setLocalTransactionState(localState);
    }


    private LocalTransactionState executeLocalTrans(SendResult result,Message msg){
        LocalTransactionState localState = LocalTransactionState.UNKNOW;
        switch (result.getSendStatus()) {
            case SEND_OK:
                LocalTransactionExecuter executer = new LocalTransactionExecuter() {
                    @Override
                    public LocalTransactionState executeLocalTransactionBranch(Message msg, Object arg) {
                        return LocalTransactionState.COMMIT_MESSAGE;
                    }
                };
                localState = executer.executeLocalTransactionBranch(msg, null);
                break;
            default:
                localState = LocalTransactionState.ROLLBACK_MESSAGE;
                break;
        }
        return localState;
    }
}
