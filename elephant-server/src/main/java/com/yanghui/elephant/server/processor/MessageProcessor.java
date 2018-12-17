package com.yanghui.elephant.server.processor;

import com.yanghui.elephant.common.message.MessageType;
import com.yanghui.elephant.common.protocol.header.SendMessageRequestHeader;
import com.yanghui.elephant.mq.producer.ProducerService;
import com.yanghui.elephant.remoting.RequestProcessor;
import com.yanghui.elephant.remoting.procotol.RemotingCommand;
import com.yanghui.elephant.server.MessageService;
import io.netty.channel.ChannelHandlerContext;
import lombok.extern.log4j.Log4j2;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
@Log4j2
public class MessageProcessor implements RequestProcessor {
	
	@Autowired
	private MessageService messageService;

	@Autowired
	private ProducerService producerService;

	@Override
	public RemotingCommand processRequest(ChannelHandlerContext ctx, RemotingCommand request) {
		log.info("处理请求消息：{}",request);
		SendMessageRequestHeader requestHeader = (SendMessageRequestHeader)request.decodeCommandCustomHeader(SendMessageRequestHeader.class);

		switch (MessageType.valueOf(requestHeader.getMessageType())) {
		case NORMAL_MESSAGE:
			return messageService.normalMessageHandle(requestHeader,request); //1 prepare
		case TRANSACTION_PRE_MESSAGE:
			return messageService.transactionMessageHandle(requestHeader,request); //1 prepare transaction msg
		default:
			break;
		}
		return null;
	}


}
