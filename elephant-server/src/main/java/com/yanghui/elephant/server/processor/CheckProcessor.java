package com.yanghui.elephant.server.processor;

import com.yanghui.elephant.server.MessageService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.yanghui.elephant.common.protocol.header.CheckTransactionStateResponseHeader;
import com.yanghui.elephant.remoting.RequestProcessor;
import com.yanghui.elephant.remoting.procotol.RemotingCommand;

import io.netty.channel.ChannelHandlerContext;
import lombok.extern.log4j.Log4j2;
@Service
@Log4j2
public class CheckProcessor   implements RequestProcessor {

	@Autowired
	private MessageService messageService;

	@Override
	public RemotingCommand processRequest(ChannelHandlerContext ctx, RemotingCommand request) {
		log.info("处理回查事务消息状态回应消息：{}",request);
		try {
			CheckTransactionStateResponseHeader header = (CheckTransactionStateResponseHeader)request.decodeCommandCustomHeader(
					CheckTransactionStateResponseHeader.class);
			messageService.handleTransactionState(header.getMessageId(), header.getCommitOrRollback());
		} catch (Exception e) {
			log.error("Check Transaction State Response processor exception：{}",e);
		}
		return null;
	}
}
