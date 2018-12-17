package com.yanghui.elephant.server.processor;

import com.yanghui.elephant.server.MessageService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.yanghui.elephant.common.protocol.header.EndTransactionRequestHeader;
import com.yanghui.elephant.remoting.RequestProcessor;
import com.yanghui.elephant.remoting.procotol.RemotingCommand;
import io.netty.channel.ChannelHandlerContext;
import lombok.extern.log4j.Log4j2;

/**
 * 介于 3 confirm 和 3.1 之间
 * local trans结束后调用
 */
@Service
@Log4j2
public class EndTransactionMessageProcessor  implements RequestProcessor {

	@Autowired
	private MessageService messageService;

	@Override
	public RemotingCommand processRequest(ChannelHandlerContext ctx, RemotingCommand request) {
		try {
			EndTransactionRequestHeader header = (EndTransactionRequestHeader)request.decodeCommandCustomHeader(EndTransactionRequestHeader.class);
			messageService.handleTransactionState(header.getMsgId(), header.getCommitOrRollback());
		} catch (Exception e) {
			log.error("EndTransaction message processor exception：{}",e);
		}
		return null;
	}
}
