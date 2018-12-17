package com.yanghui.elephant.server;

import java.util.Date;
import java.util.Map;

import com.yanghui.elephant.common.constant.ResponseCode;
import com.yanghui.elephant.common.protocol.header.SendMessageRequestHeader;
import com.yanghui.elephant.remoting.procotol.RemotingCommand;
import com.yanghui.elephant.store.mapper.MessageEntityMapper;
import lombok.extern.log4j.Log4j2;
import org.springframework.beans.factory.annotation.Autowired;

import com.alibaba.fastjson.JSON;
import com.yanghui.elephant.common.constant.LocalTransactionState;
import com.yanghui.elephant.common.constant.MessageStatus;
import com.yanghui.elephant.common.constant.SendStatus;
import com.yanghui.elephant.common.message.Message;
import com.yanghui.elephant.common.utils.StringUtil;
import com.yanghui.elephant.mq.producer.ProducerService;
import com.yanghui.elephant.store.entity.MessageEntity;
import com.yanghui.elephant.store.manager.MessageEntityManager;
@Log4j2
public  class MessageService {
	
	@Autowired
	protected MessageEntityManager messageEntityManager;
	@Autowired
	protected ProducerService producerService;
	@Autowired
	private MessageEntityMapper messageEntityMapper;

	@SuppressWarnings("unchecked")
	public void handleTransactionState(String messageId,String commitOrRollback) {
		LocalTransactionState localState = LocalTransactionState.valueOfName(commitOrRollback);
		
		MessageEntity findMessageEntity = this.messageEntityManager.findByMessageId(messageId);
		//
		boolean sendMq = false;
		switch (localState) {
		case COMMIT_MESSAGE:
			this.messageEntityManager.updateStatusByMessageId(MessageStatus.CONFIRMED.getStatus(),findMessageEntity.getMessageId());
			sendMq = true;
			break;
		case ROLLBACK_MESSAGE:
			this.messageEntityManager.updateStatusByMessageId(MessageStatus.ROLLBACK.getStatus(),findMessageEntity.getMessageId());
			break;
		default:
			break;
		}
		//4.1 push mq
		if(sendMq) {
			Message message = new Message();
			message.setBody(findMessageEntity.getBody());
			message.setDestination(findMessageEntity.getDestination());
			message.setMessageId(findMessageEntity.getMessageId());
			if(!StringUtil.isEmpty(findMessageEntity.getProperties())) {
				message.setProperties((Map<String, String>)JSON.parseObject(findMessageEntity.getProperties(),Map.class));
			}
			this.producerService.sendMessage(message);
			this.messageEntityManager.updateSendStatusByMessageId(SendStatus.ALREADY_SEND.getStatus(),
					findMessageEntity.getMessageId());
		}
	}


	@SuppressWarnings("unchecked")
	public RemotingCommand normalMessageHandle(SendMessageRequestHeader requestHeader, RemotingCommand request) {

		RemotingCommand response = RemotingCommand.buildResposeCmd(ResponseCode.SERVER_FAIL, request.getUnique());

		MessageEntity entity = buildMessageEntity(request.getBody(), requestHeader, false);

		int responseCode = saveMessage(entity);
		if(responseCode != ResponseCode.SUCCESS) {
			response.setCode(responseCode);
			return response;
		}

		Message message = new Message();
		message.setBody(request.getBody());
		message.setDestination(requestHeader.getDestination());
		message.setMessageId(requestHeader.getMessageId());
		if(!StringUtil.isEmpty(requestHeader.getProperties())) {
			message.setProperties((Map<String, String>)JSON.parseObject(requestHeader.getProperties(),Map.class));
		}
		try {
			this.producerService.sendMessage(message);
		} catch (Exception e) {
			log.error("send mq exception：{}",e);
			response.setCode(ResponseCode.SEND_MQ_FAIL);
			return response;
		}

		response.setCode(ResponseCode.SUCCESS);

		try {
			this.messageEntityManager.updateSendStatusByMessageId(SendStatus.ALREADY_SEND.getStatus(),
					requestHeader.getMessageId());
		} catch (Exception e) {
			log.error("update message status exception：{}",e);
		}
		return response;
	}

	/**
	 * 处理 1 prepare 的请求
	 * @param requestHeader
	 * @param request
	 * @return
	 */
	public RemotingCommand transactionMessageHandle(SendMessageRequestHeader requestHeader, RemotingCommand request) {
		RemotingCommand response = RemotingCommand.buildResposeCmd(ResponseCode.SERVER_FAIL, request.getUnique());
		MessageEntity entity = buildMessageEntity(request.getBody(), requestHeader, true);
		int responseCode = saveMessage(entity);
		response.setCode(responseCode);
		return response;
	}

	private MessageEntity buildMessageEntity(byte[] body,SendMessageRequestHeader requestHeader,boolean isTransaction) {
		MessageEntity entity = new MessageEntity();
		entity.setBody(body);
		entity.setCreateTime(new Date());
		entity.setDestination(requestHeader.getDestination());
		entity.setGroup(requestHeader.getProducerGroup());
		entity.setMessageId(requestHeader.getMessageId());
		entity.setProperties(requestHeader.getProperties());
		entity.setSendStatus(SendStatus.WAIT_SEND.getStatus());
		entity.setUpdateTime(entity.getCreateTime());

		if(isTransaction) {
			entity.setTransaction(true);
			entity.setStatus(MessageStatus.CONFIRMING.getStatus());
		}else {
			entity.setTransaction(false);
			entity.setStatus(MessageStatus.CONFIRMED.getStatus());
		}
		return entity;
	}

	private int saveMessage(MessageEntity entity) {
		try {
			MessageEntity find = this.messageEntityManager.findByMessageId(entity.getMessageId());
			if(find == null) {
				this.messageEntityMapper.insert(entity);
			}
			return ResponseCode.SUCCESS;
		} catch (Exception e) {
			log.error("save message exception：{}",e);
			return ResponseCode.FUSH_DB_FAIL;
		}
	}

}
