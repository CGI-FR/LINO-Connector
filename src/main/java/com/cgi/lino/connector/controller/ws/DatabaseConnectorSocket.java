package com.cgi.lino.connector.controller.ws;

import java.io.IOException;
import java.sql.SQLException;
import java.text.ParseException;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Stream;

import org.springframework.stereotype.Service;
import org.springframework.web.socket.CloseStatus;
import org.springframework.web.socket.TextMessage;
import org.springframework.web.socket.WebSocketSession;
import org.springframework.web.socket.handler.TextWebSocketHandler;

import com.cgi.lino.connector.controller.exceptions.RemoteException;
import com.cgi.lino.connector.controller.service.InfoDatabaseService;
import com.cgi.lino.connector.controller.service.PullDatabaseService;
import com.cgi.lino.connector.controller.service.ws.PushDataService;
import com.cgi.lino.connector.controller.ws.dto.CommandMessage;
import com.cgi.lino.connector.controller.ws.dto.ResultMessage;
import com.cgi.lino.connector.controller.ws.dto.constants.Actions;
import com.cgi.lino.connector.controller.ws.dto.payload.DBPayload;
import com.cgi.lino.connector.controller.ws.dto.payload.PullOpenPayload;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import static com.cgi.lino.connector.controller.ws.dto.Messages.*;

@Service
@AllArgsConstructor
@Slf4j
public class DatabaseConnectorSocket extends TextWebSocketHandler {

	private InfoDatabaseService infoDatabaseService;
	private PullDatabaseService pullDatabaseService;
	private PushDataService pushDataService;
	private ObjectMapper mapper;

	@Override
	public boolean supportsPartialMessages() {
		return false;
	}



	@Override
	public void handleTextMessage(WebSocketSession session, TextMessage message) {
		CompletableFuture.runAsync(() -> {
			log.info(INFO_MESSAGE, "20003",
					"message received from %s : %s - isOpened: %s".formatted(session.getId(), message.getPayload(),
							session.isOpen()));
			if (!message.getPayload().startsWith("CONNECT")) {
				CommandMessage cmdMessage = null;
				try {
					cmdMessage = mapper.readValue(message.getPayload(), CommandMessage.class);
					this.messageIncoming(session, cmdMessage);
				} catch (RemoteException e) {
					e.printStackTrace();
					log.error(ERROR_MESSAGE, "40003", e.getCausalityId(), "error from %s with id: %s. Cause is %s"
							.formatted(session.getId(), e.getCommandId(), e.getMessage()),e);
					this.sendError(session, e.getCommandId(),
							ERROR_MESSAGE_F.formatted("40003", e.getCausalityId(), e.getMessage()));
				} catch (JsonProcessingException e) {
					String causalityId=  UUID.randomUUID().toString();
					String errorMessage = "message received %s from %s has not been parsed, due to syntax error. Cause : %s)"
							.formatted(this.escape(message.getPayload()), session.getId(), escape(e.getMessage()));
					log.error(ERROR_MESSAGE, "40004", causalityId, errorMessage,e);
					this.sendError(session, "",
							ERROR_MESSAGE_F.formatted("40004", causalityId, errorMessage));
				} catch (Throwable e) {
					String causalityId=  UUID.randomUUID().toString();
					log.error(ERROR_MESSAGE, "40002",causalityId,
							"message received from %s with id: %s has occured an error %s)".formatted(session.getId(),
									cmdMessage.getId(), e.getMessage()),e);
					this.sendError(session, cmdMessage.getId(),
							ERROR_MESSAGE_F.formatted("40002", causalityId, e.getMessage()));
				}
			}
		});
	}

	@Override
	public void afterConnectionClosed(WebSocketSession session, CloseStatus closeStatus) {
		// your code here
	}

	@Override
	public void handleTransportError(WebSocketSession session, Throwable exception) {
		// your code here
	}

	private String escape(String message) {
		return message.replace("\"", "'");
	}

	public void messageIncoming(WebSocketSession session, CommandMessage cmdMessage) {
		switch (cmdMessage.getAction()) {
			case Actions.PING ->
				pushUniResult(session, cmdMessage, infoDatabaseService.getDatabaseInformation(cmdMessage.getId()));
			case Actions.SCHEMA ->
				pushUniResult(session, cmdMessage, infoDatabaseService.getSchemas(cmdMessage.getId()));
			case Actions.TABLES ->
				pushUniResult(session, cmdMessage,
						infoDatabaseService.getTables(cmdMessage.getId(), getDBPayload(cmdMessage).getSchema()));
			case Actions.RELATIONS ->
				pushUniResult(session, cmdMessage,
						infoDatabaseService.getRelations(cmdMessage.getId(), getDBPayload(cmdMessage).getSchema()));
			case Actions.PULL_OPEN -> pullOpen(session, cmdMessage);
			case Actions.PUSH_OPEN, Actions.PUSH_DATA, Actions.PUSH_COMMIT, Actions.PUSH_CLOSE ->
				pushUniResult(session, cmdMessage, this.pushDataService.pushAction(session.getId(), cmdMessage));
			default ->
				sendError(session, cmdMessage.getId(),
						"the action %s is not supported".formatted(cmdMessage.getAction()));
		}
	}

	private void pullOpen(WebSocketSession session, CommandMessage cmdMessage) {
		
		try {
			Stream<ObjectNode> stream = pullDatabaseService.pullData(cmdMessage.getId(),
					(PullOpenPayload) cmdMessage.getPayload());
			stream.forEach(item -> {
				this.sendResultMessage(session, cmdMessage.getId(), true, item);
			});
			this.sendResultMessage(session, cmdMessage.getId(), false, null);
		} catch (Exception e) {
			e.printStackTrace();
			throw new RemoteException(cmdMessage.getId(), e);
		}
		

	}

	private DBPayload getDBPayload(CommandMessage cmdMessage) {
		if (cmdMessage.getPayload() instanceof DBPayload) {
			return (DBPayload) cmdMessage.getPayload();
		} else {
			throw new ClassCastException(
					"The payload of the commandMessage %s can not be cast un DBPayload".formatted(cmdMessage.getId()));
		}
	}

	public void pushUniResult(WebSocketSession session, CommandMessage message, JsonNode result) {
		try {
			String ret = mapper.writeValueAsString(new ResultMessage(message.getId(), null, false, result));
			log.info(INFO_MESSAGE, "20043", "response for %s is %s".formatted(message.getId(), ret));
			session.sendMessage(new TextMessage(ret));
		} catch (IOException e) {
			throw new RemoteException(message.getId(), e);
		}
	}

	void sendError(WebSocketSession session, String id, String errorMessage) {
		try {
			ObjectNode errorPayload = mapper.createObjectNode();
			errorPayload.put("cause", errorMessage);
			String ret = mapper.writeValueAsString(new ResultMessage(id, "Error", false, errorPayload));
			session.sendMessage(new TextMessage(ret));
		} catch (IOException e) {
			throw new RemoteException(id, e);
		}
	}

	void sendResultMessage(WebSocketSession session, String id, boolean next, JsonNode payload) {
		try {
			String ret = mapper.writeValueAsString(new ResultMessage(id, null, next, payload));
			log.debug(DEBUG_MESSAGE, "10040", "response of %s for %s is %s".formatted(session.getId(), id, ret));
			session.sendMessage(new TextMessage(ret, true));

		} catch (IOException e) {
			throw new RemoteException(id, e);
		}
	}

	public void close(WebSocketSession session) {
		pushDataService.close(session.getId());
	}

}