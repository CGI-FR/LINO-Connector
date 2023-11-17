package com.cgi.lino.connector.ws;

import java.util.concurrent.BlockingQueue;

import org.springframework.web.socket.TextMessage;
import org.springframework.web.socket.WebSocketSession;
import org.springframework.web.socket.handler.TextWebSocketHandler;

import lombok.extern.slf4j.Slf4j;
@Slf4j
public class TestWebSocketHandler extends TextWebSocketHandler {

		private BlockingQueue<String> blockingQueue;

		TestWebSocketHandler(BlockingQueue<String> blockingQueue) {
			this.blockingQueue = blockingQueue;
		}

		@Override
		public void handleTextMessage(WebSocketSession session, TextMessage message) throws Exception {
			log.info("From {} - Reception: {}", session.getId(), message.getPayload());
			this.blockingQueue.add(message.getPayload());
		}

		
}
