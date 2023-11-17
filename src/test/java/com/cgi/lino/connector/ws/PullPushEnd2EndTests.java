// Copyright (C) 2021 CGI France
//
// This file is part of LINO.
//
// LINO is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// LINO is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with LINO.  If not, see <http://www.gnu.org/licenses/>.

package com.cgi.lino.connector.ws;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.IOException;
import java.sql.SQLException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import javax.sql.DataSource;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.SpringBootTest.WebEnvironment;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.web.socket.TextMessage;
import org.springframework.web.socket.WebSocketSession;
import org.springframework.web.socket.client.standard.StandardWebSocketClient;
import org.springframework.web.socket.config.annotation.EnableWebSocket;
import org.springframework.web.socket.messaging.WebSocketStompClient;

import com.cgi.lino.connector.DataUtils;
import com.cgi.lino.connector.controller.ws.dto.CommandMessage;
import com.cgi.lino.connector.controller.ws.dto.constants.Actions;
import com.cgi.lino.connector.controller.ws.dto.payload.PullOpenPayload;
import com.fasterxml.jackson.databind.ObjectMapper;

import lombok.extern.slf4j.Slf4j;

@SpringBootTest(webEnvironment = WebEnvironment.RANDOM_PORT)
@EnableWebSocket
@Slf4j
class PullPushEnd2EndTests {

	@Autowired
	ObjectMapper mapper;

	@LocalServerPort
	private Integer port;

	BlockingQueue<String> blockingQueue;
	WebSocketStompClient stompClient;
	StandardWebSocketClient standardWebSocketClient;

	@BeforeAll
	static void initDatabase(@Autowired DataSource ds) throws SQLException {
		DataUtils.initTestDatabase(ds);
	}

	@AfterAll
	static void cleanDatabase(@Autowired DataSource ds) throws SQLException {
		DataUtils.cleanDatabase(ds);
	}

	@BeforeEach
	void setup(@Autowired DataSource ds)
			throws SQLException, InterruptedException, ExecutionException, TimeoutException {
		DataUtils.resetData(ds);
		this.blockingQueue = new LinkedBlockingDeque<>();
		this.standardWebSocketClient = new StandardWebSocketClient();
	}


	@Test
	void PullPush() {
		
			try {
				
				String cmdId = "myChannel";
				CompletableFuture<WebSocketSession> sessionFuture =standardWebSocketClient.execute(new TestWebSocketHandler(blockingQueue), "ws://localhost:"+this.port+"/connector", "");
  				/* PULL_OPEN*/
				String expected1 = """
					{"id":"myChannel","error":null,"next":true,"payload":{"BLI":"blo","MYSTRING":"isAString","MYAGE":1}}""";
				String expected2 = """
					{"id":"myChannel","error":null,"next":true,"payload":{"BLI":"blio","MYSTRING":"isAString2","MYAGE":42}}""";
				String expected3 = """
					{"id":"myChannel","error":null,"next":false,"payload":null}""";
					
					
				CommandMessage commandMessagePullOpen = new CommandMessage(cmdId, Actions.PULL_OPEN, new PullOpenPayload("PUBLIC", "MYTABLE", null, null));
				String message = mapper.writeValueAsString(commandMessagePullOpen);
				log.info("send {}", commandMessagePullOpen);
				sessionFuture.get(10,TimeUnit.SECONDS).sendMessage(new TextMessage(message.getBytes()));
				assertEquals(expected1, blockingQueue.poll(10, TimeUnit.SECONDS));
				assertEquals(expected2, blockingQueue.poll(10, TimeUnit.SECONDS));
				assertEquals(expected3, blockingQueue.poll(10, TimeUnit.SECONDS));
				sessionFuture.get(10,TimeUnit.SECONDS).close();
			} catch (InterruptedException | ExecutionException   | TimeoutException | IOException e) {
				e.printStackTrace();
				Assertions.fail(e.getMessage());
			}		
	}


	

}
