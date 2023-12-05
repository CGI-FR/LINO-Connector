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
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import javax.sql.DataSource;

import org.json.JSONArray;
import org.junit.Ignore;
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
import com.cgi.lino.connector.controller.ws.dto.PushMode;
import com.cgi.lino.connector.controller.ws.dto.ResultMessage;
import com.cgi.lino.connector.controller.ws.dto.constants.Actions;
import com.cgi.lino.connector.controller.ws.dto.payload.PullOpenPayload;
import com.cgi.lino.connector.controller.ws.dto.payload.PushDataPayload;
import com.cgi.lino.connector.controller.ws.dto.payload.PushOpenPayload;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.TextNode;
import com.fasterxml.jackson.databind.node.ValueNode;

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

	DataSource datasource;

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
		this.datasource = ds;
		DataUtils.resetData(ds);
		this.blockingQueue = new LinkedBlockingDeque<>();
		this.standardWebSocketClient = new StandardWebSocketClient();
	}

	@Test
	void Pull() {

		try {

			String cmdId = "myChannel";
			CompletableFuture<WebSocketSession> sessionFuture = standardWebSocketClient
					.execute(new TestWebSocketHandler(blockingQueue), "ws://localhost:" + this.port + "/connector", "");
			/* PULL_OPEN */
			String expected1 = """
					{"id":"myChannel","error":null,"next":true,"payload":{"BLI":"blo","MYSTRING":"isAString","MYAGE":1}}""";
			String expected2 = """
					{"id":"myChannel","error":null,"next":true,"payload":{"BLI":"blio","MYSTRING":"isAString2","MYAGE":42}}""";
			String expected3 = """
					{"id":"myChannel","error":null,"next":false,"payload":null}""";

			CommandMessage commandMessagePullOpen = new CommandMessage(cmdId, Actions.PULL_OPEN,
					new PullOpenPayload("PUBLIC", "MYTABLE", null, null));
			String message = mapper.writeValueAsString(commandMessagePullOpen);
			log.info("send {}", commandMessagePullOpen);
			sessionFuture.get(10, TimeUnit.SECONDS).sendMessage(new TextMessage(message.getBytes()));
			assertEquals(expected1, blockingQueue.poll(10, TimeUnit.SECONDS));
			assertEquals(expected2, blockingQueue.poll(10, TimeUnit.SECONDS));
			assertEquals(expected3, blockingQueue.poll(10, TimeUnit.SECONDS));
			sessionFuture.get(10, TimeUnit.SECONDS).close();
		} catch (InterruptedException | ExecutionException | TimeoutException | IOException e) {
			e.printStackTrace();
			Assertions.fail(e.getMessage());
		}
	}

	@Test
	void Push() {

		try {

			String cmdId = "myChannel";
			CompletableFuture<WebSocketSession> sessionFuture = standardWebSocketClient
					.execute(new TestWebSocketHandler(blockingQueue), "ws://localhost:" + this.port + "/connector", "");

			// open push
			String expected1 = """
					{"id":"myChannel","error":null,"next":false,"payload":{"ack":"myChannel","message":"Push open: done with disableConstraints=true and mode=INSERT"}}""";
			CommandMessage commandMessagePushOpen = new CommandMessage(cmdId, Actions.PUSH_OPEN,
					new PushOpenPayload("PUBLIC", Arrays.asList("MYTABLE"), PushMode.INSERT, true));
			String message = mapper.writeValueAsString(commandMessagePushOpen);
			log.info("send {}", commandMessagePushOpen);
			sessionFuture.get(10, TimeUnit.SECONDS).sendMessage(new TextMessage(message.getBytes()));
			assertEquals(expected1, blockingQueue.poll(10, TimeUnit.SECONDS));

			// push value
			String expected2 = """
					{"id":"myChannel","error":null,"next":false,"payload":{"ack":"myChannel","message":"Push date sql response: 1"}}""";

			Map<String, ValueNode> data = new HashMap<>();
			data.put("BLI", JsonNodeFactory.instance.textNode("BLOT"));
			data.put("MYSTRING", JsonNodeFactory.instance.textNode("is not a blank string"));
			data.put("MYAGE", JsonNodeFactory.instance.numberNode(43));
			CommandMessage commandMessagePush = new CommandMessage(cmdId, Actions.PUSH_DATA,
					new PushDataPayload("PUBLIC", "MYTABLE", data, null));
			message = mapper.writeValueAsString(commandMessagePush);
			log.info("send {}", commandMessagePush);
			sessionFuture.get(10, TimeUnit.SECONDS).sendMessage(new TextMessage(message.getBytes()));
			assertEquals(expected2, blockingQueue.poll(10, TimeUnit.SECONDS));

			// push commit
			String expected3 = """
					{"id":"myChannel","error":null,"next":false,"payload":{"ack":"myChannel","message":"Push commit: done"}}""";

			CommandMessage commandMessageCommit = new CommandMessage(cmdId, Actions.PUSH_COMMIT, null);
			message = mapper.writeValueAsString(commandMessageCommit);
			log.info("send {}", commandMessageCommit);
			sessionFuture.get(10, TimeUnit.SECONDS).sendMessage(new TextMessage(message.getBytes()));
			assertEquals(expected3, blockingQueue.poll(10, TimeUnit.SECONDS));

			// push close
			String expected4 = """
					{"id":"myChannel","error":null,"next":false,"payload":{"ack":"myChannel","message":"Push close: done with enableConstraints=true"}}""";

			CommandMessage commandMessagePushClose = new CommandMessage(cmdId, Actions.PUSH_CLOSE, null);
			message = mapper.writeValueAsString(commandMessagePushClose);
			log.info("send {}", commandMessagePushClose);
			sessionFuture.get(10, TimeUnit.SECONDS).sendMessage(new TextMessage(message.getBytes()));
			assertEquals(expected4, blockingQueue.poll(10, TimeUnit.SECONDS));
			sessionFuture.get(10, TimeUnit.SECONDS).close();

			JSONArray result = DataUtils.getData(datasource, "PUBLIC", "MYTABLE");

			// check result
			String expected5 = """
					[{"MYSTRING":"isAString","MYAGE":1,"BLI":"blo"},{"MYSTRING":"isAString2","MYAGE":42,"BLI":"blio"},{"MYSTRING":"is not a blank string","MYAGE":43,"BLI":"BLOT"}]""";
			Assertions.assertEquals(expected5, result.toString());

		} catch (InterruptedException | ExecutionException | TimeoutException | IOException | SQLException e) {
			e.printStackTrace();
			Assertions.fail(e.getMessage());
		}
	}

/*	@Test
	@Ignore // problem with lock on table
	void PullUpadte() throws IOException, InterruptedException, ExecutionException, TimeoutException, SQLException {
		// ws connection clients
		String cmdIdPull = "pull";
		CompletableFuture<WebSocketSession> pullSessionFuture = standardWebSocketClient
				.execute(new TestWebSocketHandler(blockingQueue), "ws://localhost:" + this.port + "/connector", "");

		String cmdIdPush = "push";
		CompletableFuture<WebSocketSession> pushSessionFuture = standardWebSocketClient
				.execute(new TestWebSocketHandler(blockingQueue), "ws://localhost:" + this.port + "/connector", "");

		// open push
		String expected1 = """
				{"id":"push","error":null,"next":false,"payload":{"ack":"push","message":"Push open: done with disableConstraints=true and mode=UPDATE"}}""";
		CommandMessage commandMessagePushOpen = new CommandMessage(cmdIdPush, Actions.PUSH_OPEN,
				new PushOpenPayload("PUBLIC", Arrays.asList("MYTABLE"), PushMode.UPDATE, true));
		String message = mapper.writeValueAsString(commandMessagePushOpen);
		log.info("send {}", commandMessagePushOpen);
		pushSessionFuture.get(10, TimeUnit.SECONDS).sendMessage(new TextMessage(message.getBytes()));
		assertEquals(expected1, blockingQueue.poll(10, TimeUnit.SECONDS));

		// pull open
		CommandMessage commandMessagePullOpen = new CommandMessage(cmdIdPull, Actions.PULL_OPEN,
				new PullOpenPayload("PUBLIC", "MYTABLE", null, null));
		message = mapper.writeValueAsString(commandMessagePullOpen);
		log.info("send {}", commandMessagePullOpen);
		pullSessionFuture.get(10, TimeUnit.SECONDS).sendMessage(new TextMessage(message.getBytes()));

		for (int i = 0; i < 2; i++) {
			String value1 = blockingQueue.poll(10, TimeUnit.SECONDS);
			ResultMessage valueObject1 = mapper.readValue(value1, ResultMessage.class);

			// update
			Map<String, ValueNode> data = new HashMap<>();
			data.put("MYAGE", JsonNodeFactory.instance.numberNode(666));
			Map<String, ValueNode> condition = new HashMap<>();
			condition.put("BLI",new TextNode(valueObject1.getPayload().get("BLI").asText()));

			CommandMessage commandMessagePush = new CommandMessage(cmdIdPush, Actions.PUSH_DATA,
					new PushDataPayload("PUBLIC", "MYTABLE", data, condition));
			message = mapper.writeValueAsString(commandMessagePush);
			log.info("send {}", commandMessagePush);
			pushSessionFuture.get(10, TimeUnit.SECONDS).sendMessage(new TextMessage(message.getBytes()));
		}

	// push commit
			String expected3 = """
					{"id":"push","error":null,"next":false,"payload":{"ack":"push","message":"Push commit: done"}}""";

			CommandMessage commandMessageCommit = new CommandMessage(cmdIdPush, Actions.PUSH_COMMIT, null);
			message = mapper.writeValueAsString(commandMessageCommit);
			log.info("send {}", commandMessageCommit);
			pushSessionFuture.get(10, TimeUnit.SECONDS).sendMessage(new TextMessage(message.getBytes()));
			assertEquals(expected3, blockingQueue.poll(10, TimeUnit.SECONDS));

			// push close
			String expected4 = """
					{"id":"push","error":null,"next":false,"payload":{"ack":"push","message":"Push close: done with enableConstraints=true"}}""";

			CommandMessage commandMessagePushClose = new CommandMessage(cmdIdPush, Actions.PUSH_CLOSE, null);
			message = mapper.writeValueAsString(commandMessagePushClose);
			log.info("send {}", commandMessagePushClose);
			pushSessionFuture.get(10, TimeUnit.SECONDS).sendMessage(new TextMessage(message.getBytes()));
			assertEquals(expected4, blockingQueue.poll(10, TimeUnit.SECONDS));
			pullSessionFuture.get(10, TimeUnit.SECONDS).close();
			pushSessionFuture.get(10, TimeUnit.SECONDS).close();

			JSONArray result = DataUtils.getData(datasource, "PUBLIC", "MYTABLE");
			// check result
			String expected5 = """
					[{"MYSTRING":"isAString","MYAGE":1,"BLI":"blo"},{"MYSTRING":"isAString2","MYAGE":42,"BLI":"blio"},{"MYSTRING":"is not a blank string","MYAGE":43,"BLI":"BLOT"}]""";
			Assertions.assertEquals(expected5, result.toString());

	}
 */
}
