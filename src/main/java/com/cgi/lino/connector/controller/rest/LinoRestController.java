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

package com.cgi.lino.connector.controller.rest;


import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.sql.SQLException;
import java.text.ParseException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import lombok.extern.slf4j.Slf4j;


import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.servlet.mvc.method.annotation.StreamingResponseBody;

import com.cgi.lino.connector.controller.service.InfoDatabaseService;
import com.cgi.lino.connector.controller.service.PullDatabaseService;
import com.cgi.lino.connector.controller.service.rest.Pusher;
import com.cgi.lino.connector.controller.service.rest.PusherFactory;
import com.cgi.lino.connector.controller.ws.dto.payload.Filter;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.ValueNode;

@RestController
@RequestMapping(path = "/api/v1")
@Slf4j
public class LinoRestController {


	private final ObjectMapper mapper;

	private final PusherFactory pusherFactory;

	private InfoDatabaseService infoDataService;

	private PullDatabaseService pullDatabaseService;

	private static final String cmdId = "REST";

	public LinoRestController(
			final InfoDatabaseService infoDataService, 
			final PullDatabaseService pullDatabaseService, 
			final ObjectMapper mapper, 
			final PusherFactory pusherFactory) {
		this.infoDataService = infoDataService;
		this.pullDatabaseService = pullDatabaseService;
		this.mapper = mapper;
		this.pusherFactory = pusherFactory;
		mapper.getFactory().disable(JsonGenerator.Feature.AUTO_CLOSE_TARGET);
	}

	@GetMapping(path = "/", produces = MediaType.APPLICATION_JSON_VALUE)
	public ObjectNode getInfo() throws SQLException {
		return infoDataService.getDatabaseInformation(cmdId);
	}

	@GetMapping(path = "/schemas", produces = MediaType.APPLICATION_JSON_VALUE)
	public ObjectNode getSchemas() throws SQLException {
		return infoDataService.getSchemas(cmdId);
	}

	@GetMapping(path = "/tables", produces = MediaType.APPLICATION_JSON_VALUE)
	public ArrayNode getTables(@RequestParam(required = false) String schema) throws SQLException {
		return infoDataService.getTables(cmdId, schema);
	}

	@GetMapping(path = "/relations", produces = MediaType.APPLICATION_JSON_VALUE)
	public ObjectNode getRelations(@RequestParam(required = false) String schema) throws SQLException {
		return mapper.createObjectNode()
				.put("version", "v1")
				.set("relations", infoDataService.getRelations(cmdId, schema));
	}

	@Transactional(readOnly = true)
	@GetMapping(path = "/data/{tableName}", produces = MediaType.APPLICATION_NDJSON_VALUE)
	public ResponseEntity<StreamingResponseBody> pullData(
			@RequestParam(required = false) String schema,
			@PathVariable("tableName") String tableName,
			@RequestBody(required = false) Filter filter,
			@RequestHeader(name = "Select-Columns", required = false) List<String> selectColumns)
			throws SQLException, IOException, ParseException {

		StreamingResponseBody stream = out -> {
			Stream<ObjectNode> result=null;
			try {
				result = this.pullDatabaseService.pullData("rest", schema, tableName, selectColumns, filter);
			} catch (SQLException | ParseException e) {
				log.error(e.getMessage(), e);
			}
			if (result != null) {
				result.forEach(entry -> {
					try {
						mapper.writeValue(out, entry);
						out.write(System.lineSeparator().getBytes());
						out.flush();
					} catch (IOException e) {
						log.error(e.getMessage(), e);
					}

				});
			}
		};

		return ResponseEntity.ok().header("Content-Disposition", "inline").body(stream);

	}

	@Transactional
	@PostMapping(path = "/data/{tableName}", consumes = MediaType.APPLICATION_NDJSON_VALUE)
	public void pushData(@RequestParam(required = false) String schema, @RequestParam(required = false) String mode,
			@RequestParam(required = false) boolean disableConstraints,
			@PathVariable("tableName") String tableName, InputStream data,
			@RequestHeader(name = "Primary-Keys", required = false) String pkeysJson)
			throws SQLException, IOException, ParseException {
	
	

		try (BufferedReader reader = new BufferedReader(new InputStreamReader(data));
				Pusher pusher = pusherFactory.create( schema, tableName, mode, disableConstraints)) {
			pusher.open();
			String line;
			do {
				line = reader.readLine();
				if (line != null) {
					log.info("Push " + tableName + " - received " + line);
					@SuppressWarnings("unchecked")
					Map<String, ValueNode> object = mapper.readValue(line, HashMap.class);
					pusher.push(object);
				}
			} while (line != null);
		}

		log.info("Push " + tableName + " - closing connection");
	}
}
