package com.cgi.lino.connector.postgresql.controller;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.ParseException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

import javax.persistence.EntityManager;
import javax.persistence.Query;
import javax.persistence.Tuple;
import javax.sql.DataSource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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

import com.cgi.lino.connector.postgresql.controller.TableAccessor.TableDescriptor;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

@RestController
@RequestMapping(path = "/api/v1")
public class HibernateController {

	private Logger logger = LoggerFactory.getLogger(HibernateController.class);

	private final DataSource datasource;

	private final EntityManager entityManager;

	private final ObjectMapper mapper;

	private final PusherFactory pusherFactory;

	public HibernateController(final DataSource datasource, final EntityManager entityManager, final ObjectMapper mapper, final PusherFactory pusherFactory) {
		this.datasource = datasource;
		this.entityManager = entityManager;
		this.mapper = mapper;
		this.pusherFactory = pusherFactory;

		mapper.getFactory().disable(JsonGenerator.Feature.AUTO_CLOSE_TARGET);
	}

	@GetMapping(path = "/", produces = MediaType.APPLICATION_JSON_VALUE)
	public ObjectNode getInfo() throws SQLException {
		ObjectNode result = mapper.createObjectNode();

		try (Connection connection = datasource.getConnection()) {
			DatabaseMetaData databaseMetaData = connection.getMetaData();

			ObjectNode database = mapper.createObjectNode();
			database.put("product", databaseMetaData.getDatabaseProductName());
			database.put("version", databaseMetaData.getDatabaseProductVersion());
			result.set("database", database);

			ObjectNode driver = mapper.createObjectNode();
			driver.put("name", databaseMetaData.getDriverName());
			driver.put("version", databaseMetaData.getDriverVersion());
			result.set("driver", driver);

			ObjectNode jdbc = mapper.createObjectNode();
			jdbc.put("version", databaseMetaData.getJDBCMajorVersion() + databaseMetaData.getJDBCMinorVersion());
			result.set("jdbc", jdbc);
		}

		return result;
	}

	@GetMapping(path = "/schemas", produces = MediaType.APPLICATION_JSON_VALUE)
	public ObjectNode getSchemas() throws SQLException {
		ObjectNode result = mapper.createObjectNode();

		try (Connection connection = datasource.getConnection()) {
			DatabaseMetaData databaseMetaData = connection.getMetaData();

			ArrayNode schemas = mapper.createArrayNode();
			try (ResultSet schemasrs = databaseMetaData.getSchemas()) {
				while (schemasrs.next()) {
					String table_schem = schemasrs.getString("TABLE_SCHEM");
					String table_catalog = schemasrs.getString("TABLE_CATALOG");
					ObjectNode schema = mapper.createObjectNode();
					schema.put("name", table_schem);
					schema.put("catalog", table_catalog);
					schemas.add(schema);
				}
			}
			result.set("schemas", schemas);
		}

		return result;
	}

	@GetMapping(path = "/tables", produces = MediaType.APPLICATION_JSON_VALUE)
	public String getTables(@RequestParam(required = false) String schema) throws SQLException, JsonProcessingException {
		StringBuilder result = new StringBuilder("{\"version\":\"v1\",\"tables\":[");

		try (Connection connection = datasource.getConnection()) {
			DatabaseMetaData databaseMetaData = connection.getMetaData();

			String tablePrefix = "";
			try (ResultSet resultSet = databaseMetaData.getTables(null, schema, null, new String[] { "TABLE" })) {
				while (resultSet.next()) {
					String tableName = resultSet.getString("TABLE_NAME");
					result.append(tablePrefix);

					tablePrefix = ",";

					TableAccessor accessor = new TableAccessor(datasource, schema, tableName);
					TableDescriptor descriptor = accessor.getDescriptor();
					result.append(mapper.writeValueAsString(descriptor));
				}
			}
		}

		result.append("]}");
		return result.toString();
	}

	@GetMapping(path = "/relations", produces = MediaType.APPLICATION_JSON_VALUE)
	public String getRelations(@RequestParam(required = false) String schema) throws SQLException {
		StringBuilder result = new StringBuilder("{\"version\":\"v1\",\"relations\":[");

		try (Connection connection = datasource.getConnection()) {
			DatabaseMetaData databaseMetaData = connection.getMetaData();

			String fkPrefix = "";
			try (ResultSet resultSet = databaseMetaData.getTables(null, schema, null, new String[] { "TABLE" })) {
				while (resultSet.next()) {
					String tableName = resultSet.getString("TABLE_NAME");

					try (ResultSet fkSet = databaseMetaData.getImportedKeys(null, schema, tableName)) {
						while (fkSet.next()) {
							String fkName = fkSet.getString("FK_NAME");
							result.append(fkPrefix);

							fkPrefix = ",";
							result.append("{\"name\":\"");
							result.append(fkName);

							// parent table
							result.append("\",\"parent\":{\"name\":\"");
							result.append(fkSet.getString("PKTABLE_NAME"));
							result.append("\",\"keys\":[\"");
							result.append(fkSet.getString("PKCOLUMN_NAME"));
							result.append("\"]");

							// child table
							result.append("},\"child\":{\"name\":\"");
							result.append(tableName);
							result.append("\",\"keys\":[\"");
							result.append(fkSet.getString("FKCOLUMN_NAME"));
							result.append("\"]");

							result.append("}}");
						}
					}
				}
			}
		}

		result.append("]}");
		return result.toString();
	}

	@Transactional(readOnly = true)
	@GetMapping(path = "/data/{tableName}", produces = MediaType.APPLICATION_NDJSON_VALUE)
	public ResponseEntity<StreamingResponseBody> pullData(@RequestParam(required = false) String schema, @PathVariable("tableName") String tableName,
			@RequestBody(required = false) Map<String, Object> filter, @RequestHeader(name = "Select-Columns", required = false) String scolsJson) throws SQLException, IOException, ParseException {
		TableAccessor accessor = new TableAccessor(datasource, schema, tableName);

		Collection<String> selectColumns = null;
		if (scolsJson != null) {
			selectColumns = Arrays.asList(mapper.readValue(scolsJson, String[].class));
		}

		int limit = 0;
		String andWhere = null;
		Collection<Object> values;
		Set<String> whereColumns;
		if (filter != null) {
			@SuppressWarnings("unchecked")
			Map<String, Object> filterValues = (Map<String, Object>) filter.get("values");
			andWhere = (String) filter.get("where");
			values = accessor.cast(filterValues);
			whereColumns = filterValues.keySet();
			limit = (int) filter.get("limit");
		} else {
			values = Collections.emptyList();
			whereColumns = Set.of();
		}

		String querySql = accessor.getNativeQuerySelect(selectColumns, whereColumns, andWhere, limit);
		this.logger.info("Pull " + accessor.getTableNameFull() + " - " + querySql);

		StreamingResponseBody stream = out -> {
			int position = 0;
			Query query = entityManager.createNativeQuery(querySql, Tuple.class);
			for (Object value : values) {
				query.setParameter(++position, value);
			}
			@SuppressWarnings("unchecked")
			Stream<Tuple> result = query.getResultStream();
			result.forEach(entry -> {
				try {
					Map<String, Object> resultItem = new HashMap<>();
					entry.getElements().forEach(col -> {
						resultItem.put(col.getAlias(), entry.get(col));
					});
					mapper.writeValue(out, resultItem);
					out.write(System.lineSeparator().getBytes());
					out.flush();
				} catch (IOException e) {
					throw new RuntimeException(e);
				}
			});
		};

		return ResponseEntity.ok().header("Content-Disposition", "inline").body(stream);
	}

	@Transactional
	@PostMapping(path = "/data/{tableName}", consumes = MediaType.APPLICATION_NDJSON_VALUE)
	public void pushData(@RequestParam(required = false) String schema, @RequestParam(required = false) String mode, @RequestParam(required = false) boolean disableConstraints,
			@PathVariable("tableName") String tableName, InputStream data, @RequestHeader(name = "Primary-Keys", required = false) String pkeysJson) throws SQLException, IOException, ParseException {
		TableAccessor accessor = new TableAccessor(datasource, schema, tableName);

		if (pkeysJson != null) {
			String[] pkeys = mapper.readValue(pkeysJson, String[].class);
			accessor = accessor.withPrimaryKeys(pkeys);
		}

		logger.info("Push " + accessor.getTableNameFull() + " - mode=" + mode + " disableConstraints=" + disableConstraints);

		try (BufferedReader reader = new BufferedReader(new InputStreamReader(data)); Pusher pusher = pusherFactory.create(mode, disableConstraints, accessor)) {
			pusher.open();
			String line;
			do {
				line = reader.readLine();
				if (line != null) {
					logger.info("Push " + tableName + " - received " + line);
					@SuppressWarnings("unchecked")
					Map<String, Object> object = mapper.readValue(line, HashMap.class);
					pusher.push(object);
				}
			} while (line != null);
		}

		logger.info("Push " + tableName + " - closing connection");
	}
}
