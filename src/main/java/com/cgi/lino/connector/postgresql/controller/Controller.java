package com.cgi.lino.connector.postgresql.controller;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.Date;
import java.util.Iterator;
import java.util.Map;

import javax.sql.DataSource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.servlet.mvc.method.annotation.StreamingResponseBody;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

@RestController
@RequestMapping(path = "/api/v1")
public class Controller {

	private Logger logger = LoggerFactory.getLogger(Controller.class);

	@Autowired
	private DataSource datasource;

	@Autowired
	private ObjectMapper mapper;

	@GetMapping(path = "/", produces = MediaType.APPLICATION_JSON_VALUE)
	public ObjectNode getInfo() throws SQLException {
		ObjectNode result = mapper.createObjectNode();

		Connection connection = datasource.getConnection();
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

		return result;
	}

	@GetMapping(path = "/schemas", produces = MediaType.APPLICATION_JSON_VALUE)
	public ObjectNode getSchemas() throws SQLException {
		ObjectNode result = mapper.createObjectNode();

		Connection connection = datasource.getConnection();
		DatabaseMetaData databaseMetaData = connection.getMetaData();

		ArrayNode schemas = mapper.createArrayNode();
		ResultSet schemasrs = databaseMetaData.getSchemas();
		while (schemasrs.next()) {
			String table_schem = schemasrs.getString("TABLE_SCHEM");
			String table_catalog = schemasrs.getString("TABLE_CATALOG");
			ObjectNode schema = mapper.createObjectNode();
			schema.put("name", table_schem);
			schema.put("catalog", table_catalog);
			schemas.add(schema);
		}
		result.set("schemas", schemas);

		return result;
	}

	@GetMapping(path = "/tables", produces = MediaType.APPLICATION_JSON_VALUE)
	public String getTables(@RequestParam(required = false) String schema) throws SQLException {
		StringBuilder result = new StringBuilder("{\"version\":\"v1\",\"tables\":[");

		Connection connection = datasource.getConnection();
		DatabaseMetaData databaseMetaData = connection.getMetaData();

		String tablePrefix = "";
		ResultSet resultSet = databaseMetaData.getTables(null, schema, null, new String[] { "TABLE" });
		while (resultSet.next()) {
			String tableName = resultSet.getString("TABLE_NAME");
			result.append(tablePrefix);

			tablePrefix = ",";
			result.append("{\"name\":\"");
			result.append(tableName);
			result.append("\",\"keys\":[");

			// primary keys
			String pkPrefix = "";
			ResultSet pkRs = databaseMetaData.getPrimaryKeys(null, schema, tableName);
			while (pkRs.next()) {
				String pkName = pkRs.getString("COLUMN_NAME");
				result.append(pkPrefix);
				pkPrefix = ",";
				result.append("\"");
				result.append(pkName);
				result.append("\"");
			}

			result.append("]}");

		}

		result.append("]}");
		return result.toString();
	}

	@GetMapping(path = "/relations", produces = MediaType.APPLICATION_JSON_VALUE)
	public String getRelations(@RequestParam(required = false) String schema) throws SQLException {
		StringBuilder result = new StringBuilder("{\"version\":\"v1\",\"relations\":[");

		Connection connection = datasource.getConnection();
		DatabaseMetaData databaseMetaData = connection.getMetaData();

		String fkPrefix = "";
		ResultSet resultSet = databaseMetaData.getTables(null, schema, null, new String[] { "TABLE" });
		while (resultSet.next()) {
			String tableName = resultSet.getString("TABLE_NAME");

			ResultSet fkSet = databaseMetaData.getImportedKeys(null, schema, tableName);
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

		result.append("]}");
		return result.toString();
	}

	@GetMapping(path = "/data/{tableName}", produces = MediaType.APPLICATION_NDJSON_VALUE)
	public ResponseEntity<StreamingResponseBody> getData(@RequestParam(required = false) String schema,
			@PathVariable("tableName") String tableName, @RequestBody(required = false) Map<String, Object> filter)
			throws SQLException, IOException {
		String where = "where 1=1";
		int limit = 0;

		if (filter != null) {
			String filterWhere = (String) filter.get("where");
			if (filterWhere != null && !filterWhere.isBlank()) {
				where = "where " + filterWhere;
			}

			@SuppressWarnings("unchecked")
			Map<String, Object> filterValues = (Map<String, Object>) filter.get("values");
			for (Iterator<Map.Entry<String, Object>> iterator = filterValues.entrySet().iterator(); iterator
					.hasNext();) {
				Map.Entry<String, Object> filterValue = iterator.next();
				if (filterValue.getValue() instanceof String) {
					where = where + " and " + filterValue.getKey() + "='" + filterValue.getValue() + "'";
				} else if (filterValue.getValue() instanceof Integer) {
					where = where + " and " + filterValue.getKey() + "=" + filterValue.getValue();
				} else {
					this.logger.error("Unsupported filter type : " + filterValue.getValue().getClass());
				}
			}

			limit = (int) filter.get("limit");
		}

		final String filterClause;
		if (limit > 0) {
			filterClause = where + " limit " + limit;
		} else {
			filterClause = where;
		}

		mapper.getFactory().disable(JsonGenerator.Feature.AUTO_CLOSE_TARGET);

		this.logger.info("Select from " + tableName + " " + filterClause);

		StreamingResponseBody stream = out -> {
			try (Connection connection = datasource.getConnection()) {
				PreparedStatement stmt = connection.prepareStatement("select * from " + tableName + " " + filterClause);
				ResultSet rs = stmt.executeQuery();

				while (rs.next()) {
					mapRow(rs, out);
					out.write(System.lineSeparator().getBytes());
					out.flush();
				}
			} catch (SQLException e) {
				throw new IOException(e);
			}
		};

		return ResponseEntity.ok().header("Content-Disposition", "inline").body(stream);
	}

	private void mapRow(ResultSet rs, OutputStream result) throws SQLException, IOException {
		ResultSetMetaData rsmd = rs.getMetaData();
		int columnCount = rsmd.getColumnCount();

		byte[] separator = new byte[] {};

		result.write('{');
		for (int index = 1; index <= columnCount; index++) {
			String column = rsmd.getColumnName(index);
			Object value = rs.getObject(column);

			result.write(separator);
			result.write('"');
			result.write(column.getBytes());
			result.write("\":".getBytes());

			separator = ",".getBytes();

			if (value == null) {
				result.write("null".getBytes());
			} else if (value instanceof Integer) {
				mapper.writeValue(result, (Integer) value);
			} else if (value instanceof String) {
				mapper.writeValue(result, (String) value);
			} else if (value instanceof Boolean) {
				mapper.writeValue(result, (Boolean) value);
			} else if (value instanceof Date) {
				mapper.writeValue(result, (Date) value);
			} else if (value instanceof Long) {
				mapper.writeValue(result, (Long) value);
			} else if (value instanceof Short) {
				mapper.writeValue(result, (Short) value);
			} else if (value instanceof Double) {
				mapper.writeValue(result, (Double) value);
			} else if (value instanceof Float) {
				mapper.writeValue(result, (Float) value);
			} else if (value instanceof BigDecimal) {
				mapper.writeValue(result, (BigDecimal) value);
			} else if (value instanceof Byte) {
				mapper.writeValue(result, (Byte) value);
			} else if (value instanceof byte[]) {
				mapper.writeValue(result, (byte[]) value);
			} else {
				mapper.writeValue(result, "Unmappable Type: " + value.getClass());
			}
		}
		result.write('}');
	}

	@PostMapping(path = "/data/{tableName}", consumes = MediaType.APPLICATION_NDJSON_VALUE)
	public void pushData(@RequestParam(required = false) String schema, @PathVariable("tableName") String tableName,
			InputStream data) throws SQLException, IOException {

		BufferedReader reader = new BufferedReader(new InputStreamReader(data));
		String line;
		do {
			line = reader.readLine();
			if (line != null) {
				logger.info(tableName + " - received data " + line);
			}
		} while (line != null);

		logger.info(tableName + " - closing connection");
	}

}
