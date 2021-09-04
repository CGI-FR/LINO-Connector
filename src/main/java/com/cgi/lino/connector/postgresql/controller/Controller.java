package com.cgi.lino.connector.postgresql.controller;

import java.io.IOException;
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

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

@RestController
@RequestMapping(path = "/api/v1")
public class Controller {

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

	@GetMapping(path = "/data/{tableName}", produces = MediaType.APPLICATION_JSON_VALUE)
	public String getData(@RequestParam(required = false) String schema, @PathVariable("tableName") String tableName,
			@RequestBody(required = false) Map<String, Object> filter) throws SQLException, IOException {
		StringBuilder result = new StringBuilder();

		String where = "where 1=1";
		int limit = 10;

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
					System.err.println("Unsupported filter type : " + filterValue.getValue().getClass());
				}
			}

			limit = (int) filter.get("limit");
		}

		String filterClause = where;
		if (limit > 0) {
			filterClause = filterClause + " limit " + limit;
		}

		System.out.println("from " + tableName + " " + filterClause);

		try (Connection connection = datasource.getConnection()) {

			PreparedStatement stmt = connection.prepareStatement("select * from " + tableName + " " + filterClause);
			ResultSet rs = stmt.executeQuery();

			while (rs.next()) {
				mapRow(rs, result);
				result.append(System.lineSeparator());
			}
		}

		return result.toString();
	}

	private void mapRow(ResultSet rs, StringBuilder result) throws SQLException, JsonProcessingException {
		ResultSetMetaData rsmd = rs.getMetaData();
		int columnCount = rsmd.getColumnCount();

		String separator = "";

		result.append('{');
		for (int index = 1; index <= columnCount; index++) {
			String column = rsmd.getColumnName(index);
			Object value = rs.getObject(column);

			result.append(separator);
			result.append("\"" + column + "\":");

			separator = ",";

			if (value == null) {
				result.append("null");
			} else if (value instanceof Integer) {
				result.append(mapper.writeValueAsString((Integer) value));
			} else if (value instanceof String) {
				result.append(mapper.writeValueAsString((String) value));
			} else if (value instanceof Boolean) {
				result.append(mapper.writeValueAsString((Boolean) value));
			} else if (value instanceof Date) {
				result.append(mapper.writeValueAsString((Date) value));
			} else if (value instanceof Long) {
				result.append(mapper.writeValueAsString((Long) value));
			} else if (value instanceof Double) {
				result.append(mapper.writeValueAsString((Double) value));
			} else if (value instanceof Float) {
				result.append(mapper.writeValueAsString((Float) value));
			} else if (value instanceof BigDecimal) {
				result.append(mapper.writeValueAsString((BigDecimal) value));
			} else if (value instanceof Byte) {
				result.append(mapper.writeValueAsString((Byte) value));
			} else if (value instanceof byte[]) {
				result.append(mapper.writeValueAsString((byte[]) value));
			} else {
				result.append("\"Unmappable Type: " + value.getClass() + "\"");
			}
		}
		result.append('}');
	}

}
