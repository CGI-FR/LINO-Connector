package com.cgi.lino.connector.postgresql.controller;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;

import javax.sql.DataSource;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

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
	public String getData(@RequestParam(required = false) String schema, @PathVariable("tableName") String tableName)
			throws SQLException {
		StringBuilder result = new StringBuilder("{");

		try (Connection connection = datasource.getConnection()) {

			// TODO
			result.append("\"table\":\"");
			result.append(tableName);
			result.append("\"");

			result.append('}');
			result.append(System.lineSeparator());
		}

		return result.toString();
	}

}
