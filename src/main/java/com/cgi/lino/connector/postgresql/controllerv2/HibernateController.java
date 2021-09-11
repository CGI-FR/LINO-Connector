package com.cgi.lino.connector.postgresql.controllerv2;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import javax.persistence.EntityManager;
import javax.persistence.Query;
import javax.persistence.Tuple;
import javax.sql.DataSource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.servlet.mvc.method.annotation.StreamingResponseBody;

import com.cgi.lino.connector.postgresql.controller.Controller;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

@RestController
@RequestMapping(path = "/api/v2")
public class HibernateController {

	private Logger logger = LoggerFactory.getLogger(Controller.class);

	private final DataSource datasource;

	private final EntityManager entityManager;

	private final ObjectMapper mapper;

	public HibernateController(final DataSource datasource, final EntityManager entityManager,
			final ObjectMapper mapper) {
		this.datasource = datasource;
		this.entityManager = entityManager;
		this.mapper = mapper;

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
	public String getTables(@RequestParam(required = false) String schema) throws SQLException {
		StringBuilder result = new StringBuilder("{\"version\":\"v1\",\"tables\":[");

		try (Connection connection = datasource.getConnection()) {
			DatabaseMetaData databaseMetaData = connection.getMetaData();

			String tablePrefix = "";
			try (ResultSet resultSet = databaseMetaData.getTables(null, schema, null, new String[] { "TABLE" })) {
				while (resultSet.next()) {
					String tableName = resultSet.getString("TABLE_NAME");
					result.append(tablePrefix);

					tablePrefix = ",";
					result.append("{\"name\":\"");
					result.append(tableName);
					result.append("\",\"keys\":[");

					// primary keys
					String pkPrefix = "";
					try (ResultSet pkRs = databaseMetaData.getPrimaryKeys(null, schema, tableName)) {
						while (pkRs.next()) {
							String pkName = pkRs.getString("COLUMN_NAME");
							result.append(pkPrefix);
							pkPrefix = ",";
							result.append("\"");
							result.append(pkName);
							result.append("\"");
						}
					}

					result.append("]}");
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

	@GetMapping(path = "/data/{tableName}", produces = MediaType.APPLICATION_NDJSON_VALUE)
	public ResponseEntity<StreamingResponseBody> pullData(@RequestParam(required = false) String schema,
			@PathVariable("tableName") String tableName, @RequestBody(required = false) Map<String, Object> filter)
			throws SQLException, IOException {
		String where = "where 1=1";
		int limit = 0;

		List<Object> values = new ArrayList<>();
		if (filter != null) {
			String filterWhere = (String) filter.get("where");
			if (filterWhere != null && !filterWhere.isBlank()) {
				where = "where " + filterWhere;
			}

			@SuppressWarnings("unchecked")
			Map<String, Object> filterValues = (Map<String, Object>) filter.get("values");
			for (Map.Entry<String, Object> filterValue : filterValues.entrySet()) {
				where = where + " and " + filterValue.getKey() + "=?";
				values.add(filterValue.getValue());
			}

			limit = (int) filter.get("limit");
		}

		final String filterClause;
		if (limit > 0) {
			filterClause = where + " limit " + limit;
		} else {
			filterClause = where;
		}

		String querySql;
		if (schema != null) {
			querySql = "select * from " + schema + "." + tableName + " " + filterClause;
		} else {
			querySql = "select * from " + tableName + " " + filterClause;
		}

		this.logger.info("Pull " + tableName + " - " + querySql);

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
}
