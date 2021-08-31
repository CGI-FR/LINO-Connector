package com.cgi.lino.connector.postgresql.controller;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;

import javax.sql.DataSource;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping(path = "/api/v1")
public class Controller {

	@Autowired
	private DataSource datasource;

	@GetMapping(path = "/tables", produces = MediaType.APPLICATION_JSON_VALUE)
	public String getTables(@RequestParam(required = false) String schema) throws SQLException {
		StringBuilder result = new StringBuilder("{\"version\":\"v1\",\"tables\":[");

		Connection connection = datasource.getConnection();
		DatabaseMetaData databaseMetaData = connection.getMetaData();

		String tablePrefix = "";
		ResultSet resultSet = databaseMetaData.getTables(null, null, null, new String[] { "TABLE" });
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

}
