package com.cgi.lino.connector.postgresql.controller;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import javax.sql.DataSource;

import org.springframework.jdbc.core.JdbcTemplate;

import com.fasterxml.jackson.databind.ObjectMapper;

public class PushUpdate implements Pusher {

	private final String schema;

	protected final DataSource datasource;

	protected final ObjectMapper mapper;

	public PushUpdate(final DataSource datasource, final ObjectMapper mapper, final String schema) {
		this.datasource = datasource;
		this.mapper = mapper;
		this.schema = schema;
	}

	@Override
	public void push(String jsonline, String tableName) throws IOException, SQLException {
		@SuppressWarnings("unchecked")
		Map<String, Object> object = mapper.readValue(jsonline, HashMap.class);

		StringBuilder sqlString = new StringBuilder("UPDATE " + tableName);

		String keyword = " SET ";
		List<String> colNames = new ArrayList<>();
		List<Object> colValues = new ArrayList<>();
		for (Iterator<Map.Entry<String, Object>> iterator = object.entrySet().iterator(); iterator.hasNext();) {
			Map.Entry<String, Object> entry = iterator.next();
			sqlString.append(keyword);
			sqlString.append("=?");
			colNames.add(entry.getKey());
			colValues.add(entry.getValue());
		}

		List<String> pkNames = new ArrayList<>();
		try (Connection connection = datasource.getConnection()) {
			DatabaseMetaData databaseMetaData = connection.getMetaData();

			keyword = " WHERE ";
			ResultSet pkRs = databaseMetaData.getPrimaryKeys(null, schema, tableName);
			while (pkRs.next()) {
				String pkName = pkRs.getString("COLUMN_NAME");
				sqlString.append(keyword);
				sqlString.append(pkName);
				sqlString.append("=?");
				keyword = " and ";
				pkNames.add(pkName);
			}
		}

		List<Object> pkValues = new ArrayList<>();
		for (Iterator<String> iterator = pkNames.iterator(); iterator.hasNext();) {
			String pkName = iterator.next();
			pkValues.add(object.get(pkName));
		}

		JdbcTemplate template = new JdbcTemplate(datasource);
		template.update(sqlString.toString(), colValues.toArray(), pkValues.toArray());
	}

}
