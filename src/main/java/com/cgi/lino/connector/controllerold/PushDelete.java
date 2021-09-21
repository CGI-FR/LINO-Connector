package com.cgi.lino.connector.controllerold;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import javax.sql.DataSource;

import com.fasterxml.jackson.databind.ObjectMapper;

public class PushDelete implements Pusher {

	private final String schema;

	protected final DataSource datasource;

	protected final ObjectMapper mapper;

	public PushDelete(final DataSource datasource, final ObjectMapper mapper, final String schema) {
		this.datasource = datasource;
		this.mapper = mapper;
		this.schema = schema;
	}

	@Override
	public void push(String jsonline, String tableName) throws IOException, SQLException {
		@SuppressWarnings("unchecked")
		Map<String, Object> object = mapper.readValue(jsonline, HashMap.class);

		List<String> pkNames = new ArrayList<>();
		StringBuilder sqlString = new StringBuilder("DELETE FROM " + tableName);
		try (Connection connection = datasource.getConnection()) {
			DatabaseMetaData databaseMetaData = connection.getMetaData();

			String keyword = " WHERE ";
			ResultSet pkRs = databaseMetaData.getPrimaryKeys(null, schema, tableName);
			while (pkRs.next()) {
				String pkName = pkRs.getString("COLUMN_NAME");
				sqlString.append(keyword);
				sqlString.append(pkName);
				sqlString.append("=?");
				keyword = " and ";
				pkNames.add(pkName);
			}

			int i = 0;
			try (PreparedStatement statement = connection.prepareStatement(sqlString.toString())) {
				for (Iterator<String> iterator = pkNames.iterator(); iterator.hasNext();) {
					String pkName = iterator.next();
					statement.setObject(++i, object.get(pkName));
				}
				statement.execute();
			}
		}
	}

}
