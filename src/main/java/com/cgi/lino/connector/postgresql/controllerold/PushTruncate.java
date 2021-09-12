package com.cgi.lino.connector.postgresql.controllerold;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashSet;
import java.util.Set;

import javax.sql.DataSource;

import com.fasterxml.jackson.databind.ObjectMapper;

public class PushTruncate extends PushInsert implements Pusher {

	private final Set<String> done = new HashSet<>();

	public PushTruncate(DataSource datasource, ObjectMapper mapper) {
		super(datasource, mapper);
	}

	@Override
	public void push(String jsonline, String tableName) throws IOException, SQLException {
		if (!done.contains(tableName)) {
			done.add(tableName);
			try (Connection connection = datasource.getConnection(); Statement statement = connection.createStatement()) {
				statement.executeUpdate("TRUNCATE " + tableName + " CASCADE");
			}
		}
		super.push(jsonline, tableName);
	}

}
