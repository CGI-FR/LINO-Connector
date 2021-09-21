package com.cgi.lino.connector.controllerold;

import java.io.IOException;
import java.sql.SQLException;

import javax.sql.DataSource;

import com.fasterxml.jackson.databind.ObjectMapper;

public interface Pusher {

	static Pusher create(DataSource datasource, ObjectMapper mapper, String mode, String schema) {
		switch (mode) {
		case "insert":
			return new PushInsert(datasource, mapper);
		case "update":
			return new PushUpdate(datasource, mapper, schema);
		case "delete":
			return new PushDelete(datasource, mapper, schema);
		case "truncate":
			return new PushTruncate(datasource, mapper);
		default:
			throw new UnsupportedOperationException("unknown push mode: " + mode);
		}
	}

	void push(String jsonline, String tableName) throws IOException, SQLException;

}
