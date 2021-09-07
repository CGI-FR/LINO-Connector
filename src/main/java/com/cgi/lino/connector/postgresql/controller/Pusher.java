package com.cgi.lino.connector.postgresql.controller;

import java.io.IOException;

import javax.sql.DataSource;

import com.fasterxml.jackson.databind.ObjectMapper;

public interface Pusher {

	static Pusher create(DataSource datasource, ObjectMapper mapper, String mode) {
		switch (mode) {
		case "insert":
			return new PushInsert(datasource, mapper);
		default:
			throw new UnsupportedOperationException("unknown push mode: " + mode);
		}
	}

	void push(String jsonline, String tableName) throws IOException;

}
