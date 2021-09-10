package com.cgi.lino.connector.postgresql.controller;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import javax.sql.DataSource;

public class ConstraintDisabler {

	private final Set<String> disabled = new HashSet<String>();

	private final DataSource datasource;

	public ConstraintDisabler(final DataSource datasource) {
		this.datasource = datasource;
	}

	public void disable(String tableName) throws SQLException {
		if (this.disabled.contains(tableName)) {
			return;
		}

		try (Connection connection = datasource.getConnection(); Statement statement = connection.createStatement()) {
			statement.executeUpdate("ALTER TABLE " + tableName + " DISABLE TRIGGER ALL");
		}

		this.disabled.add(tableName);
	}

	public void enable(String tableName) throws SQLException {
		if (!this.disabled.contains(tableName)) {
			return;
		}

		try (Connection connection = datasource.getConnection(); Statement statement = connection.createStatement()) {
			statement.executeUpdate("ALTER TABLE " + tableName + " ENABLE TRIGGER ALL");
		}

		this.disabled.remove(tableName);
	}

	public void enableAll() throws SQLException {
		for (Iterator<String> iterator = disabled.iterator(); iterator.hasNext();) {
			String tableName = iterator.next();
			this.enable(tableName);
		}
	}

}
