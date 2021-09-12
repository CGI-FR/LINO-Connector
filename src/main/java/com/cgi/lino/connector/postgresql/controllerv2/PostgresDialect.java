package com.cgi.lino.connector.postgresql.controllerv2;

import java.util.Arrays;
import java.util.Optional;
import java.util.stream.Collectors;

import org.apache.flink.api.java.io.jdbc.dialect.JDBCDialect;

public class PostgresDialect implements JDBCDialect {

	private static final long serialVersionUID = 1L;

	@Override
	public boolean canHandle(String url) {
		return url.startsWith("jdbc:postgresql:");
	}

	@Override
	public Optional<String> defaultDriverName() {
		return Optional.of("org.postgresql.Driver");
	}

	/**
	 * Postgres upsert query. It use ON CONFLICT ... DO UPDATE SET.. to replace into
	 * Postgres.
	 */
	@Override
	public Optional<String> getUpsertStatement(String tableName, String[] fieldNames, String[] uniqueKeyFields) {
		String uniqueColumns = Arrays.stream(uniqueKeyFields).map(this::quoteIdentifier).collect(Collectors.joining(", "));
		String updateClause = Arrays.stream(fieldNames).map(f -> quoteIdentifier(f) + "=EXCLUDED." + quoteIdentifier(f)).collect(Collectors.joining(", "));
		return Optional.of(getInsertIntoStatement(tableName, fieldNames) + " ON CONFLICT (" + uniqueColumns + ")" + " DO UPDATE SET " + updateClause);
	}
}
