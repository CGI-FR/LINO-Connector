package com.cgi.lino.connector.controller;

import java.util.Arrays;
import java.util.Optional;
import java.util.stream.Collectors;

public class NativeDialectPostgres implements NativeDialect {

	@Override
	public boolean canHandle(String url) {
		return url.startsWith("jdbc:postgresql:");
	}

	/**
	 * Postgres upsert query.
	 * 
	 * It use ON CONFLICT ... DO UPDATE SET.. to replace into Postgres.
	 */
	@Override
	public Optional<String> getUpsertStatement(String schemaName, String tableName, String[] fieldNames, String[] uniqueKeyFields) {
		String uniqueColumns = Arrays.stream(uniqueKeyFields).map(this::quoteIdentifier).collect(Collectors.joining(", "));
		String updateClause = Arrays.stream(fieldNames).map(f -> quoteIdentifier(f) + "=EXCLUDED." + quoteIdentifier(f)).collect(Collectors.joining(", "));
		return Optional.of(getInsertIntoStatement(schemaName, tableName, fieldNames) + " ON CONFLICT (" + uniqueColumns + ")" + " DO UPDATE SET " + updateClause);
	}
}
