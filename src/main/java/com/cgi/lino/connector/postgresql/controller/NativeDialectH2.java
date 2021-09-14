package com.cgi.lino.connector.postgresql.controller;

public class NativeDialectH2 implements NativeDialect {

	@Override
	public boolean canHandle(String url) {
		return url.startsWith("jdbc:h2:");
	}

	@Override
	public String getTruncateStatement(String schemaName, String tableName) {
		return "TRUNCATE TABLE " + quoteIdentifier(schemaName, tableName);
	}

	@Override
	public String getDisableConstraintsStatement(String schemaName, String tableName) {
		// Not supported
		return "CREATE TABLE IF NOT EXISTS " + quoteIdentifier(schemaName, tableName);
	}

	@Override
	public String getEnableConstraintsStatement(String schemaName, String tableName) {
		// Not supported
		return "CREATE TABLE IF NOT EXISTS " + quoteIdentifier(schemaName, tableName);
	}
}
