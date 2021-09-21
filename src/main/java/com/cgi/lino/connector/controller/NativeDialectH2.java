package com.cgi.lino.connector.controller;

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
		return "ALTER TABLE " + quoteIdentifier(schemaName, tableName) + " SET REFERENTIAL_INTEGRITY FALSE";
	}

	@Override
	public String getEnableConstraintsStatement(String schemaName, String tableName) {
		return "ALTER TABLE " + quoteIdentifier(schemaName, tableName) + " SET REFERENTIAL_INTEGRITY TRUE";
	}
}
