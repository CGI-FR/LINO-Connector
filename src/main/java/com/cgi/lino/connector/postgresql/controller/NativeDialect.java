package com.cgi.lino.connector.postgresql.controller;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

public interface NativeDialect {

	static final List<NativeDialect> DIALECTS = Arrays.asList((NativeDialect) new NativeDialectPostgres());

	/**
	 * Fetch the JDBCDialect class corresponding to a given database url.
	 */
	public static Optional<NativeDialect> get(String url) {
		for (NativeDialect dialect : DIALECTS) {
			if (dialect.canHandle(url)) {
				return Optional.of(dialect);
			}
		}
		return Optional.empty();
	}

	/**
	 * Check if this dialect instance can handle a certain jdbc url.
	 * 
	 * @param url the jdbc url.
	 * @return True if the dialect can be applied on the given jdbc url.
	 */
	boolean canHandle(String url);

	/**
	 * Quotes the identifier. This is used to put quotes around the identifier in
	 * case the column name is a reserved keyword, or in case it contains characters
	 * that require quotes (e.g. space). Default using double quotes {@code "} to
	 * quote.
	 */
	default String quoteIdentifier(String identifier) {
		return "\"" + identifier + "\"";
	}

	/**
	 * Quotes the identifier. This is used to put quotes around the identifier in
	 * case the column name is a reserved keyword, or in case it contains characters
	 * that require quotes (e.g. space). Default using double quotes {@code "} to
	 * quote.
	 */
	default String quoteIdentifier(String schema, String identifier) {
		return schema != null ? quoteIdentifier(schema) + "." + quoteIdentifier(identifier) : quoteIdentifier(identifier);
	}

	/**
	 * Generate a limit clause.
	 */
	default String getLimitClause(int limit) {
		return "LIMIT " + limit;
	}

	/**
	 * Get dialect upsert statement, the database has its own upsert syntax, such as
	 * Mysql using DUPLICATE KEY UPDATE, and PostgresSQL using ON CONFLICT... DO
	 * UPDATE SET..
	 *
	 * @return None if dialect does not support upsert statement, the writer will
	 *         degrade to the use of select + update/insert, this performance is
	 *         poor.
	 */
	default Optional<String> getUpsertStatement(String schemaName, String tableName, String[] fieldNames, String[] uniqueKeyFields) {
		return Optional.empty();
	}

	/**
	 * Get row exists statement by condition fields. Default use SELECT.
	 */
	default String getRowExistsStatement(String schemaName, String tableName, String[] conditionFields) {
		String fieldExpressions = Arrays.stream(conditionFields).map(f -> quoteIdentifier(f) + "=?").collect(Collectors.joining(" AND "));
		return "SELECT 1 FROM " + quoteIdentifier(schemaName, tableName) + " WHERE " + fieldExpressions;
	}

	/**
	 * Get insert into statement.
	 */
	default String getInsertIntoStatement(String schemaName, String tableName, String[] fieldNames) {
		String columns = Arrays.stream(fieldNames).map(this::quoteIdentifier).collect(Collectors.joining(", "));
		String placeholders = Arrays.stream(fieldNames).map(f -> "?").collect(Collectors.joining(", "));
		return "INSERT INTO " + quoteIdentifier(schemaName, tableName) + "(" + columns + ")" + " VALUES (" + placeholders + ")";
	}

	/**
	 * Get update one row statement by condition fields, default not use limit 1,
	 * because limit 1 is a sql dialect.
	 */
	default String getUpdateStatement(String schemaName, String tableName, String[] fieldNames, String[] conditionFields) {
		String setClause = Arrays.stream(fieldNames).map(f -> quoteIdentifier(f) + "=?").collect(Collectors.joining(", "));
		String conditionClause = Arrays.stream(conditionFields).map(f -> quoteIdentifier(f) + "=?").collect(Collectors.joining(" AND "));
		return "UPDATE " + quoteIdentifier(schemaName, tableName) + " SET " + setClause + " WHERE " + conditionClause;
	}

	/**
	 * Get delete one row statement by condition fields, default not use limit 1,
	 * because limit 1 is a sql dialect.
	 */
	default String getDeleteStatement(String schemaName, String tableName, String[] conditionFields) {
		String conditionClause = Arrays.stream(conditionFields).map(f -> quoteIdentifier(f) + "=?").collect(Collectors.joining(" AND "));
		return "DELETE FROM " + quoteIdentifier(schemaName, tableName) + " WHERE " + conditionClause;
	}

	/**
	 * Get select fields statement by condition fields. Default use SELECT.
	 */
	default String getSelectFromStatement(String schemaName, String tableName, String[] selectFields, String[] conditionFields, String additionalCondition, int limit) {
		String selectExpressions = Arrays.stream(selectFields).map(this::quoteIdentifier).collect(Collectors.joining(", "));
		String fieldExpressions = Arrays.stream(conditionFields).map(f -> quoteIdentifier(f) + "=?").collect(Collectors.joining(" AND "));
		String limitExpression = (limit > 0) ? " " + this.getLimitClause(limit) : "";
		String additionalWhereKeyword = conditionFields.length > 0 ? " AND " : " WHERE ";
		return "SELECT " + selectExpressions + " FROM " + quoteIdentifier(schemaName, tableName) + (conditionFields.length > 0 ? " WHERE " + fieldExpressions : "")
				+ (additionalCondition != null && !additionalCondition.isBlank() ? additionalWhereKeyword + additionalCondition : "") + limitExpression;
	}

	default String getTruncateStatement(String schemaName, String tableName) {
		return "TRUNCATE " + quoteIdentifier(schemaName, tableName) + " CASCADE";
	}

	default String getDisableConstraintsStatement(String schemaName, String tableName) {
		return "ALTER TABLE " + quoteIdentifier(schemaName, tableName) + " DISABLE TRIGGER ALL";
	}

	default String getEnableConstraintsStatement(String schemaName, String tableName) {
		return "ALTER TABLE " + quoteIdentifier(schemaName, tableName) + " ENABLE TRIGGER ALL";
	}

}
