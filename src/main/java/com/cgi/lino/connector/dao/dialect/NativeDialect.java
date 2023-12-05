// Copyright (C) 2021 CGI France
//
// This file is part of LINO.
//
// LINO is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// LINO is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with LINO.  If not, see <http://www.gnu.org/licenses/>.

package com.cgi.lino.connector.dao.dialect;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

public interface NativeDialect {

    static final List<NativeDialect> DIALECTS = Arrays.asList((NativeDialect)new NativeDialectH2(), new NativeDialectDB2(), new NativeDialectPostgres(), new NativeDialectDefault());

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
        return """
            "%s" """.formatted(identifier);
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
        return "LIMIT %d".formatted(limit);
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
    default Optional<String> getUpsertStatement(String schemaName, String tableName, List<String> fieldNames, List<String> uniqueKeyFields) {
        return Optional.empty();
    }

    /**
     * Get row exists statement by condition fields. Default use SELECT.
     */
    default String getRowExistsStatement(String schemaName, String tableName, List<String> conditionFields) {
        String fieldExpressions = conditionFields.stream().map(f -> quoteIdentifier(f) + "=:"+f).collect(Collectors.joining(" AND "));
        return "SELECT 1 FROM %s WHERE %s".formatted(quoteIdentifier(schemaName, tableName),fieldExpressions);
    }

    /**
     * Get insert into statement.
     */
    default String getInsertIntoStatement(String schemaName, String tableName, List<String> fieldNames) {
        String columns = fieldNames.stream().map(this::quoteIdentifier).collect(Collectors.joining(", "));
        String placeholders = fieldNames.stream().map(f -> ":"+f).collect(Collectors.joining(", "));
        return "INSERT INTO %s(%s) VALUES (%s)".formatted(quoteIdentifier(schemaName, tableName),columns,placeholders);
    }

    /**
     * Get update one row statement by condition fields, default not use limit 1,
     * because limit 1 is a sql dialect.
     */
    default String getUpdateStatement(String schemaName, String tableName, List<String> fieldNames, List<String> conditionFields) {
        String setClause = fieldNames.stream().map(f -> quoteIdentifier(f) + "=:"+f).collect(Collectors.joining(", "));
        String conditionClause = (conditionFields!=null && !conditionFields.isEmpty())?" WHERE %s".formatted(conditionFields.stream().map(f -> quoteIdentifier(f) + "=:"+f).collect(Collectors.joining(" AND "))):"";
        return "UPDATE %s SET %s%s".formatted(quoteIdentifier(schemaName, tableName),setClause,conditionClause);
    }

    /**
     * Get delete one row statement by condition fields, default not use limit 1,
     * because limit 1 is a sql dialect.
     */
    default String getDeleteStatement(String schemaName, String tableName, List<String> conditionFields) {
        String conditionClause = conditionFields.stream().map(f -> quoteIdentifier(f) + "=:"+f).collect(Collectors.joining(" AND "));
        return "DELETE FROM %s WHERE %s ".formatted(quoteIdentifier(schemaName, tableName),conditionClause);
    }

    /**
     * Get select fields statement by condition fields. Default use SELECT.
     */
    default String getSelectFromStatement(String schemaName, String tableName, List<String> selectFields, List<String> conditionFields, String additionalCondition, Integer limit, boolean insecure) {
        String selectExpressions = (selectFields==null|| selectFields.isEmpty())?"*":selectFields.stream().map(this::quoteIdentifier).collect(Collectors.joining(", "));
        String tableExpression =  quoteIdentifier(schemaName, tableName);
        String fieldExpressions = (conditionFields==null|| conditionFields.isEmpty())?"":" WHERE "+conditionFields.stream().map(f -> quoteIdentifier(f) + "=:"+f).collect(Collectors.joining(" AND "));
        String limitExpression = (limit!=null && limit > 0) ? " " + this.getLimitClause(limit) : "";
        String additionalWhereKeyword =  (conditionFields==null || conditionFields.isEmpty())?  " WHERE " : " AND ";
        String additionalExpression =  (insecure && additionalCondition != null && !additionalCondition.isBlank()) ? additionalWhereKeyword + additionalCondition : "";
        return "SELECT %s FROM %s%s%s%s".formatted(selectExpressions,tableExpression, fieldExpressions, additionalExpression,limitExpression);

    }

    default String getTruncateStatement(String schemaName, String tableName) {
        return "TRUNCATE TABLE %s CASCADE".formatted(quoteIdentifier(schemaName, tableName));
    }


    default String getDisableConstraintsStatement(String schemaName, String tableName) {
        return "ALTER TABLE %s DISABLE TRIGGER ALL".formatted(quoteIdentifier(schemaName, tableName));
    }


    default String getEnableConstraintsStatement(String schemaName, String tableName) {
        return "ALTER TABLE %s ENABLE TRIGGER ALL".formatted(quoteIdentifier(schemaName, tableName));
    }


}
