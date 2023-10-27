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
package com.cgi.lino.connector.dao;

import com.cgi.lino.connector.dao.dialect.NativeDialect;
import com.fasterxml.jackson.databind.node.ValueNode;

import lombok.extern.slf4j.Slf4j;

import javax.sql.DataSource;
import java.sql.*;
import java.text.ParseException;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.format.SignStyle;
import java.util.*;

import static java.time.temporal.ChronoField.*;


@Slf4j
public class TableAccessor {

    private final NativeDialect dialect;

    private final String schemaName;
    private final String tableName;
    private final Map<String, ColumnDescriptor> columnDescriptors = new HashMap<>();
    private final List<String> keys;
    private final List<String> columns = new ArrayList<>();
    private final boolean insecure;
    private final String timeZone;

    private final static DateTimeFormatter FORMATTER = new DateTimeFormatterBuilder()
            .appendValue(YEAR, 4, 10, SignStyle.EXCEEDS_PAD)
            .appendLiteral('-')
            .appendValue(MONTH_OF_YEAR, 2)
            .appendLiteral('-')
            .appendValue(DAY_OF_MONTH, 2)
            .appendLiteral(' ')
            .appendValue(HOUR_OF_DAY, 2)
            .appendLiteral(':')
            .appendValue(MINUTE_OF_HOUR, 2)
            .appendLiteral(':')
            .appendValue(SECOND_OF_MINUTE, 2)
            .appendFraction(MILLI_OF_SECOND, 0, 3, true)
            .toFormatter();


    public TableAccessor(final DataSource datasource, final String schemaName, final String tableName) throws SQLException {
		this(datasource, schemaName,tableName, false,"UTC");
	}

    public TableAccessor(final DataSource datasource, final String schemaName, final String tableName, final boolean insecure, String timeZone) throws SQLException {
        this.schemaName = schemaName;
        this.tableName = tableName;
        this.insecure = insecure;
        this.timeZone = timeZone;
        try (Connection connection = datasource.getConnection()) {
            DatabaseMetaData databaseMetaData = connection.getMetaData();
            Optional<NativeDialect> dialectProvider=NativeDialect.get(databaseMetaData.getURL());
            if(dialectProvider.isEmpty()) {
                throw new RuntimeException("no dialect for %s".formatted(databaseMetaData.getURL()));
            }
            this.dialect = dialectProvider.get();
            log.info("loading dialect " + dialect.getClass().getName()  + " for " + databaseMetaData.getURL());

            try (ResultSet colRs = databaseMetaData.getColumns(null, schemaName, tableName, null)) {
                while (colRs.next()) {
                    String columnName = colRs.getString("COLUMN_NAME");
                    int columnType = colRs.getInt("DATA_TYPE");
                    String columnTypeName = colRs.getString("TYPE_NAME");
                    int columnSize = colRs.getInt("COLUMN_SIZE");
                    this.columnDescriptors.put(columnName, new ColumnDescriptor(columnName, columnType, columnTypeName, columnSize));
                    this.columns.add(columnName);
                }
            }
            try (ResultSet pkRs = databaseMetaData.getPrimaryKeys(null, schemaName, tableName)) {
                Map<Integer, String> orderedKeys = new TreeMap<>();
                while (pkRs.next()) {
                    String pkColumnName = pkRs.getString("COLUMN_NAME");
                    short pkSeq = pkRs.getShort("KEY_SEQ");
                    orderedKeys.put(pkSeq - 1, pkColumnName);
                }
                this.keys = new ArrayList<>(orderedKeys.values());
            }
        }
    }


    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        for (String columnName : this.columns) {
            sb.append(this.columnDescriptors.get(columnName).toString());
            if (this.keys.contains(columnName)) {
                sb.append(" primary_key");
            }
            sb.append(System.lineSeparator());
        }
        return sb.toString();
    }

    public Map<String,Object> cast(Map<String, ValueNode> columns) throws ParseException {
        Map<String, Object> result = new TreeMap<>();
        for (Map.Entry<String, ValueNode> column : columns.entrySet()) {
            String columnName = column.getKey();
            ColumnDescriptor descriptor = this.columnDescriptors.get(columnName);
            if (descriptor != null) {
                if(column.getValue() == null || "null".equals(column.getValue().asText())) {
                    result.put(columnName, null);
                } else {
                    switch (descriptor.type()) {
                        case Types.BIGINT,Types.DECIMAL               ->  result.put(columnName, column.getValue().asLong());
                        case Types.INTEGER              ->  result.put(columnName, column.getValue().asInt());
                        case Types.SMALLINT             ->  result.put(columnName, Short.decode(column.getValue().asText()));
                        case Types.TIMESTAMP, Types.TIMESTAMP_WITH_TIMEZONE          ->
                        {
                            String date;
                            if(column.getValue().canConvertToLong()){
                                date= FORMATTER.withZone(ZoneId.of(this.timeZone)).format(Instant.ofEpochMilli(column.getValue().asLong()).atZone(ZoneId.of("UTC")));
                            } else {
                                date = column.getValue().asText();
                            }
                            result.put(columnName, date);
                        }
                        case Types.TIME, Types.TIME_WITH_TIMEZONE         ->
                        {
                            String date;
                            if(column.getValue().canConvertToLong()){
                                date= DateTimeFormatter.ISO_DATE_TIME.withZone(ZoneId.of(this.timeZone)).format(Instant.ofEpochMilli(column.getValue().asLong()).atZone(ZoneId.of("UTC")));
                            } else {
                                date = column.getValue().asText();
                            }
                            result.put(columnName, date);
                        }

                        case Types.DATE                ->    {
                            String date;
                            if(column.getValue().canConvertToLong()){
                                date= DateTimeFormatter.ISO_LOCAL_DATE.withZone(ZoneId.of(this.timeZone)).format(Instant.ofEpochMilli(column.getValue().asLong()).atZone(ZoneId.of("UTC")));
                            } else {
                                date = column.getValue().asText();
                            }
                            result.put(columnName, date);
                        }


                        case Types.DOUBLE, Types.FLOAT,Types.NUMERIC  ->  result.put(columnName, column.getValue().asDouble());
                        default                         ->  result.put(columnName, column.getValue().asText());
                    }
                }
            } else {
                log.error("Accessor " + this.getTableNameFull() + " - unknown column name " + columnName);
            }
        }
        return result;
    }

    public String getNativeQuerySelect(List<String> fieldNames, Set<String> whereFieldNames, String andWhere, Integer limit) {
        if (fieldNames == null) {
            fieldNames = this.columns;
        }
        Map<Integer, String> fieldNamesOrdered = listToMapOredered(fieldNames);
        Map<Integer, String> whereFieldNamesOrdered = listToMapOredered(whereFieldNames);
        return this.dialect.getSelectFromStatement(this.schemaName, this.tableName, new ArrayList<>(fieldNamesOrdered.values()), new ArrayList<>(whereFieldNamesOrdered.values()), andWhere, limit, insecure);
    }

    public String getNativeQueryInsert(Collection<String> fieldNames) {
        if(fieldNames==null || fieldNames.isEmpty()){
            throw new IllegalArgumentException("for a truncate or insert mode, field names list must not be empty");
        }
        Map<Integer, String> fieldNamesOrdered = listToMapOredered(fieldNames);
        return this.dialect.getInsertIntoStatement(this.schemaName, this.tableName, new ArrayList<>(fieldNamesOrdered.values()));
    }

    public String getNativeQueryDelete(Collection<String>  whereFieldNames) {
        Map<Integer, String> whereFieldNamesOrdered = listToMapOredered(whereFieldNames);
        return this.dialect.getDeleteStatement(this.schemaName, this.tableName, new ArrayList<>(whereFieldNamesOrdered.values()));
    }

    public String getNativeQueryUpdate(Collection<String> fieldNames, Collection<String> whereFieldNames) {
        if(fieldNames==null || fieldNames.isEmpty()){
            throw new IllegalArgumentException("for a update mode, field names list must not be empty");
        }
        Map<Integer, String> fieldNamesOrdered = listToMapOredered(fieldNames);
        Map<Integer, String> whereFieldNamesOrdered = listToMapOredered(whereFieldNames) ;
        return this.dialect.getUpdateStatement(this.schemaName, this.tableName, new ArrayList<>(fieldNamesOrdered.values()), new ArrayList<>(whereFieldNamesOrdered.values()));
    }

    private Map<Integer,String> listToMapOredered(Collection<String> list){
        Map<Integer, String> itemOrdered = new TreeMap<>();
        list.forEach(item-> itemOrdered.put(this.columns.indexOf(item), item));
        return itemOrdered;
    }

    public String getNativeQueryTruncate() {
        return this.dialect.getTruncateStatement(this.schemaName, this.tableName);
    }

    public String getNativeQueryDisableContraints() {
        return this.dialect.getDisableConstraintsStatement(this.schemaName, this.tableName);
    }

    public String getNativeQueryEnableContraints() {
        return this.dialect.getEnableConstraintsStatement(this.schemaName, this.tableName);
    }

    public String getTableNameFull() {
        return (this.schemaName != null && ! this.schemaName.isEmpty())? this.schemaName + "." + this.tableName: this.tableName;
    }

    public TableDescriptor getDescriptor() {
        List<String> keys = new ArrayList<>();
        List<ColumnDescriptor> columns = new ArrayList<>();
        for (Map.Entry<String, ColumnDescriptor> column : columnDescriptors.entrySet()) {
            if (this.keys.contains(column.getKey())) {
                keys.add(column.getKey());
            }
            columns.add(column.getValue());
        }
        return new TableDescriptor(this.tableName, keys,columns);
    }

    public record ColumnDescriptor (String name, int type, String typeName, int size) {}

    public record TableDescriptor (String name, List<String> keys, List<ColumnDescriptor> columns) {}

}
