package com.cgi.lino.connector.postgresql.controller;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.text.ParseException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import javax.sql.DataSource;

import org.apache.flink.api.java.io.jdbc.dialect.JDBCDialect;
import org.apache.flink.api.java.io.jdbc.dialect.JDBCDialects;

public class TableAccessor {

	private final DataSource datasource;
	private final JDBCDialect dialect;

	private final String schemaName;
	private final String tableName;
	private final Map<String, ColumnDescriptor> columnDescriptors = new HashMap<>();
	private final List<String> keys;
	private final List<String> columns = new ArrayList<>();

	public TableAccessor(final DataSource datasource, final String schemaName, final String tableName) throws SQLException {
		this.datasource = datasource;
		this.schemaName = schemaName;
		this.tableName = tableName;
		try (Connection connection = this.datasource.getConnection()) {
			DatabaseMetaData databaseMetaData = connection.getMetaData();
			this.dialect = JDBCDialects.get(databaseMetaData.getURL()).get();
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
				pkRs.last();
				this.keys = Arrays.asList(new String[pkRs.getRow()]);
				pkRs.beforeFirst();
				while (pkRs.next()) {
					String pkColumnName = pkRs.getString("COLUMN_NAME");
					short pkSeq = pkRs.getShort("KEY_SEQ");
					this.keys.set(pkSeq - 1, pkColumnName);
				}
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

	public Collection<Object> cast(Map<String, Object> columns) throws ParseException {
		Map<Integer, Object> result = new TreeMap<>();
		for (Map.Entry<String, Object> column : columns.entrySet()) {
			String columnName = column.getKey();
			ColumnDescriptor descriptor = this.columnDescriptors.get(columnName);
			int ordinate = this.columns.indexOf(columnName);
			switch (descriptor.getType()) {
			case Types.BIGINT:
				result.put(ordinate, Long.decode(column.getValue().toString()));
				break;
			case Types.INTEGER:
				result.put(ordinate, Integer.decode(column.getValue().toString()));
				break;
			case Types.SMALLINT:
				result.put(ordinate, Short.decode(column.getValue().toString()));
				break;
			case Types.TIMESTAMP:
				result.put(ordinate, LocalDateTime.parse(column.getValue().toString(), DateTimeFormatter.ISO_DATE_TIME));
				break;
			default:
				result.put(ordinate, column.getValue());
			}
		}
		return result.values();
	}

	public Map<String, Object> keepPrimaryKeysOnly(Map<String, Object> columns) {
		Map<String, Object> result = new HashMap<String, Object>();
		for (Map.Entry<String, Object> entry : columns.entrySet()) {
			if (this.keys.contains(entry.getKey())) {
				result.put(entry.getKey(), entry.getValue());
			}
		}
		return result;
	}

	public Map<String, Object> removePrimaryKeys(Map<String, Object> columns) {
		Map<String, Object> result = new HashMap<String, Object>();
		for (Map.Entry<String, Object> entry : columns.entrySet()) {
			if (!this.keys.contains(entry.getKey())) {
				result.put(entry.getKey(), entry.getValue());
			}
		}
		return result;
	}

	public String getNativeQueryInsert(Collection<String> fieldNames) {
		Map<Integer, String> fieldNamesOrdered = new TreeMap<>();
		for (String fieldName : fieldNames) {
			int ordinate = this.columns.indexOf(fieldName);
			fieldNamesOrdered.put(ordinate, fieldName);
		}
		return this.dialect.getInsertIntoStatement(this.getTableNameFull(), toArray(fieldNamesOrdered.values()));
	}

	public String getNativeQueryDelete(Collection<String> whereFieldNames) {
		Map<Integer, String> whereFieldNamesOrdered = new TreeMap<>();
		for (String whereFieldName : whereFieldNames) {
			int ordinate = this.columns.indexOf(whereFieldName);
			whereFieldNamesOrdered.put(ordinate, whereFieldName);
		}
		return this.dialect.getDeleteStatement(this.getTableNameFull(), toArray(whereFieldNamesOrdered.values()));
	}

	public String getNativeQueryUpdate(Collection<String> fieldNames, Collection<String> whereFieldNames) {
		Map<Integer, String> fieldNamesOrdered = new TreeMap<>();
		for (String fieldName : fieldNames) {
			int ordinate = this.columns.indexOf(fieldName);
			fieldNamesOrdered.put(ordinate, fieldName);
		}
		Map<Integer, String> whereFieldNamesOrdered = new TreeMap<>();
		for (String whereFieldName : whereFieldNames) {
			int ordinate = this.columns.indexOf(whereFieldName);
			whereFieldNamesOrdered.put(ordinate, whereFieldName);
		}
		return this.dialect.getUpdateStatement(this.getTableNameFull(), toArray(fieldNamesOrdered.values()), toArray(whereFieldNamesOrdered.values()));
	}

	public String getTableNameFull() {
		if (this.schemaName != null) {
			return this.schemaName + "." + this.tableName;
		}
		return this.tableName;
	}

	public static class ColumnDescriptor {

		private final String name;
		private final int type;
		private final String typeName;
		private final int size;

		public ColumnDescriptor(String name, int type, String typeName, int size) {
			super();
			this.name = name;
			this.type = type;
			this.typeName = typeName;
			this.size = size;
		}

		public String getName() {
			return name;
		}

		public int getType() {
			return type;
		}

		public String getTypeName() {
			return typeName;
		}

		public int getSize() {
			return size;
		}

		@Override
		public String toString() {
			StringBuilder sb = new StringBuilder();
			sb.append(this.getName());
			sb.append(" ");
			sb.append(this.getTypeName());
			sb.append("(");
			sb.append(this.getSize());
			sb.append(")");
			return sb.toString();
		}
	}

	private static String[] toArray(Collection<String> coll) {
		return coll.toArray(new String[coll.size()]);
	}

}