package com.cgi.lino.connector;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import javax.sql.DataSource;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;



public class DataUtils {

    public static void initTestDatabase(DataSource ds) throws SQLException {
        try (Connection conn = ds.getConnection()) {
            conn.createStatement().execute(
                    "create table myTable (bli VARCHAR(100) PRIMARY KEY, myString VARCHAR(100) NOT NULL, myAge INT);");
            conn.createStatement().execute(
                    "CREATE TABLE mySubTable (id INT PRIMARY KEY, blo VARCHAR(255),bli_reference VARCHAR(100) NOT NULL, FOREIGN KEY (bli_reference) REFERENCES myTable(bli));");
        }
    }

    public static void cleanDatabase(DataSource ds) throws SQLException {
        try (Connection conn = ds.getConnection()) {
            conn.createStatement().execute("drop table mySubTable;");
            conn.createStatement().execute("drop table myTable;");
        }

    }

    public static void resetData(DataSource ds) throws SQLException {
        try (Connection conn = ds.getConnection()) {
            conn.createStatement().execute("DELETE FROM mySubTable WHERE 1 = 1;");
            conn.createStatement().execute("DELETE FROM myTable WHERE 1 = 1;");
            conn.createStatement().execute(
                    "insert into myTable (bli, myString, myAge) values ('blo', 'isAString',1),('blio', 'isAString2',42);");
            conn.createStatement().execute(
                    "insert into mySubTable (id, blo, bli_reference) values (1,'bli', 'blo')");
        }
    }

    public static JSONArray getData(DataSource ds, String schema, String table) throws SQLException {
        try (Connection conn = ds.getConnection()) {
            String from = (schema != null) ? schema + "." + table : table;
            return rs2Json(conn.createStatement().executeQuery("select * from %s;".formatted(from)));
        }
    }

    private static JSONArray rs2Json(ResultSet resultSet) throws SQLException {
        ResultSetMetaData md = resultSet.getMetaData();
        int numCols = md.getColumnCount();
        List<String> colNames = IntStream.range(0, numCols)
                .mapToObj(i -> {
                    try {
                        return md.getColumnName(i + 1);
                    } catch (SQLException e) {
                        e.printStackTrace();
                        return "?";
                    }
                })
                .collect(Collectors.toList());

        JSONArray result = new JSONArray();
        while (resultSet.next()) {
            JSONObject row = new JSONObject();
            colNames.forEach(cn -> {
                try {
                    row.put(cn, resultSet.getObject(cn));
                } catch (JSONException | SQLException e) {
                    e.printStackTrace();
                }
            });
            result.put(row);
        }
        return result;
    }
}
