package com.cgi.lino.connector;

import java.sql.Connection;
import java.sql.SQLException;

import javax.sql.DataSource;

import org.springframework.beans.factory.annotation.Autowired;

public class DataUtils {

    public static void initTestDatabase(DataSource ds) throws SQLException {
        try (Connection conn = ds.getConnection()) {
            conn.createStatement().execute(
                    "create table myTable (bli VARCHAR(100) PRIMARY KEY, myString VARCHAR(100) NOT NULL, myAge INT);");
            conn.createStatement().execute(
                    "CREATE TABLE mySubTable (id INT PRIMARY KEY, blo VARCHAR(255),bli_reference VARCHAR(100) NOT NULL, FOREIGN KEY (bli_reference) REFERENCES myTable(bli));");
        }
    }

    public static void cleanDatabase(@Autowired DataSource ds) throws SQLException {
        try (Connection conn = ds.getConnection()) {
            conn.createStatement().execute("drop table mySubTable;");
            conn.createStatement().execute("drop table myTable;");
        }

    }

    public static void resetData(@Autowired DataSource ds) throws SQLException {
        try (Connection conn = ds.getConnection()) {
            conn.createStatement().execute("DELETE FROM mySubTable WHERE 1 = 1;");
            conn.createStatement().execute("DELETE FROM myTable WHERE 1 = 1;");
            conn.createStatement().execute(
                    "insert into myTable (bli, myString, myAge) values ('blo', 'isAString',1),('blio', 'isAString2',42);");
            conn.createStatement().execute(
                    "insert into mySubTable (id, blo, bli_reference) values (1,'bli', 'blo')");
        }
    }
}
