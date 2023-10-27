package com.cgi.lino.connector.controller.service;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;

import javax.sql.DataSource;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import com.cgi.lino.connector.controller.exceptions.RemoteException;
import com.cgi.lino.connector.dao.TableAccessor;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import lombok.NonNull;

@Service
public class InfoDatabaseService {
    
    private ObjectMapper mapper;

    private DataSource datasource;

    public InfoDatabaseService(final DataSource datasource, final ObjectMapper mapper){
        this.datasource = datasource;
        this.mapper = mapper;
    }

    
    @Value("#{systemProperties['insecure'] ?: 'false'}")
    String insecure;

    @Value("#{systemProperties['timeZone'] ?: 'UTC'}")
    String timeZone;
    

    public ObjectNode getDatabaseInformation(@NonNull String cmdId)  {
        ObjectNode result = mapper.createObjectNode();
        try (Connection connection = datasource.getConnection()) {
            DatabaseMetaData databaseMetaData = connection.getMetaData();
            ObjectNode database = mapper.createObjectNode();
            database.put("product", databaseMetaData.getDatabaseProductName());
            database.put("version", databaseMetaData.getDatabaseProductVersion());
            result.set("database", database);
            ObjectNode driver = mapper.createObjectNode();
            driver.put("name", databaseMetaData.getDriverName());
            driver.put("version", databaseMetaData.getDriverVersion());
            result.set("driver", driver);
            ObjectNode jdbc = mapper.createObjectNode();
            jdbc.put("version", databaseMetaData.getJDBCMajorVersion() + databaseMetaData.getJDBCMinorVersion());
            result.set("jdbc", jdbc);
        } catch(SQLException e) {
            throw new RemoteException(cmdId, e.getMessage(), e);
        }
        return result;
    }

    public ObjectNode getSchemas(@NonNull String cmdId)  {
        ObjectNode result = mapper.createObjectNode();

        try (Connection connection = datasource.getConnection()) {
            DatabaseMetaData databaseMetaData = connection.getMetaData();

            ArrayNode schemas = mapper.createArrayNode();
            try (ResultSet schemasrs = databaseMetaData.getSchemas()) {
                while (schemasrs.next()) {
                    String table_schem = schemasrs.getString("TABLE_SCHEM");
                    String table_catalog = schemasrs.getString("TABLE_CATALOG");
                    ObjectNode schema = mapper.createObjectNode();
                    schema.put("name", table_schem);
                    schema.put("catalog", table_catalog);
                    schemas.add(schema);
                }
            }
            result.set("schemas", schemas);
        } catch(SQLException e) {
            throw new RemoteException(cmdId, e.getMessage(), e);
        }
        return result;
    }

    public ArrayNode getTables(@NonNull String cmdId,String schema){
        ArrayNode result = mapper.createArrayNode();
        try (Connection connection = datasource.getConnection()) {
            DatabaseMetaData databaseMetaData = connection.getMetaData();
            try (ResultSet resultSet = databaseMetaData.getTables(null, schema, null, new String[] { "TABLE" })) {
                while (resultSet.next()) {
                    String tableName = resultSet.getString("TABLE_NAME");
                    ObjectNode table = mapper.createObjectNode();
                    table.put("name",tableName);
                    TableAccessor accessor = new TableAccessor(datasource, schema,tableName, Boolean.valueOf(this.insecure),this.timeZone);
                    TableAccessor.TableDescriptor descriptor = accessor.getDescriptor();
                    ArrayNode keys = table.putArray("keys");
                    descriptor.keys().forEach(key-> keys.add(key));
                    result.add(table);
                }
            }
        } catch(SQLException e) {
            throw new RemoteException(cmdId, e.getMessage(), e);
        }
        return result;
    }

    public ArrayNode getRelations(@NonNull String cmdId, String schema)  {
        ArrayNode result = mapper.createArrayNode();
        try (Connection connection = datasource.getConnection()) {
            DatabaseMetaData databaseMetaData = connection.getMetaData();

            try (ResultSet resultSet = databaseMetaData.getTables(null, schema, null, new String[] { "TABLE" })) {
                while (resultSet.next()) {
                    String tableName = resultSet.getString("TABLE_NAME");
                    try (ResultSet fkSet = databaseMetaData.getImportedKeys(null, schema, tableName)) {
                        while (fkSet.next()) {
                            String fkName = fkSet.getString("FK_NAME");
                            ObjectNode foreignKey = mapper.createObjectNode();
                            foreignKey.put("name",fkName);
                            // parent table
                            foreignKey.putObject("parent");
                            ObjectNode parent =(ObjectNode)foreignKey.get("parent");
                            parent.put("name",fkSet.getString("PKTABLE_NAME"));
                            parent.putArray("keys");
                            ((ArrayNode)parent.get("keys")).add(fkSet.getString("PKCOLUMN_NAME"));

                            // child table
                            foreignKey.putObject("child");
                            ObjectNode child =(ObjectNode)foreignKey.get("child");
                            child.put("name",tableName);
                            child.putArray("keys");
                            ((ArrayNode)child.get("keys")).add(fkSet.getString("FKCOLUMN_NAME"));
                           result.add(foreignKey);
                        }
                    }
                }
            }
        } catch(SQLException e) {
            throw new RemoteException(cmdId, e.getMessage(), e);
        }
        return result;
    }

}
