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
package com.cgi.lino.connector.controller.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.ValueNode;

import jakarta.persistence.EntityManagerFactory;
import jakarta.persistence.Query;
import jakarta.persistence.Tuple;
import lombok.Getter;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

import java.util.*;
import java.util.stream.Stream;
import java.sql.SQLException;
import java.text.ParseException;

import javax.sql.DataSource;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import com.cgi.lino.connector.controller.ws.dto.payload.Filter;
import com.cgi.lino.connector.controller.ws.dto.payload.PullOpenPayload;
import com.cgi.lino.connector.dao.TableAccessor;
import static com.cgi.lino.connector.controller.ws.dto.Messages.*;



@Service
@Slf4j
public class PullDatabaseService {


     @Value("#{systemProperties['insecure'] ?: 'false'}")
    String insecure;

    @Value("#{systemProperties['timeZone'] ?: 'UTC'}")
    String timeZone;

    private ObjectMapper mapper ;

    private  DataSource datasource;

    @Getter
    private  EntityManagerFactory entityManagerFactory;


    public  PullDatabaseService (final  DataSource datasource,final  EntityManagerFactory entityManagerFactory, final ObjectMapper mapper) {
        this.datasource = datasource;
        this.entityManagerFactory = entityManagerFactory;
        this.mapper= mapper;
    }

   
    public Stream<ObjectNode> pullData(@NonNull String cmdId,PullOpenPayload payload) throws SQLException, ParseException{
        return this.pullData(cmdId, payload.getSchema(), payload.getTable(), payload.getColumns(), payload.getFilter());
    }

    public Stream<ObjectNode> pullData(@NonNull String cmdId,String schemaName, String tableName, List<String> selectColumns, Filter filter) throws SQLException, ParseException{

            
            TableAccessor accessor = new TableAccessor(datasource, schemaName, tableName, Boolean.valueOf(this.insecure), this.timeZone);
            Integer limit = 0;
            String andWhere = null;
            Map<String,Object> values;
            Map<String,Object> whereColumns;
            if (filter != null) {
                Map<String, ValueNode> filterValues = (filter.getValues() != null) ? filter.getValues() : new HashMap<>();
                andWhere = filter.getWhere();
                values = accessor.cast(filterValues);
                whereColumns = accessor.cast(filterValues);
                limit = filter.getLimit();
            } else {
                values = new HashMap<>();
                whereColumns = new HashMap<>();
            }
            
        
            String querySql = accessor.getNativeQuerySelect(selectColumns, whereColumns.keySet(), andWhere, limit);
            log.info(INFO_MESSAGE, "20023", "Pull %s - %s".formatted(accessor.getTableNameFull(), querySql));
            Query query = entityManagerFactory.createEntityManager().createNativeQuery(querySql, Tuple.class);
            for (Map.Entry<String,Object> entry : values.entrySet()) {
                query.setParameter(entry.getKey(), entry.getValue());
            }
            for (Map.Entry<String,Object> entry : whereColumns.entrySet()) {
                query.setParameter(entry.getKey(), entry.getValue());
            }
            @SuppressWarnings("unchecked")            
            Stream<Tuple> result = query.getResultStream();
            return result.map(entry -> {
                ObjectNode obj = mapper.createObjectNode();
                entry.getElements().forEach(tupleElement -> {
                    obj.putPOJO(tupleElement.getAlias(), entry.get(tupleElement));
                });
                log.debug(INFO_MESSAGE, "10021", "emitting row: %s".formatted(obj.toPrettyString()));
                return obj;
            });
            
        }
    
  
}
