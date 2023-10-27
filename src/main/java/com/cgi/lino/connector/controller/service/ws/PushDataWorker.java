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
package com.cgi.lino.connector.controller.service.ws;

import com.fasterxml.jackson.databind.node.ValueNode;

import jakarta.persistence.EntityManager;
import jakarta.persistence.Query;
import jakarta.transaction.HeuristicMixedException;
import jakarta.transaction.HeuristicRollbackException;
import jakarta.transaction.NotSupportedException;
import jakarta.transaction.SystemException;
import lombok.Getter;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

import java.util.*;
import java.util.stream.Collectors;

import java.sql.SQLException;
import java.text.ParseException;

import javax.sql.DataSource;

import com.cgi.lino.connector.controller.exceptions.RemoteException;
import com.cgi.lino.connector.controller.ws.dto.PushMode;
import com.cgi.lino.connector.dao.TableAccessor;
import static com.cgi.lino.connector.controller.ws.dto.PushMode.*;

@Getter
@Slf4j
public class PushDataWorker {

    /**
     * mode :
     * -truncate implique insert
     * -delete implique push delete
     * -insert -> si existe et viole contrainte unicité alors erreur
     * -update -> si n'existe pas alors erreur
     */
    private final PushMode mode;
    private final Map<String, TableAccessor> tables;

    private final boolean disabledConstraints;

    private final String commandeId;
    private final String timeZone;

    private Query query = null;
    EntityManager em = null;

    public PushDataWorker(@NonNull String commandeId, @NonNull DataSource client, @NonNull EntityManager em,
            String schema, @NonNull List<String> tables, PushMode mode, boolean disabledConstraints, String timeZone)
            throws SystemException, NotSupportedException {

        this.em = em;
        this.em.getTransaction().begin();
        this.timeZone = timeZone;
        this.commandeId = commandeId;
        this.disabledConstraints = disabledConstraints;
        this.mode = mode;
        // begin transaction
        this.tables = tables.stream().collect(Collectors.toMap(tableName -> tableName, tableName -> {
            try {
                return new TableAccessor(client, schema, tableName, false, timeZone);
            } catch (SQLException e) {
                throw new RemoteException(this.commandeId, e);
            }
        }));

        // disable constraint
        if (this.disabledConstraints) {
            this.tables.forEach((tableName, tableAccessor) -> this
                    .executeTableQuery(tableAccessor.getNativeQueryDisableContraints()));
        }
        // truncate table
        if (TRUNCATE.equals(this.mode)) {
            this.tables.forEach(
                    (tableName, tableAccessor) -> this.executeTableQuery(tableAccessor.getNativeQueryTruncate()));
        }
    }

    /**
     * This method aims to execute a query targeting a table (drop, truncate,...)
     *
     * @param query
     */
    private void executeTableQuery(String query) {
        Query nativeQuery = em.createNativeQuery(query);
        nativeQuery.executeUpdate();
    }

    public void commit() throws HeuristicRollbackException, SystemException, HeuristicMixedException, NotSupportedException {
        // commit all changes
        this.em.joinTransaction();
        this.em.getTransaction().commit();
        this.em.getTransaction().begin();;
    }

    public void close() throws HeuristicRollbackException, SystemException, HeuristicMixedException, NotSupportedException {
        this.em.joinTransaction();
        if (this.em.getTransaction().isActive()) {
            this.em.getTransaction().commit();
        }
        // enable constraints
        if (disabledConstraints) {
            this.em.getTransaction().begin();
            this.tables.forEach((tableName, tableAccessor) -> this
                    .executeTableQuery(tableAccessor.getNativeQueryEnableContraints()));
            this.em.getTransaction().commit();
        }
        if (this.em.getTransaction().isActive()) {
            this.em.close();
        }
        this.query = null;
    }

    public int pushData(String tableName, Map<String, ValueNode> fieldNames, Map<String, ValueNode> whereFieldNames)
            throws ParseException, SystemException {
        this.em.joinTransaction();
        TableAccessor accessor = tables.get(tableName);
        if (this.query == null) {
            // if query is null, then create it
            this.query = createQuery(tableName, fieldNames, whereFieldNames);
        }
        this.setParameters(accessor, query, fieldNames);
        this.setParameters(accessor, query, whereFieldNames);

        return this.query.executeUpdate();

    }

    private Query setParameters(TableAccessor accessor, Query query, Map<String, ValueNode> values)
            throws ParseException {
        if (values != null) {
            accessor.cast(values).forEach((key, value) -> query.setParameter(key, value));
        }
        return query;
    }

    private Query createQuery(String tableName, Map<String, ValueNode> fieldNames,
            Map<String, ValueNode> whereFieldNames) {
        String query = switch (this.mode) {
            case TRUNCATE, INSERT -> this.tables.get(tableName).getNativeQueryInsert(fieldNames.keySet());
            case DELETE -> this.tables.get(tableName).getNativeQueryDelete(whereFieldNames.keySet());
            case UPDATE ->
                this.tables.get(tableName).getNativeQueryUpdate(fieldNames.keySet(), whereFieldNames.keySet());
        };
        log.info("-> createQuery: %s", query);
        return this.em.createNativeQuery(query);
    }

}
