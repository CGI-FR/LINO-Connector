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

package com.cgi.lino.connector.controller.service.rest;

import jakarta.persistence.EntityManagerFactory;
import lombok.extern.slf4j.Slf4j;

import java.sql.SQLException;

import javax.sql.DataSource;

import org.springframework.stereotype.Component;

import com.cgi.lino.connector.dao.TableAccessor;

@Component
@Slf4j
public class PusherFactory {

	private final EntityManagerFactory entityManagerFactory;
	private DataSource datasource;

	public PusherFactory(final DataSource datasource, final EntityManagerFactory entityManagerFactory) {
		this.entityManagerFactory = entityManagerFactory;
		this.datasource= datasource;
	}

	public Pusher create(String schema, String tableName, String mode, boolean disableConstraints) throws SQLException {
		TableAccessor accessor = new TableAccessor(datasource, schema, tableName);

		log.info("Push " + accessor.getTableNameFull() + " - mode=" + mode + " disableConstraints="
				+ disableConstraints);
		switch (mode) {
		case "insert":
			return new PusherInsert(entityManagerFactory, accessor, disableConstraints);
		case "update":
			return new PusherUpdate(entityManagerFactory, accessor, disableConstraints);
		case "delete":
			return new PusherDelete(entityManagerFactory, accessor, disableConstraints);
		case "truncate":
			return new PusherTruncate(entityManagerFactory, accessor, disableConstraints);
		default:
			throw new UnsupportedOperationException("unknown push mode: " + mode);
		}
	}

}
