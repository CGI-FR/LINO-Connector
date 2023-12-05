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
import jakarta.persistence.Query;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cgi.lino.connector.dao.TableAccessor;

public class PusherTruncate extends PusherInsert {

	private static final Logger logger = LoggerFactory.getLogger(PusherTruncate.class);

	public PusherTruncate(final EntityManagerFactory entityManagerFactory, final TableAccessor accessor, final boolean disableConstraints) {
		super(entityManagerFactory, accessor, disableConstraints);
	}

	@Override
	public void open() {
		super.open();

		Query query = entityManager.createNativeQuery(accessor.getNativeQueryTruncate());
		query.executeUpdate();
		logger.info("  table " + accessor.getTableNameFull() + " truncate");
	}

}
