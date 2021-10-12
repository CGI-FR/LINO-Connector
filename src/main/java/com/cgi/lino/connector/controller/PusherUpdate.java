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

package com.cgi.lino.connector.controller;

import java.text.ParseException;
import java.util.Map;

import javax.persistence.EntityManagerFactory;
import javax.persistence.Query;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PusherUpdate extends Pusher {

	private static final Logger logger = LoggerFactory.getLogger(PusherUpdate.class);

	public PusherUpdate(final EntityManagerFactory entityManagerFactory, final TableAccessor accessor, final boolean disableConstraints) {
		super(entityManagerFactory, accessor, disableConstraints);
	}

	@Override
	public void push(Map<String, Object> object) throws ParseException {
		Map<String, Object> values = accessor.removePrimaryKeys(object);
		Map<String, Object> where = accessor.keepPrimaryKeysOnly(object);

		Query query = entityManager.createNativeQuery(accessor.getNativeQueryUpdate(values.keySet(), where.keySet()));

		int position = 0;
		for (Object value : accessor.cast(values)) {
			query.setParameter(++position, value);
		}
		for (Object value : accessor.cast(where)) {
			query.setParameter(++position, value);
		}

		int nb = query.executeUpdate();
		logger.info("  " + nb + " row(s) update");

	}

}
