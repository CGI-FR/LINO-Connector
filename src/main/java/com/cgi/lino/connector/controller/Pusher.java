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

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.Query;

public abstract class Pusher implements AutoCloseable {

	protected final EntityManager entityManager;

	protected final TableAccessor accessor;

	protected final boolean disableConstraints;

	public Pusher(final EntityManagerFactory entityManagerFactory, final TableAccessor accessor, final boolean disableConstraints) {
		this.entityManager = entityManagerFactory.createEntityManager();
		this.accessor = accessor;
		this.disableConstraints = disableConstraints;
	}

	public abstract void push(Map<String, Object> object) throws ParseException;

	public void open() {
		this.entityManager.getTransaction().begin();
		if (this.disableConstraints) {
			Query query = entityManager.createNativeQuery(accessor.getNativeQueryDisableContraints());
			query.executeUpdate();
		}
	}

	@Override
	public void close() {
		if (this.disableConstraints) {
			Query query = entityManager.createNativeQuery(accessor.getNativeQueryEnableContraints());
			query.executeUpdate();
		}
		this.entityManager.flush();
		this.entityManager.clear();
		this.entityManager.getTransaction().commit();
		this.entityManager.close();
	}

}
