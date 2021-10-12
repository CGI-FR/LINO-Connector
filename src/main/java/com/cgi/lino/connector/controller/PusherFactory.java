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

import javax.persistence.EntityManagerFactory;

import org.springframework.stereotype.Component;

@Component
public class PusherFactory {

	private final EntityManagerFactory entityManagerFactory;

	public PusherFactory(final EntityManagerFactory entityManagerFactory) {
		this.entityManagerFactory = entityManagerFactory;
	}

	public Pusher create(String mode, boolean disableConstraints, TableAccessor accessor) {
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
