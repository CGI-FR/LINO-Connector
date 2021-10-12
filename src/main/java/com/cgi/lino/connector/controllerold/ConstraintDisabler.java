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

package com.cgi.lino.connector.controllerold;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import javax.sql.DataSource;

public class ConstraintDisabler {

	private final Set<String> disabled = new HashSet<String>();

	private final DataSource datasource;

	public ConstraintDisabler(final DataSource datasource) {
		this.datasource = datasource;
	}

	public void disable(String tableName) throws SQLException {
		if (this.disabled.contains(tableName)) {
			return;
		}

		try (Connection connection = datasource.getConnection(); Statement statement = connection.createStatement()) {
			statement.executeUpdate("ALTER TABLE " + tableName + " DISABLE TRIGGER ALL");
		}

		this.disabled.add(tableName);
	}

	public void enable(String tableName) throws SQLException {
		if (!this.disabled.contains(tableName)) {
			return;
		}

		try (Connection connection = datasource.getConnection(); Statement statement = connection.createStatement()) {
			statement.executeUpdate("ALTER TABLE " + tableName + " ENABLE TRIGGER ALL");
		}

		this.disabled.remove(tableName);
	}

	public void enableAll() throws SQLException {
		for (Iterator<String> iterator = disabled.iterator(); iterator.hasNext();) {
			String tableName = iterator.next();
			this.enable(tableName);
		}
	}

}
