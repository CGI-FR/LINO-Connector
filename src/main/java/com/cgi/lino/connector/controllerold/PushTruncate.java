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

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashSet;
import java.util.Set;

import javax.sql.DataSource;

import com.fasterxml.jackson.databind.ObjectMapper;

public class PushTruncate extends PushInsert implements Pusher {

	private final Set<String> done = new HashSet<>();

	public PushTruncate(DataSource datasource, ObjectMapper mapper) {
		super(datasource, mapper);
	}

	@Override
	public void push(String jsonline, String tableName) throws IOException, SQLException {
		if (!done.contains(tableName)) {
			done.add(tableName);
			try (Connection connection = datasource.getConnection(); Statement statement = connection.createStatement()) {
				statement.executeUpdate("TRUNCATE " + tableName + " CASCADE");
			}
		}
		super.push(jsonline, tableName);
	}

}
