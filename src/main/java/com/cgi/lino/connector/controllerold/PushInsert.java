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
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.sql.DataSource;

import org.springframework.jdbc.core.simple.SimpleJdbcInsert;

import com.fasterxml.jackson.databind.ObjectMapper;

public class PushInsert implements Pusher {

	protected final DataSource datasource;

	protected final ObjectMapper mapper;

	private final Pattern p = Pattern.compile("^(\\d{4}-\\d{2}-\\d{2})T\\d{2}:\\d{2}:\\d{2}(\\.\\d{3,}\\+\\d{2}:\\d{2})?Z?$");

	public PushInsert(final DataSource datasource, final ObjectMapper mapper) {
		this.datasource = datasource;
		this.mapper = mapper;
	}

	@Override
	public void push(String jsonline, String tableName) throws IOException, SQLException {
		@SuppressWarnings("unchecked")
		Map<String, Object> object = mapper.readValue(jsonline, HashMap.class);
		SimpleJdbcInsert insert = new SimpleJdbcInsert(datasource).withTableName(tableName);
		try {
			insert.execute(object);
		} catch (Exception ex) {
			if (ex.getMessage().contains("Bad value for type timestamp/date/time")) {
				var entrySet = object.entrySet();
				for (var iterator = entrySet.iterator(); iterator.hasNext();) {
					var entry = iterator.next();
					if (entry.getValue() instanceof String) {
						String value = (String) entry.getValue();
						Matcher m = p.matcher(value);
						if (m.matches()) {
							entry.setValue(m.group(1));
						}
					}
				}
				insert.execute(object);
			} else {
				throw ex;
			}
		}
	}

}
