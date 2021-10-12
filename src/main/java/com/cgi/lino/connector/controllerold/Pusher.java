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

import javax.sql.DataSource;

import com.fasterxml.jackson.databind.ObjectMapper;

public interface Pusher {

	static Pusher create(DataSource datasource, ObjectMapper mapper, String mode, String schema) {
		switch (mode) {
		case "insert":
			return new PushInsert(datasource, mapper);
		case "update":
			return new PushUpdate(datasource, mapper, schema);
		case "delete":
			return new PushDelete(datasource, mapper, schema);
		case "truncate":
			return new PushTruncate(datasource, mapper);
		default:
			throw new UnsupportedOperationException("unknown push mode: " + mode);
		}
	}

	void push(String jsonline, String tableName) throws IOException, SQLException;

}
