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

package com.cgi.lino.connector.postgresql;

import java.sql.Types;

import org.hibernate.dialect.PostgreSQL10Dialect;

import com.vladmihalcea.hibernate.type.array.StringArrayType;

public class PostgreSQLCustomDialect extends PostgreSQL10Dialect {

	public PostgreSQLCustomDialect() {
		super();
		// Works with the sakila schema
		// https://github.com/fspacek/docker-postgres-sakila/blob/master/step_1.sql
		this.registerHibernateType(Types.ARRAY, StringArrayType.class.getName()); // special_features text[],
		this.registerHibernateType(Types.OTHER, String.class.getName()); // fulltext tsvector NOT NULL
	}

}