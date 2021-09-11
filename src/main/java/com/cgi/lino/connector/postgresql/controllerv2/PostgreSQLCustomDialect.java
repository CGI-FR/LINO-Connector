package com.cgi.lino.connector.postgresql.controllerv2;

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