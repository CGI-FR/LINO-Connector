package com.cgi.lino.connector.postgresql.controllerv2;

import java.sql.Types;

import org.hibernate.dialect.PostgreSQL94Dialect;

import com.vladmihalcea.hibernate.type.array.StringArrayType;

public class PostgreSQL94CustomDialect extends PostgreSQL94Dialect {

	public PostgreSQL94CustomDialect() {
		super();
		// Works with the sakila schema
		// https://github.com/fspacek/docker-postgres-sakila/blob/master/step_1.sql
		this.registerHibernateType(Types.ARRAY, StringArrayType.class.getName()); // special_features text[],
		this.registerHibernateType(Types.OTHER, String.class.getName()); // fulltext tsvector NOT NULL
	}

}