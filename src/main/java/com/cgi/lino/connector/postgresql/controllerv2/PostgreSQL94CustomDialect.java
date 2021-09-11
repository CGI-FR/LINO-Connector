package com.cgi.lino.connector.postgresql.controllerv2;

import org.hibernate.dialect.PostgreSQL94Dialect;
import org.hibernate.type.BinaryType;

import com.vladmihalcea.hibernate.type.array.StringArrayType;

public class PostgreSQL94CustomDialect extends PostgreSQL94Dialect {

	public PostgreSQL94CustomDialect() {
		super();
		this.registerHibernateType(2003, StringArrayType.class.getName());
		this.registerHibernateType(1111, BinaryType.class.getName());
	}

}