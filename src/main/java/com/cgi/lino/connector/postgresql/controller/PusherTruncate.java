package com.cgi.lino.connector.postgresql.controller;

import javax.persistence.EntityManagerFactory;
import javax.persistence.Query;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PusherTruncate extends PusherInsert {

	private static final Logger logger = LoggerFactory.getLogger(PusherTruncate.class);

	public PusherTruncate(final EntityManagerFactory entityManagerFactory, final TableAccessor accessor, final boolean disableConstraints) {
		super(entityManagerFactory, accessor, disableConstraints);
	}

	@Override
	public void open() {
		super.open();

		Query query = entityManager.createNativeQuery(accessor.getNativeQueryTruncate());
		query.executeUpdate();
		logger.info("  table " + accessor.getTableNameFull() + " truncate");
	}

}
