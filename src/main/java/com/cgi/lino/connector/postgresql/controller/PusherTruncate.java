package com.cgi.lino.connector.postgresql.controller;

import javax.persistence.EntityManager;
import javax.persistence.Query;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PusherTruncate extends PusherInsert {

	private static final Logger logger = LoggerFactory.getLogger(PusherTruncate.class);

	public PusherTruncate(final EntityManager entityManager, final TableAccessor accessor, final boolean disableConstraints) {
		super(entityManager, accessor, disableConstraints);

		Query query = entityManager.createNativeQuery(accessor.getNativeQueryTruncate());

		query.executeUpdate();
		logger.info("  table " + accessor.getTableNameFull() + " truncate");
	}

}
