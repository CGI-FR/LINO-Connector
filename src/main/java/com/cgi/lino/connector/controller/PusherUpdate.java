package com.cgi.lino.connector.controller;

import java.text.ParseException;
import java.util.Map;

import javax.persistence.EntityManagerFactory;
import javax.persistence.Query;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PusherUpdate extends Pusher {

	private static final Logger logger = LoggerFactory.getLogger(PusherUpdate.class);

	public PusherUpdate(final EntityManagerFactory entityManagerFactory, final TableAccessor accessor, final boolean disableConstraints) {
		super(entityManagerFactory, accessor, disableConstraints);
	}

	@Override
	public void push(Map<String, Object> object) throws ParseException {
		Map<String, Object> values = accessor.removePrimaryKeys(object);
		Map<String, Object> where = accessor.keepPrimaryKeysOnly(object);

		Query query = entityManager.createNativeQuery(accessor.getNativeQueryUpdate(values.keySet(), where.keySet()));

		int position = 0;
		for (Object value : accessor.cast(values)) {
			query.setParameter(++position, value);
		}
		for (Object value : accessor.cast(where)) {
			query.setParameter(++position, value);
		}

		int nb = query.executeUpdate();
		logger.info("  " + nb + " row(s) update");

	}

}
