package com.cgi.lino.connector.controller;

import java.text.ParseException;
import java.util.Map;

import javax.persistence.EntityManagerFactory;
import javax.persistence.Query;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PusherInsert extends Pusher {

	private static final Logger logger = LoggerFactory.getLogger(PusherInsert.class);

	public PusherInsert(final EntityManagerFactory entityManagerFactory, final TableAccessor accessor, final boolean disableConstraints) {
		super(entityManagerFactory, accessor, disableConstraints);
	}

	@Override
	public void push(Map<String, Object> object) throws ParseException {
		Query query = entityManager.createNativeQuery(accessor.getNativeQueryInsert(object.keySet()));

		int position = 0;
		for (Object value : accessor.cast(object)) {
			query.setParameter(++position, value);
		}

		int nb = query.executeUpdate();
		logger.info("  " + nb + " row(s) insert");
	}

}
