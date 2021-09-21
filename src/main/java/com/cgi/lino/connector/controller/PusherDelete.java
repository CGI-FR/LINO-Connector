package com.cgi.lino.connector.controller;

import java.text.ParseException;
import java.util.Map;

import javax.persistence.EntityManagerFactory;
import javax.persistence.Query;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PusherDelete extends Pusher {

	private static final Logger logger = LoggerFactory.getLogger(PusherDelete.class);

	public PusherDelete(final EntityManagerFactory entityManagerFactory, final TableAccessor accessor, final boolean disableConstraints) {
		super(entityManagerFactory, accessor, disableConstraints);
	}

	@Override
	public void push(Map<String, Object> object) throws ParseException {
		Map<String, Object> objectKeys = accessor.keepPrimaryKeysOnly(object);
		Query query = entityManager.createNativeQuery(accessor.getNativeQueryDelete(objectKeys.keySet()));

		int position = 0;
		for (Object value : accessor.cast(object)) {
			query.setParameter(++position, value);
		}

		int nb = query.executeUpdate();
		logger.info("  " + nb + " row(s) delete");
	}

}