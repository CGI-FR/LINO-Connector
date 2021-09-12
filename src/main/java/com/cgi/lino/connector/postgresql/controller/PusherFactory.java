package com.cgi.lino.connector.postgresql.controller;

import javax.persistence.EntityManager;

import org.springframework.stereotype.Component;

@Component
public class PusherFactory {

	private final EntityManager entityManager;

	public PusherFactory(final EntityManager entityManager) {
		this.entityManager = entityManager;
	}

	public Pusher create(String mode, boolean disableConstraints, TableAccessor accessor) {
		switch (mode) {
		case "insert":
			return new PusherInsert(entityManager, accessor, disableConstraints);
		case "update":
			return new PusherUpdate(entityManager, accessor, disableConstraints);
		case "delete":
			return new PusherDelete(entityManager, accessor, disableConstraints);
		case "truncate":
			return new PusherTruncate(entityManager, accessor, disableConstraints);
		default:
			throw new UnsupportedOperationException("unknown push mode: " + mode);
		}
	}

}
