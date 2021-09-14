package com.cgi.lino.connector.postgresql.controller;

import javax.persistence.EntityManagerFactory;

import org.springframework.stereotype.Component;

@Component
public class PusherFactory {

	private final EntityManagerFactory entityManagerFactory;

	public PusherFactory(final EntityManagerFactory entityManagerFactory) {
		this.entityManagerFactory = entityManagerFactory;
	}

	public Pusher create(String mode, boolean disableConstraints, TableAccessor accessor) {
		switch (mode) {
		case "insert":
			return new PusherInsert(entityManagerFactory, accessor, disableConstraints);
		case "update":
			return new PusherUpdate(entityManagerFactory, accessor, disableConstraints);
		case "delete":
			return new PusherDelete(entityManagerFactory, accessor, disableConstraints);
		case "truncate":
			return new PusherTruncate(entityManagerFactory, accessor, disableConstraints);
		default:
			throw new UnsupportedOperationException("unknown push mode: " + mode);
		}
	}

}
