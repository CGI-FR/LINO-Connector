package com.cgi.lino.connector.postgresql.controller;

import java.text.ParseException;
import java.util.Map;

import javax.persistence.EntityManager;

public abstract class Pusher implements AutoCloseable {

	protected final EntityManager entityManager;

	protected final TableAccessor accessor;

	protected final boolean disableConstraints;

	public Pusher(final EntityManager entityManager, final TableAccessor accessor, final boolean disableConstraints) {
		this.entityManager = entityManager;
		this.accessor = accessor;
		this.disableConstraints = disableConstraints;
	}

	public abstract void push(Map<String, Object> object) throws ParseException;

	public void open() {
		if (this.disableConstraints) {
			// TODO diable constraints
		}
	}

	@Override
	public void close() {
		if (this.disableConstraints) {
			// TODO enable constraints
		}
	}

}
