package com.cgi.lino.connector.postgresql.controller;

import java.text.ParseException;
import java.util.Map;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.Query;

public abstract class Pusher implements AutoCloseable {

	protected final EntityManager entityManager;

	protected final TableAccessor accessor;

	protected final boolean disableConstraints;

	public Pusher(final EntityManagerFactory entityManagerFactory, final TableAccessor accessor, final boolean disableConstraints) {
		this.entityManager = entityManagerFactory.createEntityManager();
		this.accessor = accessor;
		this.disableConstraints = disableConstraints;
	}

	public abstract void push(Map<String, Object> object) throws ParseException;

	public void open() {
		this.entityManager.getTransaction().begin();
		if (this.disableConstraints) {
			Query query = entityManager.createNativeQuery(accessor.getNativeQueryDisableContraints());
			query.executeUpdate();
		}
	}

	@Override
	public void close() {
		if (this.disableConstraints) {
			Query query = entityManager.createNativeQuery(accessor.getNativeQueryEnableContraints());
			query.executeUpdate();
		}
		this.entityManager.flush();
		this.entityManager.clear();
		this.entityManager.getTransaction().commit();
	}

}
