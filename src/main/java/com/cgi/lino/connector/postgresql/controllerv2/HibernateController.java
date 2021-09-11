package com.cgi.lino.connector.postgresql.controllerv2;

import java.io.IOException;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.stream.Stream;

import javax.persistence.EntityManager;
import javax.persistence.Tuple;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.servlet.mvc.method.annotation.StreamingResponseBody;

import com.cgi.lino.connector.postgresql.controller.Controller;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.ObjectMapper;

@RestController
@RequestMapping(path = "/api/v2")
public class HibernateController {

	private Logger logger = LoggerFactory.getLogger(Controller.class);

	private final EntityManager entityManager;

	private final ObjectMapper mapper;

	public HibernateController(final EntityManager entityManager, final ObjectMapper mapper) {
		this.entityManager = entityManager;
		this.mapper = mapper;

		mapper.getFactory().disable(JsonGenerator.Feature.AUTO_CLOSE_TARGET);
	}

	@GetMapping(path = "/data/{tableName}", produces = MediaType.APPLICATION_NDJSON_VALUE)
	public ResponseEntity<StreamingResponseBody> pullData(@RequestParam(required = false) String schema,
			@PathVariable("tableName") String tableName, @RequestBody(required = false) Map<String, Object> filter)
			throws SQLException, IOException {
		String where = "where 1=1";
		int limit = 0;

		if (filter != null) {
			String filterWhere = (String) filter.get("where");
			if (filterWhere != null && !filterWhere.isBlank()) {
				where = "where " + filterWhere;
			}

			@SuppressWarnings("unchecked")
			Map<String, Object> filterValues = (Map<String, Object>) filter.get("values");
			for (Iterator<Map.Entry<String, Object>> iterator = filterValues.entrySet().iterator(); iterator
					.hasNext();) {
				Map.Entry<String, Object> filterValue = iterator.next();
				if (filterValue.getValue() instanceof String) {
					where = where + " and " + filterValue.getKey() + "='" + filterValue.getValue() + "'";
				} else if (filterValue.getValue() instanceof Integer) {
					where = where + " and " + filterValue.getKey() + "=" + filterValue.getValue();
				} else {
					this.logger.error("Unsupported filter type : " + filterValue.getValue().getClass());
				}
			}

			limit = (int) filter.get("limit");
		}

		final String filterClause;
		if (limit > 0) {
			filterClause = where + " limit " + limit;
		} else {
			filterClause = where;
		}

		this.logger.info("Select from " + tableName + " " + filterClause);

		StreamingResponseBody stream = out -> {
			@SuppressWarnings("unchecked")
			Stream<Tuple> result = entityManager
					.createNativeQuery("select * from " + tableName + " " + filterClause, Tuple.class)
					.getResultStream();
			result.forEach(entry -> {
				try {
					Map<String, Object> resultItem = new HashMap<>();
					entry.getElements().forEach(col -> {
						resultItem.put(col.getAlias(), entry.get(col));
					});
					mapper.writeValue(out, resultItem);
					out.write(System.lineSeparator().getBytes());
					out.flush();
				} catch (IOException e) {
					throw new RuntimeException(e);
				}
			});
		};

		return ResponseEntity.ok().header("Content-Disposition", "inline").body(stream);
	}
}
