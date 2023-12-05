// Copyright (C) 2021 CGI France
//
// This file is part of LINO.
//
// LINO is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// LINO is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with LINO.  If not, see <http://www.gnu.org/licenses/>.

package com.cgi.lino.connector.rest;

import static org.junit.Assert.assertTrue;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.sql.Connection;
import java.sql.SQLException;

import javax.sql.DataSource;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.SpringBootTest.WebEnvironment;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;




@SpringBootTest(webEnvironment = WebEnvironment.RANDOM_PORT)
class NominalEnd2EndTests {

@LocalServerPort
    private int port;

	
    @Autowired
    private TestRestTemplate restTemplate;


	@BeforeAll
	static void initDatabase(@Autowired DataSource ds) throws SQLException {
		try (Connection conn = ds.getConnection()) {
			conn.createStatement().execute("create table myTable (bli VARCHAR(100) NOT NULL, myString VARCHAR(100) NOT NULL, myAge INT);");
		}
		
	}

	@AfterAll
	static void cleanDatabase(@Autowired DataSource ds) throws SQLException {
		try (Connection conn = ds.getConnection()) {
			conn.createStatement().execute("drop table myTable;");
		}
		
	}


	@BeforeEach
	void initData(@Autowired DataSource ds) throws SQLException {
		try (Connection conn = ds.getConnection()) {
			conn.createStatement().execute("TRUNCATE TABLE myTable;");
			conn.createStatement().execute("insert into myTable (bli, myString, myAge) values ('blo', 'isAString',1),('blio', 'isAString2',42);");
		}
		
	}



	@Test
	void getInfo() throws MalformedURLException, URISyntaxException {
		String expected = """
			{"database":{"product":"H2","version":"2.1.214 (2022-06-13)"},"driver":{"name":"H2 JDBC Driver","version":"2.1.214 (2022-06-13)"},"jdbc":{"version":6}}""";

        ResponseEntity<String> response = restTemplate.getForEntity(
			new URI("http://localhost:" + port + "/api/v1/").toString(), String.class);
        assertEquals(expected, response.getBody());
	}

	@Test
	void getSchema() throws MalformedURLException, URISyntaxException {
		String expected = """
			{"schemas":[{"name":"INFORMATION_SCHEMA","catalog":""";

        ResponseEntity<String> response = restTemplate.getForEntity(
			new URI("http://localhost:" + port + "/api/v1/schemas").toString(), String.class);
        assertTrue(response.getBody().contains(expected));
	}

	@Test
	void getTables() throws MalformedURLException, URISyntaxException {
		ResponseEntity<String> response = restTemplate.getForEntity(
			new URI("http://localhost:" + port + "/api/v1/tables").toString(), String.class);
        assertTrue(response.getBody().contains("MYTABLE"));
	}

	@Test
	void getRelations() throws MalformedURLException, URISyntaxException {
		String expected = """
			{"version":"v1","relations":[]}""";

        ResponseEntity<String> response = restTemplate.getForEntity(
			new URI("http://localhost:" + port + "/api/v1/relations").toString(), String.class);
        assertEquals(expected, response.getBody());
	}

	@Test
	void pullData() throws MalformedURLException, URISyntaxException {
		String expected = """
			{"BLI":"blo","MYSTRING":"isAString","MYAGE":1}{"BLI":"blio","MYSTRING":"isAString2","MYAGE":42}""";

        ResponseEntity<String> response = restTemplate.getForEntity(
			new URI("http://localhost:" + port + "/api/v1/data/MYTABLE").toString(), String.class);
        assertEquals(expected, response.getBody().replaceAll(System.lineSeparator(), ""));
	}


	@Test
	void pushInsertData() throws MalformedURLException, URISyntaxException {
		HttpHeaders headers = new HttpHeaders();
		headers.setContentType(MediaType.APPLICATION_NDJSON);
	
		String data = """
			{"MYSTRING":"isAString3","BLI":"bliotheque","MYAGE":666}""";
		HttpEntity<String> request =  new HttpEntity<String>(data, headers);
	  	String expected = """
			{"BLI":"blo","MYSTRING":"isAString","MYAGE":1}{"BLI":"blio","MYSTRING":"isAString2","MYAGE":42}{"BLI":"bliotheque","MYSTRING":"isAString3","MYAGE":666}""";
        ResponseEntity<String> response = restTemplate.postForEntity(
			new URI("http://localhost:" + port + "/api/v1/data/MYTABLE").toString()+"?mode=insert&disableConstraints=false",
			request, 
			String.class);
		 assertEquals(200, response.getStatusCode().value());
		ResponseEntity<String> responsePull = restTemplate.getForEntity(
			new URI("http://localhost:" + port + "/api/v1/data/MYTABLE").toString(), String.class);
        assertEquals(expected, responsePull.getBody().replaceAll(System.lineSeparator(), ""));
	}

	@Test
	void pushTruncateData() throws MalformedURLException, URISyntaxException {
		HttpHeaders headers = new HttpHeaders();
		headers.setContentType(MediaType.APPLICATION_NDJSON);
	
		String data = """
			{"BLI":"bliotheque","MYSTRING":"isAString3","MYAGE":666}""";
		HttpEntity<String> request =  new HttpEntity<String>(data, headers);

        ResponseEntity<String> response = restTemplate.postForEntity(
			new URI("http://localhost:" + port + "/api/v1/data/MYTABLE").toString()+"?mode=truncate&disableConstraints=false",
			request, 
			String.class);
		 assertEquals(200, response.getStatusCode().value());
		ResponseEntity<String> responsePull = restTemplate.getForEntity(
			new URI("http://localhost:" + port + "/api/v1/data/MYTABLE").toString(), String.class);
        assertEquals(data, responsePull.getBody().replaceAll(System.lineSeparator(), ""));
	}

	/**
	 * @throws MalformedURLException
	 * @throws URISyntaxException
	 */
	@Test
	void pushDeleteData() throws MalformedURLException, URISyntaxException {
		HttpHeaders headers = new HttpHeaders();
		headers.setContentType(MediaType.APPLICATION_NDJSON);
	
		String data = """
			{"MYAGE":42}""";

		String expected ="""
				{"BLI":"blo","MYSTRING":"isAString","MYAGE":1}""";
		HttpEntity<String> request =  new HttpEntity<String>(data, headers);

        ResponseEntity<String> response = restTemplate.postForEntity(
			new URI("http://localhost:" + port + "/api/v1/data/MYTABLE").toString()+"?mode=delete&disableConstraints=false",
			request, 
			String.class);
		 assertEquals(200, response.getStatusCode().value());
		ResponseEntity<String> responsePull = restTemplate.getForEntity(
			new URI("http://localhost:" + port + "/api/v1/data/MYTABLE").toString(), String.class);
        assertEquals(expected, responsePull.getBody().replaceAll(System.lineSeparator(), ""));
		data = """
			{"BLI":"blo"}""";


		request =  new HttpEntity<String>(data, headers);

        response = restTemplate.postForEntity(
			new URI("http://localhost:" + port + "/api/v1/data/MYTABLE").toURL().toString()+"?mode=delete&disableConstraints=false",
			request, 
			String.class);
		 assertEquals(200, response.getStatusCode().value());
		responsePull = restTemplate.getForEntity(
			new URI("http://localhost:" + port + "/api/v1/data/MYTABLE").toString(), String.class);
        assertEquals(null, responsePull.getBody());

	}

}
