package com.cgi.lino.connector.controller;

public class NativeDialectDefault implements NativeDialect {

	@Override
	public boolean canHandle(String url) {
		return true;
	}

}
