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

package com.cgi.lino.connector.controller;

public class NativeDialectH2 implements NativeDialect {

	@Override
	public boolean canHandle(String url) {
		return url.startsWith("jdbc:h2:");
	}

	@Override
	public String getTruncateStatement(String schemaName, String tableName) {
		return "TRUNCATE TABLE " + quoteIdentifier(schemaName, tableName);
	}

	@Override
	public String getDisableConstraintsStatement(String schemaName, String tableName) {
		return "ALTER TABLE " + quoteIdentifier(schemaName, tableName) + " SET REFERENTIAL_INTEGRITY FALSE";
	}

	@Override
	public String getEnableConstraintsStatement(String schemaName, String tableName) {
		return "ALTER TABLE " + quoteIdentifier(schemaName, tableName) + " SET REFERENTIAL_INTEGRITY TRUE";
	}
}
