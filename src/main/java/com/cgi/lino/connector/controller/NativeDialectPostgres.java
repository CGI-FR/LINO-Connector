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

import java.util.Arrays;
import java.util.Optional;
import java.util.stream.Collectors;

public class NativeDialectPostgres implements NativeDialect {

	@Override
	public boolean canHandle(String url) {
		return url.startsWith("jdbc:postgresql:");
	}

	/**
	 * Postgres upsert query.
	 * 
	 * It use ON CONFLICT ... DO UPDATE SET.. to replace into Postgres.
	 */
	@Override
	public Optional<String> getUpsertStatement(String schemaName, String tableName, String[] fieldNames, String[] uniqueKeyFields) {
		String uniqueColumns = Arrays.stream(uniqueKeyFields).map(this::quoteIdentifier).collect(Collectors.joining(", "));
		String updateClause = Arrays.stream(fieldNames).map(f -> quoteIdentifier(f) + "=EXCLUDED." + quoteIdentifier(f)).collect(Collectors.joining(", "));
		return Optional.of(getInsertIntoStatement(schemaName, tableName, fieldNames) + " ON CONFLICT (" + uniqueColumns + ")" + " DO UPDATE SET " + updateClause);
	}
}
