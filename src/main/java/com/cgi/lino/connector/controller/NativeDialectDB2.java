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
import java.util.stream.Collectors;

public class NativeDialectDB2 implements NativeDialect {

	@Override
	public boolean canHandle(String url) {
		return url.startsWith("jdbc:db2:");
	}

	/**
	 * Get select fields statement by condition fields. Default use SELECT.
	 */
	@Override
	public String getSelectFromStatement(String schemaName, String tableName, String[] selectFields, String[] conditionFields, String additionalCondition, int limit) {
		String selectExpressions = "*";
		String fieldExpressions = Arrays.stream(conditionFields).map(f -> quoteIdentifier(f) + "=?").collect(Collectors.joining(" AND "));
		String limitExpression = (limit > 0) ? " " + this.getLimitClause(limit) : "";
		String additionalWhereKeyword = conditionFields.length > 0 ? " AND " : " WHERE ";
		return "SELECT " + selectExpressions + " FROM " + quoteIdentifier(schemaName, tableName) + (conditionFields.length > 0 ? " WHERE " + fieldExpressions : "")
				+ (additionalCondition != null && !additionalCondition.isBlank() ? additionalWhereKeyword + additionalCondition : "") + limitExpression;
	}

	/**
	 * Generate a limit clause.
	 */
	@Override
	public String getLimitClause(int limit) {
		return "FETCH FIRST " + limit + " ROWS ONLY";
	}

}
