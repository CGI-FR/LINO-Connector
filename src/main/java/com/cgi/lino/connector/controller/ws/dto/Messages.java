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
package com.cgi.lino.connector.controller.ws.dto;

import lombok.experimental.UtilityClass;

@UtilityClass
public class Messages {

    public static final String DEBUG_MESSAGE = """
            {"code":"{}", "level":"DEBUG","message":"{}"}""";
    public static final String INFO_MESSAGE = """
            {"code":"{}", "level":"INFO","message":"{}"}""";

    public static final String WARN_MESSAGE = """
            {"code":"{}", "level":"WARN","causalityId":"{}","message":"{}"}""";

    public static final String ERROR_MESSAGE = """
            {"code":"{}", "level":"ERROR","causalityId":"{}","message":"{}"}
            """;

}
