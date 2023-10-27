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

package com.cgi.lino.connector.controller.ws.dto.constants;

import lombok.experimental.UtilityClass;

@UtilityClass
public class Actions {
    public static final String PING = "ping";
    public static final String SCHEMA = "schema";
    public static final String TABLES = "extract_tables";
    public static final String RELATIONS = "extract_relations";
    public static final String PULL_OPEN = "pull_open";

    public static final String PUSH_OPEN = "push_open";
    public static final String PUSH_DATA = "push_data";
    public static final String PUSH_COMMIT = "push_commit";
    public static final String PUSH_CLOSE = "push_close";
}
