// Copyright (C) 2022 CGI France
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
package com.cgi.lino.connector.controller.ws.dto.payload;


import com.cgi.lino.connector.controller.ws.dto.PushMode;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import lombok.Data;
import lombok.NonNull;

import java.util.List;
import java.util.Objects;

@JsonPropertyOrder({
        "tables",
        "mode",
        "disable_constraints"
})
@JsonInclude(JsonInclude.Include.NON_NULL)
public @Data
final class PushOpenPayload implements Payload {

    @JsonProperty("schema")
    private String schema;

    @JsonProperty("tables")
    private List<String> tables;

    @JsonProperty("mode")
    private PushMode pushMode;

    @JsonProperty("disable_constraints")
    private Boolean disableConstraints;

    @JsonCreator
    public PushOpenPayload(
            @JsonProperty("schema") String schema,
            @JsonProperty("tables") @NonNull List<String> tables,
            @JsonProperty("mode") @NonNull PushMode pushMode,
            @JsonProperty("disable_constraints") Boolean disableConstraints) {
        this.schema=schema;
        this.tables = tables;
        this.pushMode = pushMode;
        this.disableConstraints = Objects.requireNonNullElse(disableConstraints, false);
    }

}
