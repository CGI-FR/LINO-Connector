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
package com.cgi.lino.connector.controller.ws.dto.payload;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.databind.node.ValueNode;
import lombok.Data;
import lombok.NonNull;

import java.util.Map;

@JsonPropertyOrder({
        "table",
        "row",
        "conditions"
})
@JsonInclude(JsonInclude.Include.NON_NULL)
public final @Data class PushDataPayload implements Payload{

    @JsonProperty("table")
    private String table;

    @JsonProperty("row")
    private Map<String, ValueNode> row;

    @JsonProperty("conditions")
    private Map<String, ValueNode> conditions;

    @JsonCreator
    public PushDataPayload(  @JsonProperty("table") @NonNull String table,
                             @JsonProperty("row") Map<String, ValueNode> row,
                             @JsonProperty("conditions") Map<String, ValueNode> conditions){
        this.table=table;
        this.row=row;
        this.conditions=conditions;
    }
}
