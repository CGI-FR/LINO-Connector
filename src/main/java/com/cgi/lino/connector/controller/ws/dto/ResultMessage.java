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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.databind.JsonNode;
import lombok.Data;
import lombok.NonNull;
@JsonPropertyOrder({
        "id",
        "error",
        "next",
        "payload"

})
public @Data class ResultMessage {

    @JsonProperty("id")
    private String id;

    @JsonProperty("error")
    private String error;

    @JsonProperty("next")
    private Boolean next;

    @JsonProperty("payload")
    private JsonNode payload;


    @JsonCreator
    public ResultMessage(
            @JsonProperty("id") String id,
            @JsonProperty("error") String error,
            @JsonProperty("next") @NonNull Boolean next,
            @JsonProperty("payload") JsonNode payload
    ) {
        this.id=id;
        this.error=error;
        this.next=next;
        this.payload=payload;
    }

}
