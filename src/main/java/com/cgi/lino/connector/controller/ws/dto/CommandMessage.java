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


import com.cgi.lino.connector.controller.ws.dto.deserializer.CommandMessageDeserializer;
import com.cgi.lino.connector.controller.ws.dto.payload.Payload;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import lombok.*;


@JsonDeserialize(using= CommandMessageDeserializer.class)
@NoArgsConstructor
public @Data  class CommandMessage {

    /**
     * id : string identifiant unique du message
     */
    @JsonProperty("id")
    private String id;

    /**
     * action : string valeurs possible : "ping", "extract_tables", "extract_relations", "pull_open", "push_open", "push_data", "push_commit", "push_close"
     */
    @JsonProperty("action")
    private String action;


    /**
     * payload : Payload payload différent en fonction du type de l'action
     */
    @JsonProperty("payload")
    private Payload payload;


    public CommandMessage(String id, String action, Payload payload){
        this.id = id;
        this.action= action;
        this.payload= payload;
    }

}
