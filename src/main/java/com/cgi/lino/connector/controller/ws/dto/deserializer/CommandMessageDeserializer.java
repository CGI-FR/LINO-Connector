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
package com.cgi.lino.connector.controller.ws.dto.deserializer;


import com.cgi.lino.connector.controller.ws.dto.CommandMessage;
import com.cgi.lino.connector.controller.ws.dto.constants.Actions;
import com.cgi.lino.connector.controller.ws.dto.payload.DBPayload;
import com.cgi.lino.connector.controller.ws.dto.payload.Payload;
import com.cgi.lino.connector.controller.ws.dto.payload.PullOpenPayload;
import com.cgi.lino.connector.controller.ws.dto.payload.PushDataPayload;
import com.cgi.lino.connector.controller.ws.dto.payload.PushOpenPayload;
import com.fasterxml.jackson.core.JacksonException;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;

public class CommandMessageDeserializer extends JsonDeserializer<CommandMessage> {

    @Override
    public CommandMessage deserialize(JsonParser jsonParser, DeserializationContext deserializationContext) throws IOException, JacksonException {
        JsonNode commandMessageNode = jsonParser.getCodec().readTree(jsonParser);
        CommandMessage commandMessage = new CommandMessage();
        commandMessage.setId(commandMessageNode.get("id").textValue());
        commandMessage.setAction(commandMessageNode.get("action").textValue());
        ObjectMapper mapper = new ObjectMapper();
        Payload payload =switch (commandMessage.getAction()){
            case Actions.PING,Actions.SCHEMA, Actions.PUSH_COMMIT,Actions.PUSH_CLOSE -> null;
            case Actions.TABLES, Actions.RELATIONS-> mapper.readValue(commandMessageNode.get("payload").toString(), DBPayload.class);
            case Actions.PULL_OPEN -> mapper.readValue(commandMessageNode.get("payload").toString(), PullOpenPayload.class);
            case Actions.PUSH_OPEN-> mapper.readValue(commandMessageNode.get("payload").toString(), PushOpenPayload.class);
            case Actions.PUSH_DATA-> mapper.readValue(commandMessageNode.get("payload").toString(), PushDataPayload.class);
            default -> throw new UnsupportedOperationException(String.format("The deserialization of %s is not supported",commandMessage.getAction()));
        };
        commandMessage.setPayload(payload);
        return commandMessage;
    }
}
