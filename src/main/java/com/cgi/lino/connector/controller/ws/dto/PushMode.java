package com.cgi.lino.connector.controller.ws.dto;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonValue;
import lombok.Getter;

@Getter
public enum PushMode {
    @JsonProperty("truncate")
    TRUNCATE("truncate"),
    @JsonProperty("delete")
    DELETE("delete"),
    @JsonProperty("insert")
    INSERT("insert"),
    @JsonProperty("update")
    UPDATE("update");


    @JsonValue
    private String mode;

    PushMode(String mode) {
        this.mode = mode;
    }


}
