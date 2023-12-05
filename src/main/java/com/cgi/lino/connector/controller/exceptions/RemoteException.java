package com.cgi.lino.connector.controller.exceptions;

import lombok.Getter;

import java.util.UUID;

@Getter
public class RemoteException extends RuntimeException {

    private String commandId;
    private String causalityId;

    public RemoteException( String commandId, String causalityId, Throwable t) {
        super(t);
        this.commandId=commandId;
        this.causalityId=causalityId;
    }

    public RemoteException( String commandId, Throwable t) {
        super(t);
        this.commandId=commandId;
        this.causalityId=(t instanceof RemoteException)? ((RemoteException)t).getCausalityId(): UUID.randomUUID().toString();
    }

    public RemoteException(String commandId,String message) {
        super(message);
        this.commandId=commandId;
        this.causalityId= UUID.randomUUID().toString();
    }
    public RemoteException(String commandId,String message, String causalityId) {
        super(message);
        this.commandId=commandId;
        this.causalityId= causalityId;
    }
}
