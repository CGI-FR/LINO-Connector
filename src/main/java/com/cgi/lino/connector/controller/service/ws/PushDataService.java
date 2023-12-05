package com.cgi.lino.connector.controller.service.ws;



import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import jakarta.persistence.EntityManagerFactory;
import jakarta.transaction.HeuristicMixedException;
import jakarta.transaction.HeuristicRollbackException;
import jakarta.transaction.NotSupportedException;
import jakarta.transaction.SystemException;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;



import java.text.ParseException;

import javax.sql.DataSource;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import com.cgi.lino.connector.controller.exceptions.RemoteException;
import com.cgi.lino.connector.controller.ws.dto.CommandMessage;
import com.cgi.lino.connector.controller.ws.dto.constants.Actions;
import com.cgi.lino.connector.controller.ws.dto.payload.PushDataPayload;
import com.cgi.lino.connector.controller.ws.dto.payload.PushOpenPayload;


@Service
public class PushDataService {



    ObjectMapper mapper = new ObjectMapper();

    private Map<String, PushDataWorker> sessions = new ConcurrentHashMap<>();

    @Value("#{systemProperties['insecure'] ?: 'false'}")
    String insecure;

    @Value("#{systemProperties['timeZone'] ?: 'UTC'}")
    String timeZone;

    private DataSource datasource;

    private EntityManagerFactory emf;


    public PushDataService( DataSource datasource, EntityManagerFactory emf){
        this.datasource = datasource;
        this.emf= emf;
    }

 
    public JsonNode pushAction(String transactionId, CommandMessage cmdMessage) {
        ObjectNode result =mapper.createObjectNode();
        result.put("ack",cmdMessage.getId());
        String ackMessage;

        try {
            switch (cmdMessage.getAction()) {
                case Actions.PUSH_OPEN -> {
                    PushOpenPayload payload = (PushOpenPayload) cmdMessage.getPayload();
                    this.sessions.put(transactionId, new PushDataWorker(cmdMessage.getId(), datasource, emf.createEntityManager(), payload.getSchema(), payload.getTables(), payload.getPushMode(), payload.getDisableConstraints(), this.timeZone));
                    ackMessage="Push open: done with disableConstraints=%s and mode=%s".formatted(payload.getDisableConstraints(), payload.getPushMode());
                }
                case Actions.PUSH_DATA -> {
                    PushDataPayload payload = (PushDataPayload) cmdMessage.getPayload();
                    PushDataWorker worker = this.sessions.get(transactionId);
                    if (worker == null) {
                        throw new RemoteException(cmdMessage.getId(), "illegal push data: the connection has not initialized. Please emit an push_open action before push_data.");
                    }
                    if (!worker.getTables().containsKey(payload.getTable())) {
                        throw new RemoteException(cmdMessage.getId(), "illegal push data: the connection has initialized without the table %s".formatted(payload.getTable()));
                    }
                    int sqlResult = worker.pushData(payload.getTable(), payload.getRow(), payload.getConditions());
                    ackMessage="Push date sql response: %s".formatted(sqlResult);
                }
                case Actions.PUSH_COMMIT -> {
                    PushDataWorker worker = this.sessions.get(transactionId);
                    if (worker == null) {
                        throw new RemoteException(cmdMessage.getId(), "illegal push data: the connection has not initialized. Please emit an push_open action before push_commit.");
                    };
                    worker.commit();
                    ackMessage="Push commit: done";
                }
                case Actions.PUSH_CLOSE -> {
                    PushDataWorker worker = this.sessions.get(transactionId);
                    if (worker == null) {
                        throw new RemoteException(cmdMessage.getId(), "illegal push data: the connection has not initialized. Please emit an push_open action before push_close.");
                    };
                    worker.close();
                    this.sessions.remove(transactionId);
                    ackMessage="Push close: done with enableConstraints=%s".formatted(worker.isDisabledConstraints());
                }
                default ->
                        throw new RemoteException(cmdMessage.getId(), "Action %s is not a pushAction".formatted(cmdMessage.getAction()));
            }
        } catch (SystemException | NotSupportedException | ParseException | HeuristicRollbackException |
                 HeuristicMixedException  e) {
            throw new RemoteException(cmdMessage.getId(), e);
        }
        result.put("message",ackMessage);
        return result;
    }


    public void close(String transactionId) {
        PushDataWorker worker = this.sessions.get(transactionId);
        if(worker!=null) {
            try {
                worker.close();
                this.sessions.remove(transactionId);
            } catch (SystemException | HeuristicRollbackException | HeuristicMixedException  | NotSupportedException e) {
                throw new RemoteException(worker.getCommandeId(), e);
            }
        }
    }
}
