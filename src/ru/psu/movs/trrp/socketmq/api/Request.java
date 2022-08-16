package ru.psu.movs.trrp.socketmq.api;

import java.io.Serializable;

public interface Request extends Serializable {
    RequestType getRequestType();

    enum RequestType {
        Socket,
        MQ,
    }
}

