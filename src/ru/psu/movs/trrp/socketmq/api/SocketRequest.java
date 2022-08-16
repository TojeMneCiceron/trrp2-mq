package ru.psu.movs.trrp.socketmq.api;

public class SocketRequest implements Request {

    @Override
    public RequestType getRequestType() {
        return RequestType.Socket;
    }
}
