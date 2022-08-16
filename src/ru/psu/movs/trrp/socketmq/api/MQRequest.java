package ru.psu.movs.trrp.socketmq.api;

public class MQRequest implements Request {
    public String queueName;

    public MQRequest(String queueName) {
        this.queueName = queueName;
    }

    @Override
    public RequestType getRequestType() {
        return RequestType.MQ;
    }
}
