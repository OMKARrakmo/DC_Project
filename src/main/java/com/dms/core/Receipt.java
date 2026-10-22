package com.dms.core;

import java.io.Serializable;

public class Receipt implements Serializable {
    private static final long serialVersionUID = 1L;

    private int processingNodeId;
    private String host;
    private String os;
    private String threadName;
    private long lamportTs;
    private int primaryId;
    private String status;
    private String message;

    public Receipt() {
    }

    public Receipt(int processingNodeId, String host, String os, String threadName,
                   long lamportTs, int primaryId, String status, String message) {
        this.processingNodeId = processingNodeId;
        this.host = host;
        this.os = os;
        this.threadName = threadName;
        this.lamportTs = lamportTs;
        this.primaryId = primaryId;
        this.status = status;
        this.message = message;
    }

    public int getProcessingNodeId() {
        return processingNodeId;
    }

    public void setProcessingNodeId(int processingNodeId) {
        this.processingNodeId = processingNodeId;
    }

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public String getOs() {
        return os;
    }

    public void setOs(String os) {
        this.os = os;
    }

    public String getThreadName() {
        return threadName;
    }

    public void setThreadName(String threadName) {
        this.threadName = threadName;
    }

    public long getLamportTs() {
        return lamportTs;
    }

    public void setLamportTs(long lamportTs) {
        this.lamportTs = lamportTs;
    }

    public int getPrimaryId() {
        return primaryId;
    }

    public void setPrimaryId(int primaryId) {
        this.primaryId = primaryId;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    @Override
    public String toString() {
        return "Receipt{" +
                "processingNodeId=" + processingNodeId +
                ", host='" + host + '\'' +
                ", os='" + os + '\'' +
                ", threadName='" + threadName + '\'' +
                ", lamportTs=" + lamportTs +
                ", primaryId=" + primaryId +
                ", status='" + status + '\'' +
                ", message='" + message + '\'' +
                '}';
    }
}
