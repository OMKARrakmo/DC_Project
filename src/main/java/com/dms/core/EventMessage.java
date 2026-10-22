package com.dms.core;

import java.io.Serializable;
import java.time.Instant;

public class EventMessage implements Serializable {
    private static final long serialVersionUID = 1L;

    private String id;
    private String timestamp;
    private String tag;
    private int nodeId;
    private long lamportTs;
    private String message;
    private String formatted;

    public EventMessage() {
    }

    public EventMessage(String id, String tag, int nodeId, long lamportTs, String message) {
        this.id = id;
        this.timestamp = Instant.now().toString();
        this.tag = tag;
        this.nodeId = nodeId;
        this.lamportTs = lamportTs;
        this.message = message;
        this.formatted = String.format("[%s] node=%d lamport=%d %s", tag, nodeId, lamportTs, message);
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(String timestamp) {
        this.timestamp = timestamp;
    }

    public String getTag() {
        return tag;
    }

    public void setTag(String tag) {
        this.tag = tag;
    }

    public int getNodeId() {
        return nodeId;
    }

    public void setNodeId(int nodeId) {
        this.nodeId = nodeId;
    }

    public long getLamportTs() {
        return lamportTs;
    }

    public void setLamportTs(long lamportTs) {
        this.lamportTs = lamportTs;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    public String getFormatted() {
        return formatted;
    }

    public void setFormatted(String formatted) {
        this.formatted = formatted;
    }
}
