package com.dms.core;

import java.io.Serializable;

public class NodeInfo implements Serializable {
    private static final long serialVersionUID = 1L;

    private int nodeId;
    private String host;
    private int serverPort;
    private int rmiPort;
    private String os;
    private long lamportTs;
    private int primaryId;
    private int activeConnections;
    private boolean alive;

    public NodeInfo() {
    }

    public NodeInfo(int nodeId, String host, int serverPort, int rmiPort, String os,
                    long lamportTs, int primaryId, int activeConnections, boolean alive) {
        this.nodeId = nodeId;
        this.host = host;
        this.serverPort = serverPort;
        this.rmiPort = rmiPort;
        this.os = os;
        this.lamportTs = lamportTs;
        this.primaryId = primaryId;
        this.activeConnections = activeConnections;
        this.alive = alive;
    }

    public int getNodeId() {
        return nodeId;
    }

    public void setNodeId(int nodeId) {
        this.nodeId = nodeId;
    }

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public int getServerPort() {
        return serverPort;
    }

    public void setServerPort(int serverPort) {
        this.serverPort = serverPort;
    }

    public int getRmiPort() {
        return rmiPort;
    }

    public void setRmiPort(int rmiPort) {
        this.rmiPort = rmiPort;
    }

    public String getOs() {
        return os;
    }

    public void setOs(String os) {
        this.os = os;
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

    public int getActiveConnections() {
        return activeConnections;
    }

    public void setActiveConnections(int activeConnections) {
        this.activeConnections = activeConnections;
    }

    public boolean isAlive() {
        return alive;
    }

    public void setAlive(boolean alive) {
        this.alive = alive;
    }

    @Override
    public String toString() {
        return "NodeInfo{" +
                "nodeId=" + nodeId +
                ", host='" + host + '\'' +
                ", serverPort=" + serverPort +
                ", rmiPort=" + rmiPort +
                ", os='" + os + '\'' +
                ", lamportTs=" + lamportTs +
                ", primaryId=" + primaryId +
                ", activeConnections=" + activeConnections +
                ", alive=" + alive +
                '}';
    }
}
