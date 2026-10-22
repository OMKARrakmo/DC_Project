package com.dms.core;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

@Component
public class NodeConfig {

    @Value("${node.id:1}")
    private int id;

    @Value("${node.host:127.0.0.1}")
    private String host;

    @Value("${server.port:8080}")
    private int serverPort;

    @Value("${rmi.port:1099}")
    private int rmiPort;

    @Value("${peers:}")
    private String peersString;

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
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

    public String getPeersString() {
        return peersString;
    }

    public void setPeersString(String peersString) {
        this.peersString = peersString;
    }

    public static class PeerAddress {
        private final String host;
        private final int serverPort;
        private final int rmiPort;

        public PeerAddress(String host, int serverPort, int rmiPort) {
            this.host = host;
            this.serverPort = serverPort;
            this.rmiPort = rmiPort;
        }

        public String getHost() {
            return host;
        }

        public int getServerPort() {
            return serverPort;
        }

        public int getRmiPort() {
            return rmiPort;
        }

        @Override
        public String toString() {
            return host + ":" + serverPort + ":" + rmiPort;
        }
    }

    public List<PeerAddress> getParsedPeers() {
        if (peersString == null || peersString.trim().isEmpty()) {
            return Collections.emptyList();
        }
        List<PeerAddress> peers = new ArrayList<>();
        String[] parts = peersString.split(",");
        for (String p : parts) {
            String trimmed = p.trim();
            if (trimmed.isEmpty()) continue;
            String[] tokens = trimmed.split(":");
            if (tokens.length >= 3) {
                try {
                    String h = tokens[0].trim();
                    int sPort = Integer.parseInt(tokens[1].trim());
                    int rPort = Integer.parseInt(tokens[2].trim());
                    peers.add(new PeerAddress(h, sPort, rPort));
                } catch (NumberFormatException ignored) {
                }
            }
        }
        return peers;
    }
}
