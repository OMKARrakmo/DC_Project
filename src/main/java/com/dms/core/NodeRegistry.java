package com.dms.core;

import com.dms.rmi.RemoteDonationService;
import com.dms.rmi.TimeoutClientSocketFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

@Component
public class NodeRegistry {

    private final NodeConfig nodeConfig;
    private final LamportClock lamportClock;
    private final TimeoutClientSocketFactory socketFactory = new TimeoutClientSocketFactory();

    private final AtomicInteger activeConnections = new AtomicInteger(0);
    private final AtomicInteger primaryId = new AtomicInteger(1);

    // Map of nodeId -> PeerState
    private final Map<Integer, PeerState> peersById = new ConcurrentHashMap<>();
    // Fallback map by address key "host:serverPort:rmiPort" -> PeerState
    private final Map<String, PeerState> peersByAddr = new ConcurrentHashMap<>();

    private EventLogger eventLogger;

    public static class PeerState {
        private final String host;
        private final int serverPort;
        private final int rmiPort;
        private int nodeId;
        private boolean alive = false;
        private long lastSeen = 0;
        private String os = "Unknown";
        private long lamportTs = 0;
        private int activeConnections = 0;
        private RemoteDonationService stub;

        public PeerState(String host, int serverPort, int rmiPort) {
            this.host = host;
            this.serverPort = serverPort;
            this.rmiPort = rmiPort;
            this.nodeId = -1;
        }

        public String getHost() { return host; }
        public int getServerPort() { return serverPort; }
        public int getRmiPort() { return rmiPort; }
        public int getNodeId() { return nodeId; }
        public void setNodeId(int nodeId) { this.nodeId = nodeId; }
        public boolean isAlive() { return alive; }
        public void setAlive(boolean alive) { this.alive = alive; }
        public long getLastSeen() { return lastSeen; }
        public void setLastSeen(long lastSeen) { this.lastSeen = lastSeen; }
        public String getOs() { return os; }
        public void setOs(String os) { this.os = os; }
        public long getLamportTs() { return lamportTs; }
        public void setLamportTs(long lamportTs) { this.lamportTs = lamportTs; }
        public int getActiveConnections() { return activeConnections; }
        public void setActiveConnections(int activeConnections) { this.activeConnections = activeConnections; }
        public RemoteDonationService getStub() { return stub; }
        public void setStub(RemoteDonationService stub) { this.stub = stub; }
    }

    public NodeRegistry(NodeConfig nodeConfig, LamportClock lamportClock) {
        this.nodeConfig = nodeConfig;
        this.lamportClock = lamportClock;
        this.primaryId.set(nodeConfig.getId());

        // Initialize configured peers
        for (NodeConfig.PeerAddress p : nodeConfig.getParsedPeers()) {
            String key = p.getHost() + ":" + p.getServerPort() + ":" + p.getRmiPort();
            PeerState ps = new PeerState(p.getHost(), p.getServerPort(), p.getRmiPort());
            peersByAddr.put(key, ps);
        }
    }

    public void setEventLogger(EventLogger eventLogger) {
        this.eventLogger = eventLogger;
    }

    public int getPrimaryId() {
        return primaryId.get();
    }

    public void setPrimaryId(int id) {
        this.primaryId.set(id);
    }

    public int getActiveConnections() {
        return activeConnections.get();
    }

    public void incrementConnections() {
        activeConnections.incrementAndGet();
    }

    public void decrementConnections() {
        activeConnections.decrementAndGet();
    }

    public NodeInfo getLocalNodeInfo() {
        return new NodeInfo(
                nodeConfig.getId(),
                nodeConfig.getHost(),
                nodeConfig.getServerPort(),
                nodeConfig.getRmiPort(),
                System.getProperty("os.name"),
                lamportClock.get(),
                primaryId.get(),
                activeConnections.get(),
                true
        );
    }

    public List<NodeInfo> getAllNodes() {
        List<NodeInfo> list = new ArrayList<>();
        list.add(getLocalNodeInfo());

        for (PeerState ps : peersByAddr.values()) {
            if (ps.getNodeId() != -1 && ps.getNodeId() != nodeConfig.getId()) {
                list.add(new NodeInfo(
                        ps.getNodeId(),
                        ps.getHost(),
                        ps.getServerPort(),
                        ps.getRmiPort(),
                        ps.getOs(),
                        ps.getLamportTs(),
                        primaryId.get(),
                        ps.getActiveConnections(),
                        ps.isAlive()
                ));
            } else if (ps.getNodeId() == -1) {
                // Configured but not yet contacted
                list.add(new NodeInfo(
                        -1,
                        ps.getHost(),
                        ps.getServerPort(),
                        ps.getRmiPort(),
                        ps.getOs(),
                        0,
                        primaryId.get(),
                        0,
                        false
                ));
            }
        }
        list.sort(Comparator.comparingInt(NodeInfo::getNodeId));
        return list;
    }

    public RemoteDonationService getPeerStub(int targetNodeId) {
        PeerState ps = peersById.get(targetNodeId);
        if (ps != null && ps.isAlive()) {
            try {
                if (ps.getStub() != null) {
                    return ps.getStub();
                }
                Registry registry = LocateRegistry.getRegistry(ps.getHost(), ps.getRmiPort(), socketFactory);
                RemoteDonationService stub = (RemoteDonationService) registry.lookup("RemoteDonationService");
                ps.setStub(stub);
                return stub;
            } catch (Exception e) {
                ps.setStub(null);
                ps.setAlive(false);
            }
        }
        return null;
    }

    public PeerState getPeerState(int targetNodeId) {
        return peersById.get(targetNodeId);
    }

    public List<PeerState> getLivePeers() {
        List<PeerState> live = new ArrayList<>();
        for (PeerState ps : peersByAddr.values()) {
            if (ps.isAlive()) {
                live.add(ps);
            }
        }
        return live;
    }

    /**
     * Heartbeat runs every 2s per SPEC section 8.
     * Pings each configured peer via RMI ping().
     */
    @Scheduled(fixedDelay = 2000, initialDelay = 2000)
    public void heartbeat() {
        long now = System.currentTimeMillis();

        for (PeerState ps : peersByAddr.values()) {
            try {
                Registry registry = LocateRegistry.getRegistry(ps.getHost(), ps.getRmiPort(), socketFactory);
                RemoteDonationService stub = (RemoteDonationService) registry.lookup("RemoteDonationService");
                ps.setStub(stub);

                NodeInfo info = stub.ping();
                ps.setNodeId(info.getNodeId());
                ps.setOs(info.getOs());
                ps.setLamportTs(info.getLamportTs());
                ps.setActiveConnections(info.getActiveConnections());
                ps.setLastSeen(now);

                peersById.put(info.getNodeId(), ps);

                boolean wasAlive = ps.isAlive();
                ps.setAlive(true);

                // Synchronize Lamport clock with received timestamp
                lamportClock.update(info.getLamportTs());

                if (!wasAlive && eventLogger != null) {
                    eventLogger.log("EXP8-FT", String.format("Peer node=%d (%s:%d) is ALIVE",
                            info.getNodeId(), ps.getHost(), ps.getServerPort()));
                }
            } catch (Exception e) {
                // Failure to contact peer
                ps.setStub(null);
                if (ps.isAlive() && (now - ps.getLastSeen() > 6000)) {
                    ps.setAlive(false);
                    if (eventLogger != null) {
                        eventLogger.log("EXP8-FT", String.format("Peer %s:%d suspected DEAD (heartbeat timeout > 6s)",
                                ps.getHost(), ps.getRmiPort()));
                    }
                }
            }
        }

        updateCoordinatorForP1();
    }

    /**
     * For P1: coordinator is highest alive node id (self or alive peers).
     * (Formal Bully/Ring election is added in P3/P5).
     */
    private void updateCoordinatorForP1() {
        int maxId = nodeConfig.getId();
        for (PeerState ps : peersById.values()) {
            if (ps.isAlive() && ps.getNodeId() > maxId) {
                maxId = ps.getNodeId();
            }
        }
        primaryId.set(maxId);
    }
}
