package com.dms.lb;

import com.dms.core.EventLogger;
import com.dms.core.LamportClock;
import com.dms.core.NodeConfig;
import com.dms.core.NodeRegistry;
import org.springframework.stereotype.Service;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

@Service
public class LoadBalancingService {

    public static final String STRATEGY_LEAST_CONNECTIONS = "LEAST_CONNECTIONS";
    public static final String STRATEGY_ROUND_ROBIN = "ROUND_ROBIN";

    private final NodeConfig nodeConfig;
    private final NodeRegistry nodeRegistry;
    private final EventLogger eventLogger;
    private final LamportClock lamportClock;

    private final LeastConnectionsLoadBalancer leastConnectionsLB;
    private final RoundRobinLoadBalancer roundRobinLB;

    private final AtomicReference<String> strategy = new AtomicReference<>(STRATEGY_LEAST_CONNECTIONS);
    private final AtomicLong artificialDelayMs = new AtomicLong(0);

    // Track active requests sent by this gateway to each node
    private final Map<Integer, AtomicInteger> gatewayInFlight = new ConcurrentHashMap<>();

    public LoadBalancingService(NodeConfig nodeConfig,
                                NodeRegistry nodeRegistry,
                                EventLogger eventLogger,
                                LamportClock lamportClock,
                                LeastConnectionsLoadBalancer leastConnectionsLB,
                                RoundRobinLoadBalancer roundRobinLB) {
        this.nodeConfig = nodeConfig;
        this.nodeRegistry = nodeRegistry;
        this.eventLogger = eventLogger;
        this.lamportClock = lamportClock;
        this.leastConnectionsLB = leastConnectionsLB;
        this.roundRobinLB = roundRobinLB;
    }

    public String getStrategy() {
        return strategy.get();
    }

    public void setStrategy(String newStrategy) {
        if (STRATEGY_ROUND_ROBIN.equalsIgnoreCase(newStrategy)) {
            strategy.set(STRATEGY_ROUND_ROBIN);
            roundRobinLB.reset();
        } else {
            strategy.set(STRATEGY_LEAST_CONNECTIONS);
            leastConnectionsLB.reset();
        }
        lamportClock.tick();
        eventLogger.log("EXP6-LB", "Load balancer strategy changed to: " + strategy.get());
    }

    public long getArtificialDelayMs() {
        return artificialDelayMs.get();
    }

    public void setArtificialDelayMs(long ms) {
        artificialDelayMs.set(Math.max(0, ms));
        lamportClock.tick();
        eventLogger.log("EXP6-LB", "Artificial processing delay set to: " + artificialDelayMs.get() + "ms");
    }

    public int getInFlight(int nodeId) {
        AtomicInteger ai = gatewayInFlight.get(nodeId);
        return (ai != null) ? Math.max(0, ai.get()) : 0;
    }

    public Map<Integer, Integer> getActiveConnectionsSnapshot() {
        Map<Integer, Integer> map = new TreeMap<>();
        int localId = nodeConfig.getId();
        int localTotal = nodeRegistry.getActiveConnections() + getInFlight(localId);
        map.put(localId, localTotal);

        for (NodeRegistry.PeerState ps : nodeRegistry.getLivePeers()) {
            int peerId = ps.getNodeId();
            if (peerId > 0) {
                int peerTotal = ps.getActiveConnections() + getInFlight(peerId);
                map.put(peerId, peerTotal);
            }
        }
        return map;
    }

    public int chooseNode() {
        List<Integer> candidates = new ArrayList<>();
        candidates.add(nodeConfig.getId());

        for (NodeRegistry.PeerState ps : nodeRegistry.getLivePeers()) {
            if (ps.getNodeId() > 0 && !candidates.contains(ps.getNodeId())) {
                candidates.add(ps.getNodeId());
            }
        }
        Collections.sort(candidates);

        Map<Integer, Integer> conns = getActiveConnectionsSnapshot();
        String currentStrategy = strategy.get();
        int selected;

        if (STRATEGY_ROUND_ROBIN.equalsIgnoreCase(currentStrategy)) {
            selected = roundRobinLB.selectNode(candidates, conns);
        } else {
            selected = leastConnectionsLB.selectNode(candidates, conns);
        }

        long ts = lamportClock.tick();
        eventLogger.logWithLamport("EXP6-LB", ts, String.format(
                "Strategy=%s selected node=%d from candidates=%s (activeConns=%s)",
                currentStrategy, selected, candidates, conns));

        return selected;
    }

    public InFlightHandle trackInFlight(int nodeId) {
        return new InFlightHandle(nodeId);
    }

    public class InFlightHandle implements AutoCloseable {
        private final int nodeId;

        public InFlightHandle(int nodeId) {
            this.nodeId = nodeId;
            gatewayInFlight.computeIfAbsent(nodeId, k -> new AtomicInteger(0)).incrementAndGet();
        }

        @Override
        public void close() {
            gatewayInFlight.computeIfAbsent(nodeId, k -> new AtomicInteger(0)).decrementAndGet();
        }
    }
}
