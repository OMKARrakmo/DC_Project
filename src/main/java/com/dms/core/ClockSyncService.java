package com.dms.core;

import com.dms.rmi.RemoteDonationService;
import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.time.ZoneId;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;

@Service
public class ClockSyncService {

    private final NodeConfig nodeConfig;
    private final NodeRegistry nodeRegistry;
    private final EventLogger eventLogger;
    private final LamportClock lamportClock;

    private final AtomicLong artificialSkewMs = new AtomicLong(0);
    private final AtomicLong physicalClockOffset = new AtomicLong(0);

    public ClockSyncService(NodeConfig nodeConfig,
                            @Lazy NodeRegistry nodeRegistry,
                            EventLogger eventLogger,
                            LamportClock lamportClock) {
        this.nodeConfig = nodeConfig;
        this.nodeRegistry = nodeRegistry;
        this.eventLogger = eventLogger;
        this.lamportClock = lamportClock;
    }

    public long getArtificialSkew() {
        return getArtificialSkewMs();
    }

    public long getArtificialSkewMs() {
        return artificialSkewMs.get();
    }

    public void setArtificialSkew(long ms) {
        setArtificialSkewMs(ms);
    }

    public void setArtificialSkewMs(long ms) {
        this.artificialSkewMs.set(ms);
        long ts = lamportClock.tick();
        eventLogger.logWithLamport("EXP3-CLOCK", ts, String.format(
                "Artificial physical clock skew set to: %+dms (Virtual clock: %d)",
                ms, getVirtualClockMillis()));
    }

    public long getPhysicalClockOffset() {
        return physicalClockOffset.get();
    }

    public void applyOffset(long offsetMs) {
        physicalClockOffset.addAndGet(offsetMs);
        long ts = lamportClock.tick();
        eventLogger.logWithLamport("EXP3-CLOCK", ts, String.format(
                "Applied Berkeley clock offset: %+dms (Cumulative offset: %+dms, Virtual clock: %d)",
                offsetMs, physicalClockOffset.get(), getVirtualClockMillis()));
    }

    public void resetClock() {
        artificialSkewMs.set(0);
        physicalClockOffset.set(0);
        long ts = lamportClock.tick();
        eventLogger.logWithLamport("EXP3-CLOCK", ts, "Clock skew and offsets reset to 0.");
    }

    /**
     * Virtual physical clockMillis.
     * Note: Operating system clock is NEVER modified; only this process virtual clock is synchronized.
     */
    public long getVirtualClockMillis() {
        return System.currentTimeMillis() + artificialSkewMs.get() + physicalClockOffset.get();
    }

    public Map<String, Object> getStatus() {
        long sys = System.currentTimeMillis();
        long virt = getVirtualClockMillis();
        long skew = artificialSkewMs.get();
        long offset = physicalClockOffset.get();
        long lamp = lamportClock.get();

        String fmtVirt = Instant.ofEpochMilli(virt).atZone(ZoneId.systemDefault()).toLocalTime().toString();
        String fmtSys = Instant.ofEpochMilli(sys).atZone(ZoneId.systemDefault()).toLocalTime().toString();

        Map<String, Object> map = new HashMap<>();
        map.put("nodeId", nodeConfig.getId());
        map.put("systemTimeMillis", sys);
        map.put("physicalSystemMillis", sys);
        map.put("artificialSkewMs", skew);
        map.put("artificialSkewMillis", skew);
        map.put("physicalClockOffset", offset);
        map.put("physicalClockOffsetMillis", offset);
        map.put("virtualTimeMillis", virt);
        map.put("physicalVirtualMillis", virt);
        map.put("lamportClock", lamp);
        map.put("lamportTime", lamp);
        map.put("formattedVirtualTime", fmtVirt);
        map.put("formattedSystemTime", fmtSys);
        map.put("isCoordinator", nodeRegistry.getPrimaryId() == nodeConfig.getId());
        map.put("coordinatorId", nodeRegistry.getPrimaryId());
        return map;
    }

    /**
     * Executes the Berkeley Physical Clock Synchronization Algorithm:
     * Coordinator collects clock readings from all live cluster nodes,
     * computes the average time, and distributes relative offsets.
     */
    public Map<String, Object> runBerkeleySync() {
        long startTs = lamportClock.tick();
        int coordId = nodeConfig.getId();

        eventLogger.logWithLamport("EXP3-CLOCK", startTs, String.format(
                "Starting Berkeley Clock Synchronization from Coordinator Node %d...", coordId));

        Map<Integer, Long> nodeTimes = new TreeMap<>();
        // Include self
        long localTime = getVirtualClockMillis();
        nodeTimes.put(coordId, localTime);

        // Poll each live peer
        for (NodeRegistry.PeerState ps : nodeRegistry.getLivePeers()) {
            if (ps.getNodeId() == coordId || ps.getNodeId() <= 0) continue;
            try {
                RemoteDonationService stub = nodeRegistry.getPeerStub(ps.getNodeId());
                if (stub != null) {
                    long peerTime = stub.clockMillis();
                    nodeTimes.put(ps.getNodeId(), peerTime);
                }
            } catch (Exception e) {
                eventLogger.log("EXP3-CLOCK", String.format(
                        "Failed to query clock from Node %d: %s", ps.getNodeId(), e.getMessage()));
            }
        }

        if (nodeTimes.isEmpty()) {
            Map<String, Object> err = new HashMap<>();
            err.put("error", "No active nodes available for clock synchronization");
            return err;
        }

        // Calculate average time across all participating nodes
        double sum = 0;
        for (long t : nodeTimes.values()) {
            sum += t;
        }
        long averageTime = Math.round(sum / nodeTimes.size());

        eventLogger.log("EXP3-CLOCK", String.format(
                "Berkeley Average Computed across %d nodes: %d ms (Clocks: %s)",
                nodeTimes.size(), averageTime, nodeTimes));

        List<Map<String, Object>> adjustments = new ArrayList<>();
        List<Map<String, Object>> nodeClocks = new ArrayList<>();

        // Compute and dispatch offsets
        for (Map.Entry<Integer, Long> entry : nodeTimes.entrySet()) {
            int nId = entry.getKey();
            long reportedTime = entry.getValue();
            long offset = averageTime - reportedTime;

            Map<String, Object> adj = new HashMap<>();
            adj.put("nodeId", nId);
            adj.put("reportedTimeBefore", reportedTime);
            adj.put("clockBeforeMillis", reportedTime);
            adj.put("appliedOffset", offset);
            adj.put("relativeOffsetMillis", offset);
            adj.put("calculatedTimeAfter", reportedTime + offset);
            adj.put("clockAfterMillis", reportedTime + offset);

            boolean success = false;
            String errorMsg = null;

            if (nId == coordId) {
                applyOffset(offset);
                success = true;
            } else {
                try {
                    RemoteDonationService stub = nodeRegistry.getPeerStub(nId);
                    if (stub != null) {
                        stub.applyOffset(offset);
                        success = true;
                    } else {
                        errorMsg = "Peer stub unavailable";
                    }
                } catch (Exception e) {
                    errorMsg = e.getMessage();
                }
            }

            adj.put("success", success);
            adj.put("status", success ? "SYNCHRONIZED" : "FAILED");
            if (errorMsg != null) {
                adj.put("error", errorMsg);
            }

            adjustments.add(adj);
            nodeClocks.add(adj);
        }

        long endTs = lamportClock.tick();
        eventLogger.logWithLamport("EXP3-CLOCK", endTs, String.format(
                "Berkeley Clock Synchronization complete across %d nodes (Synchronized to avg: %d)",
                adjustments.size(), averageTime));

        String fmtAvg = Instant.ofEpochMilli(averageTime).atZone(ZoneId.systemDefault()).toLocalTime().toString();

        Map<String, Object> response = new HashMap<>();
        response.put("coordinatorId", coordId);
        response.put("participatingNodesCount", nodeTimes.size());
        response.put("liveNodeCount", nodeTimes.size());
        response.put("averageTime", averageTime);
        response.put("averageClockMillis", averageTime);
        response.put("formattedAverageTime", fmtAvg);
        response.put("adjustments", adjustments);
        response.put("nodeClocks", nodeClocks);
        response.put("note", "OS clock is never modified; virtual clocks synchronized via applyOffset relative offsets.");

        return response;
    }
}
