package com.dms.election;

import com.dms.core.EventLogger;
import com.dms.core.LamportClock;
import com.dms.core.NodeConfig;
import com.dms.core.NodeRegistry;
import com.dms.rmi.RemoteDonationService;
import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Component;

import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

@Component("ringElection")
public class RingElection implements ElectionAlgorithm {

    private final NodeConfig nodeConfig;
    private final NodeRegistry nodeRegistry;
    private final EventLogger eventLogger;
    private final LamportClock lamportClock;
    private final AtomicBoolean electionInProgress = new AtomicBoolean(false);
    private final ExecutorService executor = Executors.newCachedThreadPool();

    public RingElection(NodeConfig nodeConfig,
                        @Lazy NodeRegistry nodeRegistry,
                        EventLogger eventLogger,
                        LamportClock lamportClock) {
        this.nodeConfig = nodeConfig;
        this.nodeRegistry = nodeRegistry;
        this.eventLogger = eventLogger;
        this.lamportClock = lamportClock;
    }

    @Override
    public String getAlgorithmName() {
        return "RING";
    }

    @Override
    public void startElection() {
        if (!electionInProgress.compareAndSet(false, true)) {
            return;
        }

        executor.submit(() -> {
            try {
                long ts = lamportClock.tick();
                int myId = nodeConfig.getId();
                eventLogger.logWithLamport("EXP4-ELECT", ts,
                        String.format("Starting Ring election (myId=%d)", myId));

                List<Integer> ring = getLiveRingNodeIds();
                if (ring.size() <= 1) {
                    // Only this node is live
                    becomeCoordinator();
                    return;
                }

                List<Integer> visited = new ArrayList<>();
                visited.add(myId);

                forwardTokenToSuccessor(visited, ring);
            } catch (Exception e) {
                eventLogger.log("EXP4-ELECT", "Ring election error: " + e.getMessage());
            } finally {
                electionInProgress.set(false);
            }
        });
    }

    public void handleRingElectionMessage(List<Integer> visitedIds) {
        long ts = lamportClock.tick();
        int myId = nodeConfig.getId();
        eventLogger.logWithLamport("EXP4-ELECT", ts, String.format(
                "Received Ring token with visited nodes: %s", visitedIds));

        if (visitedIds.contains(myId)) {
            // Token completed a full cycle around the ring!
            int winner = Collections.max(visitedIds);
            eventLogger.logWithLamport("EXP4-ELECT", lamportClock.tick(), String.format(
                    "Ring token completed circuit %s. Elected coordinator Node %d (highest active ID).",
                    visitedIds, winner));

            nodeRegistry.setPrimaryId(winner);
            broadcastCoordinator(winner);
        } else {
            // Append self to the visited list and forward to next successor in the ring
            List<Integer> updatedList = new ArrayList<>(visitedIds);
            updatedList.add(myId);

            List<Integer> ring = getLiveRingNodeIds();
            eventLogger.log("EXP4-ELECT", String.format(
                    "Node %d added to Ring token (%s). Forwarding to next successor in ring...",
                    myId, updatedList));

            forwardTokenToSuccessor(updatedList, ring);
        }
    }

    private void forwardTokenToSuccessor(List<Integer> tokenList, List<Integer> ring) {
        int myId = nodeConfig.getId();
        int myIndex = ring.indexOf(myId);
        int ringSize = ring.size();

        if (ringSize <= 1 || myIndex < 0) {
            becomeCoordinator();
            return;
        }

        boolean sent = false;
        // Try successive nodes in the ring until an alive peer accepts the token
        for (int i = 1; i < ringSize; i++) {
            int successorId = ring.get((myIndex + i) % ringSize);
            if (successorId == myId) break;

            try {
                RemoteDonationService stub = nodeRegistry.getPeerStub(successorId);
                if (stub != null) {
                    eventLogger.log("EXP4-ELECT", String.format(
                            "Sent Ring token %s to successor node=%d", tokenList, successorId));
                    stub.ringElectionMsg(tokenList);
                    sent = true;
                    break;
                }
            } catch (Exception e) {
                eventLogger.log("EXP4-ELECT", String.format(
                        "Successor node=%d failed to accept Ring token (%s). Skipping to next in ring...",
                        successorId, e.getMessage()));
            }
        }

        if (!sent) {
            // No successor could be reached: become coordinator
            becomeCoordinator();
        }
    }

    private void becomeCoordinator() {
        int myId = nodeConfig.getId();
        long ts = lamportClock.tick();
        nodeRegistry.setPrimaryId(myId);

        eventLogger.logWithLamport("EXP4-ELECT", ts, String.format(
                "Promoted to COORDINATOR via Ring election (primaryId=%d). Broadcasting COORDINATOR message to all peers...", myId));

        broadcastCoordinator(myId);
    }

    private void broadcastCoordinator(int coordinatorId) {
        for (NodeRegistry.PeerState ps : nodeRegistry.getKnownPeers()) {
            if (ps.getNodeId() > 0 && ps.getNodeId() != nodeConfig.getId()) {
                try {
                    RemoteDonationService stub = nodeRegistry.getPeerStub(ps.getNodeId());
                    if (stub != null) {
                        stub.coordinatorMsg(coordinatorId);
                        eventLogger.log("EXP4-ELECT", String.format(
                                "Sent COORDINATOR message to peer node=%d (New primary=%d)", ps.getNodeId(), coordinatorId));
                    }
                } catch (Exception e) {
                    eventLogger.log("EXP4-ELECT", String.format(
                            "Failed to send COORDINATOR to node=%d (%s)", ps.getNodeId(), e.getMessage()));
                }
            }
        }
    }

    @Override
    public void handleElectionMessage(int fromNodeId) {
        // Fallback for generic election message
        startElection();
    }

    @Override
    public void handleCoordinatorMessage(int newCoordinatorId) {
        long ts = lamportClock.tick();
        nodeRegistry.setPrimaryId(newCoordinatorId);
        eventLogger.logWithLamport("EXP4-ELECT", ts, String.format(
                "COORDINATOR announcement accepted (Ring): Node %d is now PRIMARY", newCoordinatorId));
    }

    private List<Integer> getLiveRingNodeIds() {
        Set<Integer> set = new TreeSet<>();
        set.add(nodeConfig.getId());

        for (NodeRegistry.PeerState ps : nodeRegistry.getKnownPeers()) {
            if (ps.getNodeId() > 0 && ps.isAlive()) {
                set.add(ps.getNodeId());
            }
        }
        return new ArrayList<>(set);
    }
}
