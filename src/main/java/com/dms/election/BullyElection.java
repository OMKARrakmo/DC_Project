package com.dms.election;

import com.dms.core.EventLogger;
import com.dms.core.LamportClock;
import com.dms.core.NodeConfig;
import com.dms.core.NodeRegistry;
import com.dms.rmi.RemoteDonationService;
import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

@Component("bullyElection")
public class BullyElection implements ElectionAlgorithm {

    private final NodeConfig nodeConfig;
    private final NodeRegistry nodeRegistry;
    private final EventLogger eventLogger;
    private final LamportClock lamportClock;
    private final AtomicBoolean electionInProgress = new AtomicBoolean(false);
    private final ExecutorService executor = Executors.newCachedThreadPool();

    public BullyElection(NodeConfig nodeConfig,
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
        return "BULLY";
    }

    @Override
    public void startElection() {
        if (!electionInProgress.compareAndSet(false, true)) {
            // Election already running
            return;
        }

        executor.submit(() -> {
            try {
                long ts = lamportClock.tick();
                int myId = nodeConfig.getId();
                eventLogger.logWithLamport("EXP4-ELECT", ts,
                        String.format("Starting Bully election (myId=%d)", myId));

                List<Integer> higherNodeIds = new ArrayList<>();
                for (NodeRegistry.PeerState ps : nodeRegistry.getKnownPeers()) {
                    if (ps.getNodeId() > myId) {
                        higherNodeIds.add(ps.getNodeId());
                    }
                }

                if (higherNodeIds.isEmpty()) {
                    // No higher nodes exist: this node wins immediately
                    becomeCoordinator();
                    return;
                }

                boolean higherAnswered = false;
                for (int higherId : higherNodeIds) {
                    try {
                        RemoteDonationService stub = nodeRegistry.getPeerStub(higherId);
                        if (stub != null) {
                            eventLogger.log("EXP4-ELECT", String.format(
                                    "Sent ELECTION message to higher node=%d", higherId));
                            stub.electionMsg(myId);
                            higherAnswered = true;
                            eventLogger.log("EXP4-ELECT", String.format(
                                    "Received OK from higher node=%d", higherId));
                        }
                    } catch (Exception e) {
                        eventLogger.log("EXP4-ELECT", String.format(
                                "Higher node=%d failed to respond to electionMsg (%s)", higherId, e.getMessage()));
                    }
                }

                if (!higherAnswered) {
                    // None of the higher nodes answered: become coordinator
                    becomeCoordinator();
                } else {
                    // Higher node answered: wait up to 4s for COORDINATOR message
                    eventLogger.log("EXP4-ELECT",
                            "Higher node answered OK. Waiting for COORDINATOR announcement...");
                    Thread.sleep(4000);
                    // If coordinator is still not set or dead, re-evaluate
                    int currentCoord = nodeRegistry.getPrimaryId();
                    if (currentCoord <= myId) {
                        eventLogger.log("EXP4-ELECT",
                                "Timeout waiting for higher node COORDINATOR message. Re-evaluating leadership.");
                        becomeCoordinator();
                    }
                }
            } catch (Exception e) {
                eventLogger.log("EXP4-ELECT", "Bully election error: " + e.getMessage());
            } finally {
                electionInProgress.set(false);
            }
        });
    }

    private void becomeCoordinator() {
        int myId = nodeConfig.getId();
        long ts = lamportClock.tick();
        nodeRegistry.setPrimaryId(myId);

        eventLogger.logWithLamport("EXP4-ELECT", ts, String.format(
                "Promoted to COORDINATOR (primaryId=%d). Broadcasting COORDINATOR message to all peers...", myId));

        for (NodeRegistry.PeerState ps : nodeRegistry.getKnownPeers()) {
            if (ps.getNodeId() > 0 && ps.getNodeId() != myId) {
                try {
                    RemoteDonationService stub = nodeRegistry.getPeerStub(ps.getNodeId());
                    if (stub != null) {
                        stub.coordinatorMsg(myId);
                        eventLogger.log("EXP4-ELECT", String.format(
                                "Sent COORDINATOR message to peer node=%d", ps.getNodeId()));
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
        long ts = lamportClock.tick();
        int myId = nodeConfig.getId();
        eventLogger.logWithLamport("EXP4-ELECT", ts, String.format(
                "Received ELECTION message from node=%d", fromNodeId));

        if (myId > fromNodeId) {
            eventLogger.log("EXP4-ELECT", String.format(
                    "Sending OK to node=%d (myId=%d > fromId=%d). Starting local election.",
                    fromNodeId, myId, fromNodeId));
            startElection();
        }
    }

    @Override
    public void handleCoordinatorMessage(int newCoordinatorId) {
        long ts = lamportClock.tick();
        nodeRegistry.setPrimaryId(newCoordinatorId);
        eventLogger.logWithLamport("EXP4-ELECT", ts, String.format(
                "COORDINATOR announcement accepted: Node %d is now PRIMARY", newCoordinatorId));
    }
}
