package com.dms.election;

import com.dms.core.EventLogger;
import com.dms.core.LamportClock;
import com.dms.core.NodeConfig;
import org.springframework.context.annotation.Primary;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

@Service
@Primary
public class ElectionManager implements ElectionAlgorithm {

    private final BullyElection bullyElection;
    private final RingElection ringElection;
    private final EventLogger eventLogger;
    private final LamportClock lamportClock;
    private final NodeConfig nodeConfig;

    private final AtomicReference<String> currentAlgorithm = new AtomicReference<>("BULLY");

    public ElectionManager(BullyElection bullyElection,
                           RingElection ringElection,
                           EventLogger eventLogger,
                           LamportClock lamportClock,
                           NodeConfig nodeConfig) {
        this.bullyElection = bullyElection;
        this.ringElection = ringElection;
        this.eventLogger = eventLogger;
        this.lamportClock = lamportClock;
        this.nodeConfig = nodeConfig;
    }

    public String getAlgorithm() {
        return currentAlgorithm.get();
    }

    @Override
    public String getAlgorithmName() {
        return currentAlgorithm.get();
    }

    public void setAlgorithm(String algorithm) {
        if (algorithm == null) return;
        String upper = algorithm.trim().toUpperCase();
        if ("RING".equals(upper)) {
            currentAlgorithm.set("RING");
        } else {
            currentAlgorithm.set("BULLY");
        }
        long ts = lamportClock.tick();
        eventLogger.logWithLamport("EXP4-ELECT", ts, String.format(
                "Election algorithm switched to: %s", currentAlgorithm.get()));
    }

    @Override
    public void startElection() {
        if ("RING".equals(currentAlgorithm.get())) {
            ringElection.startElection();
        } else {
            bullyElection.startElection();
        }
    }

    @Override
    public void handleElectionMessage(int fromNodeId) {
        if ("RING".equals(currentAlgorithm.get())) {
            ringElection.handleElectionMessage(fromNodeId);
        } else {
            bullyElection.handleElectionMessage(fromNodeId);
        }
    }

    @Override
    public void handleCoordinatorMessage(int newCoordinatorId) {
        if ("RING".equals(currentAlgorithm.get())) {
            ringElection.handleCoordinatorMessage(newCoordinatorId);
        } else {
            bullyElection.handleCoordinatorMessage(newCoordinatorId);
        }
    }

    public void handleRingElectionMessage(List<Integer> visitedIds) {
        currentAlgorithm.set("RING");
        ringElection.handleRingElectionMessage(visitedIds);
    }

    public BullyElection getBullyElection() {
        return bullyElection;
    }

    public RingElection getRingElection() {
        return ringElection;
    }
}
