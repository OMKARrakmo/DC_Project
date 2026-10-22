package com.dms.lb;

import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

@Component("leastConnectionsLoadBalancer")
public class LeastConnectionsLoadBalancer implements LoadBalancer {

    private final AtomicInteger tieBreaker = new AtomicInteger(0);

    @Override
    public int selectNode(List<Integer> candidateNodeIds, Map<Integer, Integer> activeConnections) {
        if (candidateNodeIds == null || candidateNodeIds.isEmpty()) {
            throw new IllegalArgumentException("Candidate node list cannot be empty");
        }

        int minConns = Integer.MAX_VALUE;
        List<Integer> tiedCandidates = new ArrayList<>();

        for (int node : candidateNodeIds) {
            int conns = activeConnections.getOrDefault(node, 0);
            if (conns < minConns) {
                minConns = conns;
                tiedCandidates.clear();
                tiedCandidates.add(node);
            } else if (conns == minConns) {
                tiedCandidates.add(node);
            }
        }

        if (tiedCandidates.size() == 1) {
            return tiedCandidates.get(0);
        }

        // Break ties evenly
        int idx = Math.abs(tieBreaker.getAndIncrement() % tiedCandidates.size());
        return tiedCandidates.get(idx);
    }

    @Override
    public String getStrategyName() {
        return "LEAST_CONNECTIONS";
    }

    public void reset() {
        tieBreaker.set(0);
    }
}
