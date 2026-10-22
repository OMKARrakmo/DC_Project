package com.dms.lb;

import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

@Component("roundRobinLoadBalancer")
public class RoundRobinLoadBalancer implements LoadBalancer {

    private final AtomicInteger index = new AtomicInteger(0);

    @Override
    public int selectNode(List<Integer> candidateNodeIds, Map<Integer, Integer> activeConnections) {
        if (candidateNodeIds == null || candidateNodeIds.isEmpty()) {
            throw new IllegalArgumentException("Candidate node list cannot be empty");
        }
        int idx = Math.abs(index.getAndIncrement() % candidateNodeIds.size());
        return candidateNodeIds.get(idx);
    }

    @Override
    public String getStrategyName() {
        return "ROUND_ROBIN";
    }

    public void reset() {
        index.set(0);
    }
}
