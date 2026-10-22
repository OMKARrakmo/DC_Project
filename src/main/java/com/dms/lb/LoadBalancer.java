package com.dms.lb;

import java.util.List;
import java.util.Map;

public interface LoadBalancer {
    int selectNode(List<Integer> candidateNodeIds, Map<Integer, Integer> activeConnections);
    String getStrategyName();
}
