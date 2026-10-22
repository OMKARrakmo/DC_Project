package com.dms;

import com.dms.lb.LeastConnectionsLoadBalancer;
import com.dms.lb.LoadBalancingService;
import com.dms.lb.RoundRobinLoadBalancer;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.util.*;
import java.util.concurrent.*;

@SpringBootTest(properties = {
    "node.id=1",
    "server.port=0",
    "rmi.port=12099",
    "peers="
})
public class LoadBalancerTest {

    @Autowired
    private RoundRobinLoadBalancer roundRobinLB;

    @Autowired
    private LeastConnectionsLoadBalancer leastConnectionsLB;

    @Autowired
    private LoadBalancingService loadBalancingService;

    @Test
    void testRoundRobinCyclesEvenly() {
        List<Integer> candidates = List.of(1, 2, 3);
        Map<Integer, Integer> conns = Map.of(1, 0, 2, 0, 3, 0);

        roundRobinLB.reset();
        Map<Integer, Integer> distribution = new HashMap<>();

        for (int i = 0; i < 12; i++) {
            int selected = roundRobinLB.selectNode(candidates, conns);
            distribution.put(selected, distribution.getOrDefault(selected, 0) + 1);
        }

        Assertions.assertEquals(4, distribution.get(1), "Node 1 should receive 4 requests");
        Assertions.assertEquals(4, distribution.get(2), "Node 2 should receive 4 requests");
        Assertions.assertEquals(4, distribution.get(3), "Node 3 should receive 4 requests");
    }

    @Test
    void testLeastConnectionsSelectsMinimum() {
        List<Integer> candidates = List.of(1, 2, 3);

        // Case 1: Node 2 has the lowest connections
        Map<Integer, Integer> conns1 = Map.of(1, 5, 2, 1, 3, 4);
        int selected1 = leastConnectionsLB.selectNode(candidates, conns1);
        Assertions.assertEquals(2, selected1);

        // Case 2: Node 3 has the lowest connections
        Map<Integer, Integer> conns2 = Map.of(1, 3, 2, 4, 3, 0);
        int selected2 = leastConnectionsLB.selectNode(candidates, conns2);
        Assertions.assertEquals(3, selected2);
    }

    @Test
    void testLeastConnectionsSpreadsOverlappingRequests() {
        List<Integer> candidates = List.of(1, 2, 3);
        leastConnectionsLB.reset();

        // Simulate 12 concurrent requests where selected node's connection count increments
        Map<Integer, Integer> activeConns = new ConcurrentHashMap<>(Map.of(1, 0, 2, 0, 3, 0));
        Map<Integer, Integer> distribution = new HashMap<>();

        for (int i = 0; i < 12; i++) {
            int selected = leastConnectionsLB.selectNode(candidates, activeConns);
            distribution.put(selected, distribution.getOrDefault(selected, 0) + 1);
            activeConns.put(selected, activeConns.get(selected) + 1);
        }

        // Each node should receive exactly 4 requests because each selection balanced the lowest
        Assertions.assertEquals(4, distribution.get(1));
        Assertions.assertEquals(4, distribution.get(2));
        Assertions.assertEquals(4, distribution.get(3));
    }

    @Test
    void testLoadBalancingServiceStrategyAndDelayConfiguration() {
        loadBalancingService.setStrategy("ROUND_ROBIN");
        Assertions.assertEquals("ROUND_ROBIN", loadBalancingService.getStrategy());

        loadBalancingService.setStrategy("LEAST_CONNECTIONS");
        Assertions.assertEquals("LEAST_CONNECTIONS", loadBalancingService.getStrategy());

        loadBalancingService.setArtificialDelayMs(250);
        Assertions.assertEquals(250, loadBalancingService.getArtificialDelayMs());
        loadBalancingService.setArtificialDelayMs(0);
    }
}
