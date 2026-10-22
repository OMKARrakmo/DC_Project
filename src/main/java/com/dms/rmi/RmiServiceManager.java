package com.dms.rmi;

import com.dms.core.EventLogger;
import com.dms.core.NodeConfig;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.rmi.Remote;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;

@Component
public class RmiServiceManager {

    private static final Logger log = LoggerFactory.getLogger(RmiServiceManager.class);

    private final NodeConfig nodeConfig;
    private final RemoteDonationServiceImpl serviceImpl;
    private final EventLogger eventLogger;

    private Registry registry;
    private Remote exportedStub;

    public RmiServiceManager(NodeConfig nodeConfig,
                             RemoteDonationServiceImpl serviceImpl,
                             EventLogger eventLogger) {
        this.nodeConfig = nodeConfig;
        this.serviceImpl = serviceImpl;
        this.eventLogger = eventLogger;
    }

    @PostConstruct
    public void start() {
        try {
            // SPEC Section 5: Call System.setProperty("java.rmi.server.hostname", node.host) BEFORE exporting anything
            System.setProperty("java.rmi.server.hostname", nodeConfig.getHost());

            TimeoutClientSocketFactory csf = new TimeoutClientSocketFactory();

            // Create or get registry
            try {
                registry = LocateRegistry.createRegistry(nodeConfig.getRmiPort());
                log.info("Created local RMI registry on port {}", nodeConfig.getRmiPort());
            } catch (Exception e) {
                registry = LocateRegistry.getRegistry(nodeConfig.getRmiPort());
                log.info("Using existing RMI registry on port {}", nodeConfig.getRmiPort());
            }

            // SPEC Section 5: UnicastRemoteObject.exportObject(impl, 0, new TimeoutClientSocketFactory(), null);
            exportedStub = UnicastRemoteObject.exportObject(serviceImpl, 0, csf, null);
            registry.rebind("RemoteDonationService", exportedStub);

            eventLogger.log("EXP1-RMI", String.format(
                    "RMI service exported and bound on rmi://%s:%d/RemoteDonationService",
                    nodeConfig.getHost(), nodeConfig.getRmiPort()));
        } catch (Exception e) {
            log.error("Failed to export RMI service", e);
            throw new RuntimeException("RMI Export failed: " + e.getMessage(), e);
        }
    }

    @PreDestroy
    public void stop() {
        if (exportedStub != null) {
            try {
                registry.unbind("RemoteDonationService");
            } catch (Exception ignored) {
            }
            try {
                UnicastRemoteObject.unexportObject(serviceImpl, true);
            } catch (Exception ignored) {
            }
        }
    }
}
