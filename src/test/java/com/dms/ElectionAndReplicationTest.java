package com.dms;

import com.dms.core.*;
import com.dms.election.BullyElection;
import com.dms.repl.ReplicationService;
import com.dms.rmi.RemoteDonationService;
import com.dms.rmi.RemoteDonationServiceImpl;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.math.BigDecimal;
import java.rmi.RemoteException;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

@SpringBootTest
public class ElectionAndReplicationTest {

    @Autowired
    private NodeConfig nodeConfig;

    @Autowired
    private NodeRegistry nodeRegistry;

    @Autowired
    private DonationRepository donationRepository;

    @Autowired
    private ReplicationService replicationService;

    @Autowired
    private BullyElection bullyElection;

    @Autowired
    private RemoteDonationServiceImpl serviceImpl;

    @BeforeEach
    public void setup() {
        donationRepository.deleteAll();
        nodeRegistry.setSimulatedCrash(false);
        nodeRegistry.setPrimaryId(nodeConfig.getId());
        replicationService.setReplicationMode(ReplicationService.MODE_SYNCHRONOUS);
        replicationService.setAsyncDelayMs(100);
    }

    @Test
    public void testBullyElectionBasicContract() {
        assertEquals("BULLY", bullyElection.getAlgorithmName());

        // Coordinator message updates primary ID
        bullyElection.handleCoordinatorMessage(99);
        assertEquals(99, nodeRegistry.getPrimaryId());

        // Reset back to self
        nodeRegistry.setPrimaryId(nodeConfig.getId());
    }

    @Test
    public void testReplicationModeAndDelayConfiguration() {
        replicationService.setReplicationMode(ReplicationService.MODE_ASYNCHRONOUS);
        assertEquals(ReplicationService.MODE_ASYNCHRONOUS, replicationService.getReplicationMode());

        replicationService.setAsyncDelayMs(1500);
        assertEquals(1500, replicationService.getAsyncDelayMs());

        replicationService.setReplicationMode(ReplicationService.MODE_SYNCHRONOUS);
        assertEquals(ReplicationService.MODE_SYNCHRONOUS, replicationService.getReplicationMode());
    }

    @Test
    public void testLedgerHashConsistency() throws Exception {
        // Empty hash
        assertEquals("EMPTY", replicationService.computeLedgerHash());

        // Add donation
        Donation d1 = new Donation(null, "Donor A", new BigDecimal("500.00"), "Mumbai", 1L, "RCP-1111", "node-1", "COMMITTED", 1L, false);
        donationRepository.save(d1);

        String hash1 = replicationService.computeLedgerHash();
        assertNotNull(hash1);
        assertNotEquals("EMPTY", hash1);
        assertEquals(64, hash1.length()); // SHA-256 hex is 64 characters

        // Replicating same record gives deterministic hash
        String hashAgain = replicationService.computeLedgerHash();
        assertEquals(hash1, hashAgain);
    }

    @Test
    public void testSimulatedCrashRejectsRmiCalls() {
        nodeRegistry.setSimulatedCrash(true);
        assertTrue(nodeRegistry.isSimulatedCrash());

        assertThrows(RemoteException.class, () -> serviceImpl.ping());
        assertThrows(RemoteException.class, () -> serviceImpl.branchTotal("Mumbai"));
        assertThrows(RemoteException.class, () -> serviceImpl.snapshot());

        nodeRegistry.setSimulatedCrash(false);
        assertFalse(nodeRegistry.isSimulatedCrash());

        assertDoesNotThrow(() -> serviceImpl.ping());
    }

    @Test
    public void testReplicatePreservesSequenceAndReceipt() throws Exception {
        Donation d = new Donation(
                null,
                "Donor Replicate",
                new BigDecimal("1000.00"),
                "Pune",
                10L,
                "RCP-REPL-123",
                "node-1",
                "COMMITTED",
                5L,
                false
        );

        serviceImpl.replicate(d);

        List<Donation> list = donationRepository.findByIsolatedFalseOrderBySeqAsc();
        assertEquals(1, list.size());
        Donation stored = list.get(0);
        assertEquals("RCP-REPL-123", stored.getReceiptId());
        assertEquals(5L, stored.getSeq());
        assertEquals("COMMITTED", stored.getStatus());
        assertEquals(new BigDecimal("1000.00"), stored.getAmount());
    }
}
