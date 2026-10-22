package com.dms;

import com.dms.core.*;
import com.dms.rmi.RemoteDonationService;
import com.dms.rmi.TimeoutClientSocketFactory;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.math.BigDecimal;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;

@SpringBootTest(properties = {
    "node.id=1",
    "server.port=0",
    "rmi.port=11099",
    "peers="
})
public class RmiDonationServiceTest {

    @Autowired
    private RemoteDonationService remoteService;

    @Autowired
    private DonationRepository donationRepository;

    @Autowired
    private LamportClock lamportClock;

    @Autowired
    private NodeConfig nodeConfig;

    @Test
    void testRmiLookupAndPing() throws Exception {
        Registry registry = LocateRegistry.getRegistry("127.0.0.1", nodeConfig.getRmiPort(), new TimeoutClientSocketFactory());
        RemoteDonationService service = (RemoteDonationService) registry.lookup("RemoteDonationService");

        NodeInfo info = service.ping();
        Assertions.assertNotNull(info);
        Assertions.assertEquals(1, info.getNodeId());
        Assertions.assertTrue(info.isAlive());
    }

    @Test
    void testProcessValidDonation() throws Exception {
        DonationRequest request = new DonationRequest("Omkar Babar", new BigDecimal("500.00"), "Mumbai");
        request.setClientLamportTs(lamportClock.get());

        Receipt receipt = remoteService.process(request);

        Assertions.assertNotNull(receipt);
        Assertions.assertEquals(1, receipt.getProcessingNodeId());
        Assertions.assertEquals("CONFIRMED", receipt.getStatus());
        Assertions.assertNotNull(receipt.getThreadName());
        Assertions.assertNotNull(receipt.getOs());
        Assertions.assertTrue(receipt.getLamportTs() > 0);

        BigDecimal mumbaiTotal = remoteService.branchTotal("Mumbai");
        Assertions.assertTrue(mumbaiTotal.compareTo(new BigDecimal("500.00")) >= 0);
    }

    @Test
    void testValidationRejection() throws Exception {
        // Amount below minimum 100
        DonationRequest invalidAmount = new DonationRequest("Omkar", new BigDecimal("50.00"), "Mumbai");
        Receipt r1 = remoteService.process(invalidAmount);
        Assertions.assertEquals("REJECTED", r1.getStatus());

        // Unknown branch
        DonationRequest invalidBranch = new DonationRequest("Omkar", new BigDecimal("500.00"), "Delhi");
        Receipt r2 = remoteService.process(invalidBranch);
        Assertions.assertEquals("REJECTED", r2.getStatus());
    }
}
