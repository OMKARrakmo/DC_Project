package com.dms;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;

@SpringBootTest(properties = {
    "node.id=99",
    "server.port=0",
    "rmi.port=10999",
    "peers="
})
class DmsApplicationTests {

    @Test
    void contextLoads() {
    }
}
