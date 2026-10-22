package com.dms.rmi;

import java.io.IOException;
import java.io.Serializable;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.rmi.server.RMIClientSocketFactory;

public class TimeoutClientSocketFactory implements RMIClientSocketFactory, Serializable {
    private static final long serialVersionUID = 1L;

    @Override
    public Socket createSocket(String host, int port) throws IOException {
        Socket s = new Socket();
        s.connect(new InetSocketAddress(host, port), 2000);  // connect timeout
        s.setSoTimeout(4000);                                 // read timeout
        return s;
    }

    @Override
    public boolean equals(Object obj) {
        return obj != null && getClass() == obj.getClass();
    }

    @Override
    public int hashCode() {
        return getClass().hashCode();
    }
}
