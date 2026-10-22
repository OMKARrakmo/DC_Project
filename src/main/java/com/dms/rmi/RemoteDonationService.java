package com.dms.rmi;

import com.dms.core.Donation;
import com.dms.core.DonationRequest;
import com.dms.core.NodeInfo;
import com.dms.core.Receipt;

import java.math.BigDecimal;
import java.rmi.Remote;
import java.rmi.RemoteException;
import java.util.List;
import java.util.Map;

public interface RemoteDonationService extends Remote {

    NodeInfo ping() throws RemoteException;

    Receipt process(DonationRequest r) throws RemoteException;

    Receipt commit(Donation d) throws RemoteException;

    void replicate(Donation d) throws RemoteException;

    BigDecimal branchTotal(String branch) throws RemoteException;

    void electionMsg(int fromId) throws RemoteException;

    void coordinatorMsg(int newCoordinatorId) throws RemoteException;

    long clockMillis() throws RemoteException;

    void applyOffset(long millis) throws RemoteException;

    Map<String, BigDecimal> mapPhase(List<Integer> liveSortedIds) throws RemoteException;

    List<Donation> snapshot() throws RemoteException;
}
