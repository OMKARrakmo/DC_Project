package com.dms.election;

public interface ElectionAlgorithm {

    void startElection();

    void handleElectionMessage(int fromNodeId);

    void handleCoordinatorMessage(int newCoordinatorId);

    String getAlgorithmName();
}
