package com.dms.core;

import java.io.Serializable;
import java.math.BigDecimal;

public class DonationRequest implements Serializable {
    private static final long serialVersionUID = 1L;

    private String donor;
    private BigDecimal amount;
    private String branch;
    private boolean isolated = false;
    private long clientLamportTs = 0L;
    private Integer targetNodeId; // Optional: specify target node to route via RMI
    private Integer delayMs;      // Optional: artificial processing delay in ms

    public DonationRequest() {
    }

    public DonationRequest(String donor, BigDecimal amount, String branch) {
        this.donor = donor;
        this.amount = amount;
        this.branch = branch;
    }

    public DonationRequest(String donor, BigDecimal amount, String branch, boolean isolated, long clientLamportTs, Integer targetNodeId) {
        this.donor = donor;
        this.amount = amount;
        this.branch = branch;
        this.isolated = isolated;
        this.clientLamportTs = clientLamportTs;
        this.targetNodeId = targetNodeId;
    }

    public Integer getDelayMs() {
        return delayMs;
    }

    public void setDelayMs(Integer delayMs) {
        this.delayMs = delayMs;
    }

    public String getDonor() {
        return donor;
    }

    public void setDonor(String donor) {
        this.donor = donor;
    }

    public BigDecimal getAmount() {
        return amount;
    }

    public void setAmount(BigDecimal amount) {
        this.amount = amount;
    }

    public String getBranch() {
        return branch;
    }

    public void setBranch(String branch) {
        this.branch = branch;
    }

    public boolean isIsolated() {
        return isolated;
    }

    public void setIsolated(boolean isolated) {
        this.isolated = isolated;
    }

    public long getClientLamportTs() {
        return clientLamportTs;
    }

    public void setClientLamportTs(long clientLamportTs) {
        this.clientLamportTs = clientLamportTs;
    }

    public Integer getTargetNodeId() {
        return targetNodeId;
    }

    public void setTargetNodeId(Integer targetNodeId) {
        this.targetNodeId = targetNodeId;
    }

    @Override
    public String toString() {
        return "DonationRequest{" +
                "donor='" + donor + '\'' +
                ", amount=" + amount +
                ", branch='" + branch + '\'' +
                ", isolated=" + isolated +
                ", clientLamportTs=" + clientLamportTs +
                ", targetNodeId=" + targetNodeId +
                ", delayMs=" + delayMs +
                '}';
    }
}
