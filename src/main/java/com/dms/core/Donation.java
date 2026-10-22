package com.dms.core;

import jakarta.persistence.*;
import java.io.Serializable;
import java.math.BigDecimal;

@Entity
@Table(name = "donations")
public class Donation implements Serializable {
    private static final long serialVersionUID = 1L;

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(nullable = false)
    private String donor;

    @Column(nullable = false, precision = 15, scale = 2)
    private BigDecimal amount;

    @Column(nullable = false)
    private String branch;

    @Column(nullable = false)
    private long lamportTs;

    @Column(nullable = false)
    private String receiptId;

    @Column(nullable = false)
    private String handledBy;

    @Column(nullable = false)
    private String status;

    @Column(nullable = false)
    private Long seq = 0L;

    @Column(nullable = false)
    private boolean isolated = false;

    public Donation() {
    }

    public Donation(Long id, String donor, BigDecimal amount, String branch,
                    long lamportTs, String receiptId, String handledBy,
                    String status, Long seq, boolean isolated) {
        this.id = id;
        this.donor = donor;
        this.amount = amount;
        this.branch = branch;
        this.lamportTs = lamportTs;
        this.receiptId = receiptId;
        this.handledBy = handledBy;
        this.status = status;
        this.seq = seq != null ? seq : 0L;
        this.isolated = isolated;
    }

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
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

    public long getLamportTs() {
        return lamportTs;
    }

    public void setLamportTs(long lamportTs) {
        this.lamportTs = lamportTs;
    }

    public String getReceiptId() {
        return receiptId;
    }

    public void setReceiptId(String receiptId) {
        this.receiptId = receiptId;
    }

    public String getHandledBy() {
        return handledBy;
    }

    public void setHandledBy(String handledBy) {
        this.handledBy = handledBy;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public Long getSeq() {
        return seq;
    }

    public void setSeq(Long seq) {
        this.seq = seq;
    }

    public boolean isIsolated() {
        return isolated;
    }

    public void setIsolated(boolean isolated) {
        this.isolated = isolated;
    }

    @Override
    public String toString() {
        return "Donation{" +
                "id=" + id +
                ", donor='" + donor + '\'' +
                ", amount=" + amount +
                ", branch='" + branch + '\'' +
                ", lamportTs=" + lamportTs +
                ", receiptId='" + receiptId + '\'' +
                ", handledBy='" + handledBy + '\'' +
                ", status='" + status + '\'' +
                ", seq=" + seq +
                ", isolated=" + isolated +
                '}';
    }
}
