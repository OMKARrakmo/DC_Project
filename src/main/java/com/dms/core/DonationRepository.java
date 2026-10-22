package com.dms.core;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.math.BigDecimal;
import java.util.List;

@Repository
public interface DonationRepository extends JpaRepository<Donation, Long> {

    List<Donation> findByIsolatedFalseOrderByIdAsc();

    List<Donation> findByIsolatedFalseOrderBySeqAsc();

    java.util.Optional<Donation> findByReceiptId(String receiptId);

    List<Donation> findByBranchAndIsolatedFalse(String branch);

    @Query("SELECT COALESCE(SUM(d.amount), 0) FROM Donation d WHERE LOWER(d.branch) = LOWER(:branch) AND d.isolated = false")
    BigDecimal sumByBranch(@Param("branch") String branch);
}
