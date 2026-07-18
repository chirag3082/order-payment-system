package com.chirag.orderpayment.order.repo;

import com.chirag.orderpayment.order.domain.ProcessedResult;
import org.springframework.data.jpa.repository.JpaRepository;

public interface ProcessedResultRepository extends JpaRepository<ProcessedResult, String> {
}
