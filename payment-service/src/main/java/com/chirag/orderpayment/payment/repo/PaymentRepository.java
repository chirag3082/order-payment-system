package com.chirag.orderpayment.payment.repo;

import com.chirag.orderpayment.payment.domain.Payment;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.Optional;

public interface PaymentRepository extends JpaRepository<Payment, String> {
    Optional<Payment> findFirstByOrderId(String orderId);
}
