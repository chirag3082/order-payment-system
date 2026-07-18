package com.chirag.orderpayment.order.repo;

import com.chirag.orderpayment.order.domain.Order;
import org.springframework.data.jpa.repository.JpaRepository;

public interface OrderRepository extends JpaRepository<Order, String> {
}
