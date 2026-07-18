-- Creates isolated databases and users for each service (database-per-service).
CREATE USER "order" WITH PASSWORD 'order';
CREATE DATABASE orderdb OWNER "order";

CREATE USER payment WITH PASSWORD 'payment';
CREATE DATABASE paymentdb OWNER payment;
