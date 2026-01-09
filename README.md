# Midas Core – Transaction Processing System

Midas Core is a Spring Boot–based backend service that processes financial transactions using Kafka, validates them against user balances, stores them in a relational database, integrates with an external incentive service, and exposes a REST API for querying user balances.

This project was completed as part of the JPMorgan Chase & Co. Forage Software Engineering Virtual Experience.

---

## 🚀 What Midas Core Does

- Consumes transaction messages from a Kafka topic
- Validates transactions based on:
  - Valid sender and recipient
  - Sufficient sender balance
- Stores valid transactions using JPA and an H2 in-memory database
- Integrates with an external Incentive API via REST
- Applies incentive amounts to recipients (without deducting from senders)
- Exposes a REST endpoint to query user balances

---

## 🛠 Tech Stack

- Java 17
- Spring Boot
- Spring Kafka
- Spring Data JPA (Hibernate)
- H2 In-Memory Database
- REST APIs (RestTemplate)
- Apache Kafka
- Maven

---



