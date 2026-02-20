# Enterprise Connectivity Guide

## 1. VPC Peering (Cloud-to-Cloud)
**Target Audience**: Hedge funds and proprietary trading firms hosted on AWS.

### Architecture
- **Model**: Private Service Provider (Torii Data) <-> Consumer (Client).
- **Routing**: Traffic stays entirely within the AWS global backbone, bypassing the public internet.
- **Latency**: Sub-millisecond network latency between Availability Zones (AZs).

### Setup Process
1.  **Initiation**: Client requests peering with Torii Data VPC (`vpc-xxxxxxxx`).
2.  **Handshake**: Torii Data accepts the peering connection (`pcx-xxxxxxxx`).
3.  **Routing**:
    -   Torii Route Table: Add route to `Client_CIDR` via `pcx-id`.
    -   Client Route Table: Add route to `Torii_CIDR` via `pcx-id`.
4.  **Security**:
    -   Traffic is whitelisted in `sg-institutional` Security Group.
    -   Only TCP ports `8080` (API), `9002` (Gateway), and `9800` (FIX) are exposed.

## 2. AWS Direct Connect (On-Premise to Cloud)
**Target Audience**: High-Frequency Trading (HFT) firms with colocation presence (e.g., NY4, LD4, CH2).

### Architecture
-   **Physical Link**: dedicated 1Gbps or 10Gbps cross-connect fiber.
-   **Virtual Interface (VIF)**: Private VIF attached directly to the Torii Data VPC or Transit Gateway.
-   **Redundancy**: Dual Direct Connect links in separate locations for HA.

### Benefits
-   **Deterministic Latency**: Consistent networking performance with minimal jitter (<1ms).
-   **Throughput**: Guaranteed bandwidth allocation.
-   **Security**: Dedicated fiber path, isolated from public internet threats.

## 3. Network Security & Compliance
-   **Zero Trust**: All connections, including private VPC peers, must use TLS 1.3 encryption.
-   **Authentication**: Mutual TLS (mTLS) optional for Institutional tier.
-   **IP Whitelisting**: Strict ingress rules. Only registered institutional CIDRs can access the Gateway.
-   **DDoS Protection**: AWS Shield Advanced protection on all public and private endpoints.
