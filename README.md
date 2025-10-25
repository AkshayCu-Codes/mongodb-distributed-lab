# MongoDB Distributed Systems Lab

A comprehensive hands-on laboratory project demonstrating distributed database concepts using MongoDB replica sets, including replication strategies, consistency models, transaction patterns, and real-time monitoring.

## 🎯 Overview
This lab project provides practical experience with MongoDB's distributed systems capabilities through four comprehensive experiments exploring replication strategies, consistency models, transaction patterns, and real-time monitoring of replica set health.

---
## 📊 Architecture Diagrams
### Diagram 1: MongoDB Replica Set Architecture
```mermaid
graph TB
    Client[Client Application]
    
    subgraph Cluster["Replica Set: rs0"]
        Primary["PRIMARY
        mongo1:27017
        172.18.0.4"]
        Secondary1["SECONDARY
        mongo2:27018
        172.18.0.3"]
        Secondary2["SECONDARY
        mongo3:27019
        172.18.0.2"]
    end
    
    Client -->|Writes| Primary
    Client -.->|Reads optional| Secondary1
    Client -.->|Reads optional| Secondary2
    
    Primary ==>|Async Replication via Oplog| Secondary1
    Primary ==>|Async Replication via Oplog| Secondary2
    
    Secondary1 -.->|Heartbeat| Primary
    Secondary2 -.->|Heartbeat| Primary
    
    style Primary fill:#4CAF50,stroke:#2E7D32,stroke-width:3px,color:#fff
    style Secondary1 fill:#2196F3,stroke:#1565C0,stroke-width:2px,color:#fff
    style Secondary2 fill:#2196F3,stroke:#1565C0,stroke-width:2px,color:#fff
    style Client fill:#FF9800,stroke:#E65100,stroke-width:2px,color:#fff
```
### Diagram 2: CAP Theorem Trade-offs
```mermaid
graph LR
    subgraph CAP["CAP Theorem"]
        C["Consistency
        All nodes see same data"]
        A["Availability
        Every request gets response"]
        P["Partition Tolerance
        Works despite network failures"]
    end
    
    subgraph Models["Consistency Models"]
        Strong["Strong Consistency CP Model
        w:majority
        readConcern:majority"]
        Eventual["Eventual Consistency AP Model
        w:1
        readConcern:local"]
        Causal["Causal Consistency Hybrid
        Version vectors
        Operation ordering"]
    end
    
    C --> Strong
    P --> Strong
    
    A --> Eventual
    P --> Eventual
    
    C -.-> Causal
    A -.-> Causal
    P --> Causal
    
    style C fill:#4CAF50,stroke:#2E7D32,stroke-width:2px
    style A fill:#2196F3,stroke:#1565C0,stroke-width:2px
    style P fill:#FF9800,stroke:#E65100,stroke-width:2px
    style Strong fill:#E8F5E9,stroke:#4CAF50,stroke-width:2px
    style Eventual fill:#E3F2FD,stroke:#2196F3,stroke-width:2px
    style Causal fill:#FFF3E0,stroke:#FF9800,stroke-width:2px
```
### Diagram 3: Primary Failover Process
```mermaid
sequenceDiagram
    participant C as Client
    participant M1 as mongo1 PRIMARY
    participant M2 as mongo2 SECONDARY
    participant M3 as mongo3 SECONDARY
    
    Note over M1,M3: Normal Operation
    C->>M1: Write Request
    M1->>M2: Replicate
    M1->>M3: Replicate
    M1->>C: Acknowledge
    
    Note over M1: PRIMARY FAILS
    rect rgba(238, 19, 19, 1)
        M1--xM2: Heartbeat Timeout
        M1--xM3: Heartbeat Timeout
    end
    
    Note over M2,M3: Election Process 10-15s
    M2->>M3: Request Vote
    M3->>M2: Grant Vote
    
    Note over M2: NEW PRIMARY ELECTED
    rect rgba(28, 231, 28, 1)
        M2->>M2: Become PRIMARY
        C->>M2: Write Request
        M2->>M3: Replicate
        M2->>C: Acknowledge
    end
    
    Note over M1: Old PRIMARY Rejoins
    M1->>M2: Heartbeat
    M2->>M1: You are SECONDARY
    M1->>M1: Become SECONDARY
```

### Diagram 4: Lab Experiment Flow
```mermaid
graph LR
    Setup["Setup
    Docker Compose
    Initialize Replica Set"]
    
    PartB["Part B
    Replication
    Experiments"]
    PartC["Part C
    Consistency
    Models"]
    PartD["Part D
    Transaction
    Patterns"]
    Bonus["Bonus
    Monitoring
    Dashboard"]
    
    B1[Write Concerns]
    B2[Leader-Follower]
    B3[Failover]
    
    C1[Strong CP]
    C2[Eventual AP]
    C3[Causal]
    
    D1[ACID Demo]
    D2[Saga Demo]
    D3[Comparison]
    
    Setup --> PartB
    PartB --> PartC
    PartC --> PartD
    PartD --> Bonus
    
    PartB --> B1 --> B2 --> B3
    PartC --> C1 --> C2 --> C3
    PartD --> D1 --> D2 --> D3
    
    style Setup fill:#9C27B0,stroke:#6A1B9A,stroke-width:2px,color:#fff
    style PartB fill:#2196F3,stroke:#1565C0,stroke-width:2px,color:#fff
    style PartC fill:#4CAF50,stroke:#2E7D32,stroke-width:2px,color:#fff
    style PartD fill:#FF9800,stroke:#E65100,stroke-width:2px,color:#fff
    style Bonus fill:#F44336,stroke:#C62828,stroke-width:2px,color:#fff
```
## 📁 Lab Structure
```
mongodb-distributed-lab/
├── replication_experiments.py      
├── consistency_experiments.py      
├── transaction_analysis.py         
├── monitoring_dashboard.py         
├── populate_data.py                
├── requirements.txt                
├── docker-compose.yml              
└── README.md                       
```

---

## ⚙️ Prerequisites

### Required Software
- Docker and Docker Compose (v20.10+)
- Python 3.8 or higher
- pip package manager

### System Requirements
- 4GB RAM minimum (8GB recommended)
- 10GB free disk space

---

## Part A: Installation & Setup

### Step 1: Install Python Dependencies
```bash
pip install -r requirements.txt
```

### Step 2: Start MongoDB Replica Set
Create a docker-compose.yml file with three MongoDB nodes (mongo1, mongo2, mongo3) configured as a replica set named "rs0".
```bash
docker-compose up -d
```

### Step 3: Initialize Replica Set
Connect to the primary node and initialize the replica set with three members.
```bash
docker exec -it mongo1 mongosh
```
Run initialization commands and verify the cluster status.

---

## Lab Experiments

### Part B: Replication Strategies
**Experiments:**
- Write Concerns - Compare w:1, w:majority, and w:3 configurations
- Leader-Follower Replication - Observe replication lag between primary and secondaries
- Primary Failover - Simulate automatic election when primary fails
- Multi-Primary Discussion - Conceptual comparison with leaderless systems

```bash
# Run replication experiments
python replication_experiments.py
```
**Interactive Steps:**
- Follow prompts to execute each experiment sequentially
- For failover experiment, manually stop primary node when instructed
- Observe automatic election and topology changes

---

### Part C: Consistency Models
**Experiments:**
- Strong Consistency (CP Model) - w:majority, readConcern:majority
- Eventual Consistency (AP Model) - w:1, readConcern:local with stale reads demonstration
- Causal Consistency (Bonus) - Lamport clocks and operation ordering

```bash
# Run consistency experiments
python consistency_experiments.py
```
**Interactive Steps:**
- Press Enter to proceed through each consistency model
- Observe CAP theorem trade-offs in real-time
- Compare consistency vs availability behaviors

---

### Part D: Transaction Patterns
**Demonstrations:**
- ACID Transaction Pattern - E-commerce order processing with atomic operations
- Saga Pattern - Same workflow with compensating transactions
- Comparison Analysis - Side-by-side evaluation of both approaches

```bash
# Run transaction analysis
python transaction_analysis.py
```
**Covers:**
- ACID properties demonstration
- Saga pattern with compensation logic
- Trade-offs between transaction models

---

### Bonus: Real-Time Monitoring
**Features:**
- Live replication lag visualization
- Node health status timeline
- Auto-refresh dashboard with matplotlib

```bash
# Launch real-time dashboard
python monitoring_dashboard.py

# Quick status check
python monitoring_dashboard.py --status
```

---

## Key Concepts Covered

### Distributed Systems
- CAP Theorem (Consistency, Availability, Partition Tolerance)
- Replication strategies and consensus protocols
- Eventual consistency and convergence
- Network partitions and fault tolerance

### MongoDB Architecture
- Replica sets and primary-secondary topology
- Write concerns and read preferences
- Read concerns and consistency levels
- Oplog-based replication

### Transaction Models
- ACID properties and distributed transactions
- Saga pattern with compensating transactions
- Trade-offs between consistency and availability

---

## 🔧 Troubleshooting

### Common Issues

**Connection Timeout**
- Wait 15–20 seconds after replica set initialization for election to complete

**Replica Set Not Initialized**
- Verify cluster status and re-initialize if needed

**Port Already in Use**
- Check and modify ports in docker-compose.yml or stop conflicting services

**Python Module Not Found**
- Reinstall dependencies from requirements.txt

**Docker Network Issues**
- Restart containers and prune networks

### Verification Commands
```bash
# Check containers
docker ps

# Check replica set status
docker exec -it mongo1 mongosh --eval "rs.status()"

# View logs
docker logs mongo1
```

---

## Requirements File
```
pymongo==4.6.0
docker==7.0.0
matplotlib==3.8.2
numpy==1.26.2
```

---

## 👤 Author
**Akshay Channapla Udaya Kumar**  
Educational project for distributed systems learning.
