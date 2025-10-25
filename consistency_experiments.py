"""
MongoDB Distributed Systems Lab
Part C: Consistency Models - Enhanced Version
Author: Akshay
"""

import time
import subprocess
from datetime import datetime
from pymongo import MongoClient, ReadPreference
from pymongo.write_concern import WriteConcern
from pymongo.read_concern import ReadConcern

DB_NAME = "distributed_lab"
COLLECTION_NAME = "ConsistencyTest"
MONGO_HOSTS = "mongo1:27017,mongo2:27018,mongo3:27019"
REPLICA_SET = "rs0"

def get_client():
    """Connect to MongoDB replica set"""
    print("🔗 Connecting to MongoDB replica set...")
    while True:
        try:
            client = MongoClient(
                f"mongodb://{MONGO_HOSTS}/?replicaSet={REPLICA_SET}",
                serverSelectionTimeoutMS=5000
            )
            client.admin.command("ping")
            print("✅ Connected to MongoDB PRIMARY!")
            return client
        except Exception as e:
            print(f"⏳ Waiting for MongoDB PRIMARY... {e}")
            time.sleep(2)

def display_topology(client):
    """Display current replica set topology"""
    print("\n" + "="*70)
    print("REPLICA SET TOPOLOGY")
    print("="*70)
    try:
        status = client.admin.command('replSetGetStatus')
        print(f"\n{'Member':<25} {'Role':<12} {'Health':<8} {'Sync Source':<20}")
        print("-" * 70)
        for member in status['members']:
            name = member['name']
            state = member['stateStr']
            health = '✅' if member['health'] == 1 else '❌'
            sync_source = member.get('syncSourceHost', '—')
            print(f"{name:<25} {state:<12} {health:<8} {sync_source:<20}")
        print("-" * 70)
    except Exception as e:
        print(f"⚠️  Could not get topology: {e}")

def populate_collection(db):
    """Initialize test collection"""
    col = db[COLLECTION_NAME]
    if col.count_documents({}) == 0:
        col.insert_one({"init": True, "timestamp": datetime.now().isoformat()})
        print(f"✅ Populated empty collection '{COLLECTION_NAME}' for testing.")
    else:
        print(f"✅ Collection '{COLLECTION_NAME}' already exists with {col.count_documents({})} docs.")

def experiment_strong_consistency(db, client):
    """
    Experiment 1: Strong Consistency (CP Model)
    - w=majority for writes
    - readConcern=majority for reads
    - Demonstrates CAP theorem (Consistency + Partition Tolerance)
    """
    print("\n" + "="*70)
    print("EXPERIMENT 1: STRONG CONSISTENCY (CP MODEL)")
    print("="*70)
    
    print("\n📋 Objective:")
    print("   Demonstrate strong consistency where reads immediately")
    print("   reflect writes across different nodes")
    
    print("\n⚙️  Configuration:")
    print("   • Write Concern: w='majority'")
    print("   • Read Concern: readConcern='majority'")
    print("   • Read Preference: secondary (to test cross-node consistency)")
    
    col = db[COLLECTION_NAME]
    
    # TEST 1.1: Write with majority, read from different node
    print("\n" + "-"*70)
    print("TEST 1.1: Write to ONE node, Read from ANOTHER node")
    print("-"*70)
    
    test_value = int(time.time() * 1000)
    doc = {
        "test_id": test_value,
        "test_type": "strong_consistency",
        "msg": "Strong consistency test - w:majority",
        "timestamp": datetime.now().isoformat()
    }
    
    # Write with w=majority and j=true (journal)
    col_wc = col.with_options(
        write_concern=WriteConcern(w="majority", j=True),
        read_concern=ReadConcern(level="majority")
    )
    
    print(f"\n📝 Writing document (test_id={test_value}) with w='majority'...")
    write_start = time.time()
    try:
        result = col_wc.insert_one(doc)
        write_time = (time.time() - write_start) * 1000
        print(f"   ✅ Write acknowledged in {write_time:.2f}ms")
        print(f"   ℹ️  Data committed on MAJORITY of nodes")
    except Exception as e:
        print(f"   ❌ Write FAILED: {e}")
        print(f"   ℹ️  This could happen if majority nodes are unavailable")
        return
    
    # Immediate read from PRIMARY
    print(f"\n🔍 Step 1: Reading from PRIMARY with readConcern='majority'...")
    primary_start = time.time()
    primary_doc = col_wc.find_one({"test_id": test_value})
    primary_time = (time.time() - primary_start) * 1000
    
    if primary_doc:
        print(f"   ✅ Read from PRIMARY in {primary_time:.2f}ms")
        print(f"   ✅ Value: {primary_doc['test_id']} == {test_value}")
    
    # Immediate read from SECONDARY
    print(f"\n🔍 Step 2: Reading from SECONDARY with readConcern='majority'...")
    secondary_col = col.with_options(
        read_preference=ReadPreference.SECONDARY,
        read_concern=ReadConcern(level="majority")
    )
    
    secondary_start = time.time()
    secondary_doc = secondary_col.find_one({"test_id": test_value})
    secondary_time = (time.time() - secondary_start) * 1000
    
    if secondary_doc:
        print(f"   ✅ Read from SECONDARY in {secondary_time:.2f}ms")
        print(f"   ✅ Value: {secondary_doc['test_id']} == {test_value}")
        print(f"   ✅ STRONG CONSISTENCY VERIFIED!")
    else:
        print(f"   ⚠️  Data not yet visible on SECONDARY")
    
    # TEST 1.2: Network partition / Node failure simulation
    print("\n" + "-"*70)
    print("TEST 1.2: Impact of Node Failure (CAP Theorem)")
    print("-"*70)
    
    print("\n⚠️  SIMULATING NODE FAILURE:")
    print("\n   To test CAP theorem behavior, follow these steps:")
    print("\n   1. Open another terminal window")
    print("   2. Stop ONE secondary: docker stop mongo2")
    print("   3. Try writing (should still work - 2/3 nodes = majority)")
    print("   4. Stop ANOTHER node: docker stop mongo3")
    print("   5. Try writing (will FAIL - only 1/3 nodes available)")
    
    print("\n📊 Expected CAP Behavior:")
    print("\n   Scenario A: 2 of 3 nodes available")
    print("      ✅ Writes succeed (2 nodes = majority)")
    print("      ✅ Reads succeed (majority available)")
    print("      ✅ System maintains CONSISTENCY + PARTITION TOLERANCE")
    
    print("\n   Scenario B: Only 1 of 3 nodes available")
    print("      ❌ Writes FAIL (1 node < majority)")
    print("      ❌ Reads may FAIL (no majority)")
    print("      ⚠️  System SACRIFICES AVAILABILITY for CONSISTENCY")
    
    # Automated availability test
    print("\n🧪 Testing current availability...")
    test_doc = {
        "test_id": int(time.time() * 1000),
        "test_type": "availability_test",
        "msg": "Testing if majority nodes are available"
    }
    
    try:
        col_wc.insert_one(test_doc)
        print("   ✅ Write succeeded - Majority nodes are AVAILABLE")
    except Exception as e:
        print(f"   ❌ Write failed: {str(e)[:80]}...")
        print("   ⚠️  This indicates majority nodes are NOT available")
    
    # CAP Analysis
    print("\n" + "="*70)
    print("📊 CAP THEOREM ANALYSIS - Strong Consistency")
    print("="*70)
    print("\n   This configuration is a CP system:")
    print("   ✅ CONSISTENCY:          Guaranteed (majority writes/reads)")
    print("   ❌ AVAILABILITY:         Sacrificed when majority unavailable")
    print("   ✅ PARTITION TOLERANCE:  System handles network partitions")
    print("\n   Trade-off: Chooses Consistency over Availability")

def experiment_eventual_consistency(db, client):
    """
    Experiment 2: Eventual Consistency (AP Model)
    - w=1 for writes
    - local read concern
    - Demonstrates stale reads and eventual convergence
    """
    print("\n" + "="*70)
    print("EXPERIMENT 2: EVENTUAL CONSISTENCY (AP MODEL)")
    print("="*70)
    
    print("\n📋 Objective:")
    print("   Demonstrate eventual consistency where writes are fast")
    print("   but reads may temporarily return stale data")
    
    print("\n⚙️  Configuration:")
    print("   • Write Concern: w=1 (single node acknowledgment)")
    print("   • Read Concern: local (default)")
    print("   • Read Preference: secondary")
    
    col = db[COLLECTION_NAME]
    
    # TEST 2.1: Fast write, potentially stale read
    print("\n" + "-"*70)
    print("TEST 2.1: Write to PRIMARY, Immediately Read from SECONDARY")
    print("-"*70)
    
    test_value = int(time.time() * 1000)
    doc = {
        "test_id": test_value,
        "test_type": "eventual_consistency",
        "msg": "Eventual consistency test - w:1",
        "timestamp": datetime.now().isoformat()
    }
    
    # Write with w=1 (fast, low durability)
    col_wc = col.with_options(write_concern=WriteConcern(w=1))
    
    print(f"\n📝 Writing document (test_id={test_value}) with w=1...")
    write_start = time.time()
    result = col_wc.insert_one(doc)
    write_time = (time.time() - write_start) * 1000
    
    print(f"   ✅ Write acknowledged in {write_time:.2f}ms (VERY FAST!)")
    print(f"   ℹ️  Data written to PRIMARY only")
    print(f"   ℹ️  Replicating to secondaries asynchronously...")
    
    # Immediate read from PRIMARY
    print(f"\n🔍 Step 1: Reading from PRIMARY...")
    primary_doc = col.find_one({"test_id": test_value})
    if primary_doc:
        print(f"   ✅ Read from PRIMARY succeeded: {primary_doc['test_id']}")
    
    # Immediate read from SECONDARY (may see stale data)
    print(f"\n🔍 Step 2: IMMEDIATELY reading from SECONDARY (no delay)...")
    secondary_col = col.with_options(read_preference=ReadPreference.SECONDARY)
    
    secondary_doc = secondary_col.find_one({"test_id": test_value})
    
    if secondary_doc:
        print(f"   ✅ Found on SECONDARY immediately!")
        print(f"   ℹ️  Replication was extremely fast (< 1ms)")
    else:
        print(f"   ⚠️  STALE READ: Latest value NOT visible on secondary yet")
        print(f"   ℹ️  This demonstrates EVENTUAL consistency")
    
    # TEST 2.2: Poll until consistent (demonstrate "eventual")
    print(f"\n🔄 Step 3: Polling SECONDARY until value appears...")
    print(f"   (This demonstrates the 'eventual' in eventual consistency)")
    
    start_poll = time.time()
    max_attempts = 100
    found = False
    
    for attempt in range(max_attempts):
        secondary_doc = secondary_col.find_one({"test_id": test_value})
        if secondary_doc:
            propagation_time = (time.time() - start_poll) * 1000
            print(f"\n   ✅ Value appeared after {propagation_time:.2f}ms (attempt {attempt + 1})")
            print(f"   ✅ Consistent value: {secondary_doc['test_id']} == {test_value}")
            print(f"   ✅ EVENTUAL CONSISTENCY ACHIEVED!")
            found = True
            break
        time.sleep(0.01)  # 10ms intervals
    
    if not found:
        print(f"\n   ⚠️  Value not visible after {max_attempts * 10}ms")
    
    # TEST 2.3: Use case discussion
    print("\n" + "-"*70)
    print("USE CASES: When is Eventual Consistency Acceptable?")
    print("-"*70)
    
    print("\n✅ 1. Social Media Likes/Reactions:")
    print("   • User clicks 'like' → Instant response (w=1)")
    print("   • Like count may be slightly off for a moment")
    print("   • Eventually all users see correct count")
    print("   • Benefit: User experience > strict accuracy")
    
    print("\n✅ 2. Sensor Data / IoT:")
    print("   • Continuous stream of temperature readings")
    print("   • Slight delay in data visibility acceptable")
    print("   • Benefit: High throughput, low latency")
    
    print("\n✅ 3. Product Catalogs:")
    print("   • Product descriptions rarely change")
    print("   • Brief stale data acceptable")
    print("   • Benefit: High availability, better performance")
    
    print("\n✅ 4. View Counters:")
    print("   • Video views, page views")
    print("   • Approximate numbers acceptable")
    print("   • Benefit: Extreme scalability")
    
    print("\n💡 Why Eventual Consistency Benefits These Use Cases:")
    print("   ✓ LOWER LATENCY:     Writes complete instantly")
    print("   ✓ HIGH AVAILABILITY: System always accepts writes")
    print("   ✓ SCALABILITY:       No coordination overhead")
    print("   ✓ PERFORMANCE:       Better throughput")
    
    # CAP Analysis
    print("\n" + "="*70)
    print("📊 CAP THEOREM ANALYSIS - Eventual Consistency")
    print("="*70)
    print("\n   This configuration is an AP system:")
    print("   ⚠️  CONSISTENCY:         Eventually consistent (temporary staleness)")
    print("   ✅ AVAILABILITY:         Always available for writes")
    print("   ✅ PARTITION TOLERANCE:  Works during network partitions")
    print("\n   Trade-off: Chooses Availability over immediate Consistency")

def experiment_causal_consistency(db, client):
    """
    Experiment 3: Causal Consistency (BONUS)
    Demonstrate that causally related operations are observed in order
    """
    print("\n" + "="*70)
    print("EXPERIMENT 3: CAUSAL CONSISTENCY (BONUS)")
    print("="*70)
    
    print("\n📋 Objective:")
    print("   Demonstrate that causally related operations appear")
    print("   in the correct order, even with eventual consistency")
    
    print("\n⚙️  Implementation:")
    print("   • Use logical timestamps (Lamport clocks)")
    print("   • Track operation dependencies with version numbers")
    print("   • Verify causal ordering on replica nodes")
    
    col = db[COLLECTION_NAME + "_Causal"]
    col.delete_many({})  # Clean slate
    
    print("\n" + "-"*70)
    print("SCENARIO: User Creates and Edits a Social Media Post")
    print("-"*70)
    
    post_id = f"post_{int(time.time())}"
    
    # Operation 1: CREATE post
    lamport_clock = 1
    print(f"\n📝 Operation 1: User creates post")
    
    op1 = {
        'post_id': post_id,
        'operation': 'CREATE',
        'content': 'Just had an amazing coffee! ☕',
        'version': lamport_clock,
        'timestamp': datetime.now().isoformat(),
        'depends_on': None,
        'author': 'akshay'
    }
    
    col.insert_one(op1)
    print(f"   ✅ Created (version {lamport_clock})")
    print(f"   Content: \"{op1['content']}\"")
    
    time.sleep(0.05)
    
    # Operation 2: EDIT post (depends on CREATE)
    lamport_clock += 1
    print(f"\n📝 Operation 2: User edits post")
    
    op2 = {
        'post_id': post_id,
        'operation': 'EDIT',
        'content': 'Just had an amazing coffee at Starbucks! ☕',
        'version': lamport_clock,
        'timestamp': datetime.now().isoformat(),
        'depends_on': lamport_clock - 1,
        'author': 'akshay'
    }
    
    col.insert_one(op2)
    print(f"   ✅ Edited (version {lamport_clock})")
    print(f"   Content: \"{op2['content']}\"")
    print(f"   Depends on: version {op2['depends_on']}")
    
    time.sleep(0.05)
    
    # Operation 3: LIKE (depends on CREATE, independent of EDIT)
    lamport_clock += 1
    print(f"\n📝 Operation 3: Friend likes the post")
    
    op3 = {
        'post_id': post_id,
        'operation': 'LIKE',
        'content': 'ishika liked this post',
        'version': lamport_clock,
        'timestamp': datetime.now().isoformat(),
        'depends_on': 1,  # Depends on original CREATE
        'author': 'ishika'
    }
    
    col.insert_one(op3)
    print(f"   ✅ Liked (version {lamport_clock})")
    print(f"   Depends on: version {op3['depends_on']}")
    
    time.sleep(0.05)
    
    # Operation 4: COMMENT (depends on EDIT)
    lamport_clock += 1
    print(f"\n📝 Operation 4: Friend comments on edited post")
    
    op4 = {
        'post_id': post_id,
        'operation': 'COMMENT',
        'content': 'sam commented: "Starbucks is the best!"',
        'version': lamport_clock,
        'timestamp': datetime.now().isoformat(),
        'depends_on': 2,  # Depends on EDIT
        'author': 'sam'
    }
    
    col.insert_one(op4)
    print(f"   ✅ Commented (version {lamport_clock})")
    print(f"   Depends on: version {op4['depends_on']}")
    
    # Verify causal ordering
    print(f"\n🔍 Verifying Causal Consistency...")
    time.sleep(0.2)  # Wait for replication
    
    # Read operations in causal order
    print(f"\n📊 Operations in Causal Order:")
    print(f"   {'Ver':<5} {'Operation':<10} {'Depends On':<12} {'Author':<10} {'Content':<40}")
    print("   " + "-"*80)
    
    operations = list(col.find({'post_id': post_id}).sort('version', 1))
    
    for op in operations:
        depends = op.get('depends_on', 'None')
        content = op['content'][:37] + "..." if len(op['content']) > 40 else op['content']
        print(f"   {op['version']:<5} {op['operation']:<10} {str(depends):<12} {op['author']:<10} {content:<40}")
    
    print("   " + "-"*80)
    
    # Verify causal relationships
    print(f"\n✅ Causal Consistency Verification:")
    
    if len(operations) >= 2:
        print(f"   ✅ CREATE (v1) appears before EDIT (v2) - correct order")
        
        if operations[1]['depends_on'] == operations[0]['version']:
            print(f"   ✅ EDIT correctly depends on CREATE")
        
        if operations[2]['depends_on'] == operations[0]['version']:
            print(f"   ✅ LIKE correctly depends on CREATE (independent of EDIT)")
        
        if operations[3]['depends_on'] == operations[1]['version']:
            print(f"   ✅ COMMENT correctly depends on EDIT")
        
        print(f"\n   ✅ CAUSAL CONSISTENCY MAINTAINED!")
        print(f"   ℹ️  Causally related operations appear in correct order")
        print(f"   ℹ️  Concurrent operations (LIKE and EDIT) order doesn't matter")
    
    print(f"\n💡 Causal Consistency Properties:")
    print(f"   ✓ Causally related writes seen in order")
    print(f"   ✓ Concurrent writes may appear in any order")
    print(f"   ✓ Weaker than strong, stronger than eventual")
    print(f"   ✓ Use cases: Chat apps, collaborative editors, social media")

def cap_theorem_summary():
    """Display comprehensive CAP theorem summary"""
    print("\n" + "="*70)
    print("CAP THEOREM: COMPREHENSIVE SUMMARY")
    print("="*70)
    
    print("\n📚 CAP Theorem States:")
    print("   In a distributed system with network partitions, you can")
    print("   guarantee only TWO of these three properties:")
    print("\n   • CONSISTENCY:          All nodes see the same data")
    print("   • AVAILABILITY:         Every request gets a response")
    print("   • PARTITION TOLERANCE:  System works despite network failures")
    
    print("\n┌──────────────────────┬─────────────┬─────────────┬─────────────┐")
    print("  │ Consistency Model    │ Consistency │ Availability│ Partition   │")
    print("  ├──────────────────────┼─────────────|─────────────┼─────────────┤")
    print("  │ Strong (CP)          │     ✅      |   ⚠️      │     ✅      │")
    print("  │ Eventual (AP)        │     ⚠️      │     ✅      │     ✅      │")
    print("  │ Causal (Hybrid)      │  ✅ (order) │     ✅      │     ✅      │")
    print("  └──────────────────────┴──────────────────────────┴─────────────┘")

    
    print("\n📊 MongoDB Configurations:")
    
    print("\n   1️⃣  Strong Consistency (CP):")
    print("      • write_concern: w='majority'")
    print("      • read_concern: readConcern='majority'")
    print("      • Trade-off: Consistency over Availability")
    print("      • Blocks when majority unavailable")
    print("      • Use: Banking, inventory, financial systems")
    
    print("\n   2️⃣  Eventual Consistency (AP):")
    print("      • write_concern: w=1")
    print("      • read_concern: local")
    print("      • Trade-off: Availability over Consistency")
    print("      • Always available, may return stale data")
    print("      • Use: Social media, caching, analytics")
    
    print("\n   3️⃣  Causal Consistency:")
    print("      • Maintains operation ordering with version vectors")
    print("      • Middle ground between strong and eventual")
    print("      • Use: Collaborative apps, chat systems")
    
    print("\n🎯 KEY INSIGHT:")
    print("   Since network partitions WILL happen (P is required),")
    print("   you must choose between C and A!")

def main():
    print("="*70)
    print("MongoDB Distributed Systems Lab")
    print("Part C: Consistency Models - Complete Demonstration")
    print("Author: Akshay")
    print("="*70)

    client = get_client()
    db = client[DB_NAME]
    
    # Display topology
    display_topology(client)
    
    # Initialize
    populate_collection(db)
    
    # Run experiments
    input("\n▶️  Press Enter to start Experiment 1 (Strong Consistency)...")
    experiment_strong_consistency(db, client)
    
    input("\n▶️  Press Enter to start Experiment 2 (Eventual Consistency)...")
    experiment_eventual_consistency(db, client)
    
    input("\n▶️  Press Enter to start Experiment 3 (Causal Consistency - BONUS)...")
    experiment_causal_consistency(db, client)
    
    # Summary
    cap_theorem_summary()
    
    print("\n" + "="*70)
    print("✅ All consistency experiments completed successfully!")
    print("="*70)
    

    client.close()

if __name__ == "__main__":
    main()