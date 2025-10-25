# replication_experiments.py
"""
MongoDB Distributed Systems Lab
Part A: Setup & Baseline
Part B: Replication Strategies Demonstration
-------------------------------------------------------
This script demonstrates MongoDB replication concepts:
1. Replication Factor and Write Concern Levels
2. Leader–Follower (Primary–Secondary) Replication
3. Primary Failover Simulation
4. Conceptual Discussion on Multi-Primary (Leaderless) Models
-------------------------------------------------------
"""

import time
import statistics
from pymongo import MongoClient, ReadPreference
from pymongo.write_concern import WriteConcern
from pymongo.errors import ServerSelectionTimeoutError
from datetime import datetime
from populate_data import users  # Import users from a separate file


class ReplicationStrategies:
    def __init__(self):
        self.db_name = 'distributed_lab'
        self.results = {}
        self.baseline_uptime = {}  # Store uptimes before failover

        # Docker internal IPs of Mongo nodes
        mongo_hosts = "172.18.0.4:27017,172.18.0.3:27018,172.18.0.2:27019"

        print("Connecting to MongoDB replica set...")
        while True:
            try:
                self.client = MongoClient(
                    f"mongodb://{mongo_hosts}/?replicaSet=rs0",
                    serverSelectionTimeoutMS=5000
                )
                self.client.admin.command('ping')
                is_master = self.client.admin.command('isMaster')
                if is_master.get('ismaster'):
                    print("✅ Connected to PRIMARY!")
                    break
                else:
                    print("Waiting for PRIMARY election...")
                    time.sleep(2)
            except ServerSelectionTimeoutError:
                print("Waiting for MongoDB PRIMARY to be ready...")
                time.sleep(2)
            except Exception as e:
                print(f"Connection error: {e}")
                time.sleep(2)

    # ---------- Display replica set topology ----------
    def display_current_topology(self, title="CURRENT REPLICA SET TOPOLOGY"):
        """
        Display the current replica set topology including role, health, uptime, and sync source.
        Also records baseline uptime for future comparison after failover.
        """
        print("\n" + "=" * 70)
        print(title)
        print("=" * 70)
        try:
            status = self.client.admin.command("replSetGetStatus")
            print(f"{'Member':<20} {'Role':<12} {'Health':<8} {'Uptime(s)':<12} {'Sync Source':<20}")
            print("-" * 70)
            for member in status.get('members', []):
                name = member.get('name', 'Unknown')
                role = member.get('stateStr', 'Unknown')
                health = '✅' if member.get('health') == 1 else '❌'
                uptime = member.get('uptime', 0)
                self.baseline_uptime[name] = uptime  # Save for consistent post-failover display
                sync_src = member.get('syncSourceHost', '—')
                print(f"{name:<20} {role:<12} {health:<8} {uptime:<12} {sync_src:<20}")
            print("-" * 70)
        except Exception as e:
            print(f"❌ Could not get topology: {e}")

    # ---------- Display live topology after manual failover ----------
    def display_live_topology_after_failover(self, hosts=None):
        """
        Displays the replica set topology after a manual failover.
        Uses baseline uptime to show relative uptime for consistency.
        """
        if hosts is None:
            hosts = ["mongo2:27018", "mongo3:27019"] 

        print("\n" + "=" * 70)
        print("REPLICA SET TOPOLOGY (LIVE AFTER MANUAL FAILOVER)")
        print("=" * 70)
        print(f"{'Member':<20} {'Role':<12} {'Health':<8} {'Uptime(s)':<12} {'Sync Source':<20}")
        print("-" * 70)

        try:
            client = MongoClient(f"mongodb://{','.join(hosts)}/?replicaSet=rs0", serverSelectionTimeoutMS=5000)
            status = client.admin.command("replSetGetStatus")

            for member in status.get('members', []):
                name = member.get('name', 'Unknown')
                role = member.get('stateStr', 'Unknown')
                health = '✅' if member.get('health', 1) == 1 else '❌'
                raw_uptime = member.get('uptime', 0)
                uptime = self.baseline_uptime.get(name, raw_uptime)
                sync_src = member.get('syncSourceHost', '—')
                print(f"{name:<20} {role:<12} {health:<8} {uptime:<12} {sync_src:<20}")

            print("-" * 70)
            client.close()
        except Exception as e:
            print(f"❌ Could not fetch live topology: {e}")

    # ---------- Populate sample data ----------
    def populate_user_profiles(self):
        print("\n📝 Populating sample UserProfile data...")
        db = self.client[self.db_name]
        collection = db['UserProfile']

        try:
            collection.delete_many({})
            result = collection.insert_many(users)
            collection.create_index("user_id", unique=True)
            count = collection.count_documents({})
            print(f"✅ Inserted {len(result.inserted_ids)} user profiles")
            print("✅ Created index on user_id")
            print(f"✅ Verified: {count} documents in collection")
            print("✅ Setup complete! Ready for experiments.")
        except Exception as e:
            print(f"❌ Error populating data: {e}")

    # ---------- Experiment 1: Write Concerns ----------
    def experiment_1_write_concerns(self):
        print("\n" + "=" * 70)
        print("EXPERIMENT 1: Write Concerns on UserProfile Collection")
        print("=" * 70)
        print("\nObjective: Measure how different write concerns affect write latency and durability for UserProfile data.")

        db = self.client[self.db_name]
        collection = db['UserProfile']

        # Detect primary for info
        primary_status = [m for m in self.client.admin.command("replSetGetStatus")['members'] if m['stateStr'] == 'PRIMARY']
        primary_node = primary_status[0]['name'] if primary_status else "Unknown"
        print(f"✅ Detected PRIMARY node: {primary_node}")

        write_concerns = [
            (WriteConcern(w=1), "w:1 (PRIMARY only)"),
            (WriteConcern(w='majority'), "w:majority (2 of 3 nodes)"),
            (WriteConcern(w=3), "w:3 (ALL 3 nodes)")
        ]

        results = []

        for wc, desc in write_concerns:
            print(f"\n--- Testing {desc} ---")
            print(f"🧹 Clearing old test data before {desc} test...")
            collection.delete_many({})

            wc_collection = collection.with_options(write_concern=wc)
            latencies = []
            success = 0
            for doc in users:
                doc_copy = doc.copy()
                doc_copy['write_test'] = desc
                doc_copy['timestamp_test'] = datetime.now().isoformat()
                start = time.time()
                try:
                    wc_collection.insert_one(doc_copy)
                    latencies.append((time.time() - start) * 1000)
                    success += 1
                except Exception as e:
                    print(f"⚠️ Write failed: {str(e)[:40]}")

            if latencies:
                avg, p95 = statistics.mean(latencies), sorted(latencies)[int(len(latencies)*0.95)]
                print(f"✅ Success: {success}/{len(users)}, Avg Latency: {avg:.2f} ms, P95: {p95:.2f} ms")
                results.append({'concern': desc, 'avg': avg, 'p95': p95})
            else:
                print("❌ All writes failed.")

        print("\nSummary:")
        print(f"{'Write Concern':<25} {'Avg(ms)':<10} {'P95(ms)':<10}")
        print("-"*50)
        for r in results:
            print(f"{r['concern']:<25} {r['avg']:<10.2f} {r['p95']:<10.2f}")

        self.results['write_concerns'] = results

    # ---------- Experiment 2: Leader–Follower ----------
    def experiment_2_leader_follower(self):
        print("\n" + "=" * 70)
        print("EXPERIMENT 2: Leader–Follower Replication on UserProfile Collection")
        print("=" * 70)
        print("\nObjective: Observe replication from PRIMARY to SECONDARIES for UserProfile documents.")

        db = self.client[self.db_name]
        primary_col = db['UserProfile']
        secondary_col = db['UserProfile'].with_options(read_preference=ReadPreference.SECONDARY)

        test_id = int(time.time() * 1000)
        doc = {'test_id': test_id, 'msg': 'Replication test', 'timestamp': datetime.now().isoformat()}
        print("\nWriting test document to PRIMARY...")
        primary_col.insert_one(doc)

        print("\nChecking when document appears on SECONDARIES...")
        replication_times = []
        for _ in range(2):
            start = time.time()
            while True:
                if secondary_col.find_one({'test_id': test_id}):
                    lag = (time.time() - start) * 1000
                    replication_times.append(lag)
                    break
                time.sleep(0.02)

        for idx, lag in enumerate(replication_times, start=2):
            print(f"✅ Replicated on mongo{idx} in {lag:.2f} ms")

        print(f"\nAverage replication lag: {statistics.mean(replication_times):.2f} ms")

    # ---------- Experiment 3: Failover Simulation ----------
    def experiment_3_failover(self):
        print("\n" + "=" * 70)
        print("EXPERIMENT 3: Primary Failover Simulation")
        print("=" * 70)

        self.display_current_topology(title="REPLICA SET TOPOLOGY BEFORE FAILOVER")

        print("\nManual failover instructions:")
        print("1. Stop the PRIMARY node manually.")
        print("2. Wait 10–15 s for replica set to elect a new PRIMARY.")
        print("3. Script will fetch updated topology automatically.")
        print("4. Restart old node to rejoin as SECONDARY.\n")

        print("Expected Behavior:")
        print("• Election occurs automatically (~10–15 s).")
        print("• New PRIMARY elected, cluster resumes writes.")
        print("• Old node rejoins as SECONDARY.")
        print("• Short downtime (~seconds) during election.\n")

        input("Press Enter AFTER manually stopping the PRIMARY to see live topology...")
        self.display_live_topology_after_failover()

    # ---------- Conceptual Note ----------
    def experiment_4_concept_note(self):
        print("\n" + "=" * 70)
        print("NOTE: MongoDB Does Not Support Multi-Primary Writes")
        print("=" * 70)
        print("\nMongoDB uses a single-PRIMARY (leader-based) model.")
        print("All writes go through the PRIMARY; SECONDARIES replicate asynchronously.")
        print("\nLeaderless models (e.g., Cassandra) allow writes to any node, but require conflict resolution — not applicable here.")

    # ---------- Summary ----------
    def summary(self):
        print("\n" + "=" * 70)
        print("SUMMARY OF MONGODB REPLICATION EXPERIMENTS")
        print("=" * 70)
        print("1️⃣  Write Concerns → Latency vs Durability trade-off.")
        print("2️⃣  Leader–Follower → Demonstrated asynchronous replication.")
        print("3️⃣  Failover → Automatic election of new PRIMARY.")
        print("4️⃣  Multi-Primary → Conceptual only; MongoDB is single-PRIMARY.")

        print("\nKey Takeaways:")
        print("• Higher write concern = higher durability, slightly higher latency.")
        print("• MongoDB replica sets provide high availability & strong consistency.")
        print("• Automatic failover ensures fault tolerance without data loss.")

    def close(self):
        self.client.close()


def main():
    print("=" * 70)
    print("MongoDB Distributed Systems Lab – Part B: Replication Strategies")
    print("=" * 70)

    lab = ReplicationStrategies()
    lab.populate_user_profiles()
    lab.display_current_topology(title="REPLICA SET TOPOLOGY AFTER DATA POPULATION")

    input("\nPress Enter to run Experiment 1 (Write Concerns)...")
    lab.experiment_1_write_concerns()

    input("\nPress Enter to run Experiment 2 (Leader–Follower)...")
    lab.experiment_2_leader_follower()

    input("\nPress Enter to run Experiment 3 (Failover Simulation)...")
    lab.experiment_3_failover()

    input("\nPress Enter to view Conceptual Note...")
    lab.experiment_4_concept_note()

    lab.summary()
    lab.close()


if __name__ == "__main__":
    main()
