"""
MongoDB Distributed Systems Lab - Distributed Transactions Analysis
Part D: Conceptual comparison of ACID vs Saga patterns
"""

from pymongo import MongoClient
from pymongo.write_concern import WriteConcern
from pymongo.read_concern import ReadConcern
import time
from datetime import datetime

class TransactionAnalysis:
    def __init__(self):
        self.client = MongoClient(
            'mongodb://mongo1:27017/?directConnection=true',
            serverSelectionTimeoutMS=5000
        )
        self.db = self.client['distributed_lab']
    
    def demonstrate_acid_transaction(self):
        """
        Demonstrate ACID transaction concept for e-commerce order
        Note: Using simplified version without multi-document transactions
        """
        print("\n" + "=" * 70)
        print("DEMONSTRATION 1: ACID Transaction Pattern")
        print("=" * 70)
        
        print("\n📦 Scenario: E-commerce Order Processing")
        print("   Operations:")
        print("   1. Deduct inventory")
        print("   2. Create order record")
        print("   3. Process payment")
        print("   4. Update user account")
        
        # Setup collections
        inventory = self.db['Inventory']
        orders = self.db['Orders']
        payments = self.db['Payments']
        users = self.db['Users']
        
        # Initialize test data
        try:
            inventory.delete_many({})
            orders.delete_many({})
            payments.delete_many({})
            users.delete_many({})
        except:
            pass
        
        inventory.insert_one({'product_id': 'Mango', 'stock': 10})
        users.insert_one({'user_id': 'USER456', 'balance': 1000.0})
        
        print("\n📋 Initial State:")
        print(f"   - Product Mango: 10 units in stock")
        print(f"   - User USER456: $1000.00 balance")
        
        # Simulated ACID Transaction (conceptual)
        print("\n🔄 Executing ACID Transaction (Conceptual)...")
        print("   Note: With directConnection, demonstrating concept without multi-doc transactions")
        
        try:
            # Step 1: Deduct inventory
            print("   [1/4] Deducting inventory...")
            result = inventory.update_one(
                {'product_id': 'Mango', 'stock': {'$gte': 1}},
                {'$inc': {'stock': -1}}
            )
            if result.modified_count == 0:
                raise Exception("Insufficient stock")
            
            # Step 2: Create order
            print("   [2/4] Creating order...")
            order_id = f"ORD{int(time.time())}"
            orders.insert_one({
                'order_id': order_id,
                'user_id': 'USER456',
                'product_id': 'Mango',
                'amount': 99.99,
                'status': 'confirmed',
                'timestamp': datetime.now()
            })
            
            # Step 3: Process payment
            print("   [3/4] Processing payment...")
            payments.insert_one({
                'payment_id': f"PAY{int(time.time())}",
                'order_id': order_id,
                'amount': 99.99,
                'status': 'completed'
            })
            
            # Step 4: Update user balance
            print("   [4/4] Updating user balance...")
            users.update_one(
                {'user_id': 'USER456'},
                {'$inc': {'balance': -99.99}}
            )
            
            print("   ✅ All operations successful")
            
            # Verify final state
            final_inventory = inventory.find_one({'product_id': 'Mango'})
            final_user = users.find_one({'user_id': 'USER456'})
            
            print("\n📊 Final State:")
            print(f"   - Product Mango: {final_inventory['stock']} units in stock")
            print(f"   - User USER456: ${final_user['balance']:.2f} balance")
            print(f"   - Order created: {order_id}")
            
        except Exception as e:
            print(f"   ❌ Transaction failed: {e}")
            print("   🔄 In a real ACID transaction, all changes would be rolled back")
            print("   ℹ️  Database would remain consistent")
        
        print("\n💡 ACID Properties:")
        print("   ✓ Atomicity:    All-or-nothing execution")
        print("   ✓ Consistency:  Database stays in valid state")
        print("   ✓ Isolation:    Transactions don't interfere")
        print("   ✓ Durability:   Committed data persists")
        
        print("\n📈 Advantages:")
        print("   + Strong consistency guarantees")
        print("   + Simple programming model")
        print("   + Automatic rollback on failure")
        
        print("\n📉 Disadvantages:")
        print("   - Requires distributed transaction coordinator")
        print("   - Performance overhead (locks, 2PC)")
        print("   - Scalability limitations")
        print("   - May block during network partitions")
    
    def demonstrate_saga_pattern(self):
        """
        Demonstrate Saga pattern with compensating transactions
        Each step has a compensating action for rollback
        """
        print("\n" + "=" * 70)
        print("DEMONSTRATION 2: Saga Pattern (Compensating Transactions)")
        print("=" * 70)
        
        print("\n📦 Scenario: E-commerce Order Processing (Distributed)")
        print("   Saga Steps:")
        print("   1. Reserve inventory → Compensate: Release reservation")
        print("   2. Create order → Compensate: Cancel order")
        print("   3. Process payment → Compensate: Refund payment")
        print("   4. Update user → Compensate: Revert user update")
        
        # Setup collections
        inventory = self.db['SagaInventory']
        orders = self.db['SagaOrders']
        payments = self.db['SagaPayments']
        users = self.db['SagaUsers']
        saga_log = self.db['SagaLog']
        
        # Initialize test data
        try:
            inventory.delete_many({})
            orders.delete_many({})
            payments.delete_many({})
            users.delete_many({})
            saga_log.delete_many({})
        except:
            pass
        
        inventory.insert_one({'product_id': 'Mango', 'stock': 10, 'reserved': 0})
        users.insert_one({'user_id': 'USER456', 'balance': 1000.0})
        
        print("\n📋 Initial State:")
        print(f"   - Product Mango: 10 units available, 0 reserved")
        print(f"   - User USER456: $1000.00 balance")
        
        # Execute Saga
        print("\n🔄 Executing Saga Transaction...")
        
        saga_id = f"SAGA{int(time.time())}"
        completed_steps = []
        
        try:
            # Step 1: Reserve inventory
            print("   [1/4] Reserving inventory...")
            result = inventory.update_one(
                {'product_id': 'Mango', 'stock': {'$gte': 1}},
                {'$inc': {'stock': -1, 'reserved': 1}}
            )
            if result.modified_count == 0:
                raise Exception("Insufficient stock")
            completed_steps.append('reserve_inventory')
            saga_log.insert_one({
                'saga_id': saga_id,
                'step': 'reserve_inventory',
                'status': 'completed',
                'timestamp': datetime.now()
            })
            time.sleep(0.1)  # Simulate network delay
            
            # Step 2: Create order
            print("   [2/4] Creating order...")
            order_id = f"ORD{int(time.time())}"
            orders.insert_one({
                'order_id': order_id,
                'user_id': 'USER456',
                'product_id': 'Mango',
                'amount': 99.99,
                'status': 'pending',
                'saga_id': saga_id
            })
            completed_steps.append('create_order')
            saga_log.insert_one({
                'saga_id': saga_id,
                'step': 'create_order',
                'status': 'completed',
                'order_id': order_id,
                'timestamp': datetime.now()
            })
            time.sleep(0.1)
            
            # Step 3: Process payment (simulate success or failure)
            print("   [3/4] Processing payment...")
            # Uncomment next line to simulate failure:
            # raise Exception("Payment gateway timeout")
            
            payments.insert_one({
                'payment_id': f"PAY{int(time.time())}",
                'order_id': order_id,
                'amount': 99.99,
                'status': 'completed',
                'saga_id': saga_id
            })
            completed_steps.append('process_payment')
            saga_log.insert_one({
                'saga_id': saga_id,
                'step': 'process_payment',
                'status': 'completed',
                'timestamp': datetime.now()
            })
            time.sleep(0.1)
            
            # Step 4: Update user
            print("   [4/4] Updating user balance...")
            users.update_one(
                {'user_id': 'USER456'},
                {'$inc': {'balance': -99.99}}
            )
            completed_steps.append('update_user')
            saga_log.insert_one({
                'saga_id': saga_id,
                'step': 'update_user',
                'status': 'completed',
                'timestamp': datetime.now()
            })
            
            # Mark saga complete
            orders.update_one(
                {'order_id': order_id},
                {'$set': {'status': 'confirmed'}}
            )
            inventory.update_one(
                {'product_id': 'Mango'},
                {'$inc': {'reserved': -1}}
            )
            
            print("   ✅ SAGA COMPLETED SUCCESSFULLY")
            
            # Verify final state
            final_inventory = inventory.find_one({'product_id': 'Mango'})
            final_user = users.find_one({'user_id': 'USER456'})
            
            print("\n📊 Final State:")
            print(f"   - Product Mango: {final_inventory['stock']} units, {final_inventory['reserved']} reserved")
            print(f"   - User USER456: ${final_user['balance']:.2f} balance")
            print(f"   - Order: {order_id} (confirmed)")
            
        except Exception as e:
            print(f"   ❌ Saga step failed: {e}")
            print("   🔄 EXECUTING COMPENSATING TRANSACTIONS...")
            
            # Compensate in reverse order
            for step in reversed(completed_steps):
                if step == 'update_user':
                    print("   ↩️  Reverting user balance...")
                    users.update_one(
                        {'user_id': 'USER456'},
                        {'$inc': {'balance': 99.99}}
                    )
                
                elif step == 'process_payment':
                    print("   ↩️  Refunding payment...")
                    payment = payments.find_one({'saga_id': saga_id})
                    if payment:
                        payments.update_one(
                            {'payment_id': payment['payment_id']},
                            {'$set': {'status': 'refunded'}}
                        )
                
                elif step == 'create_order':
                    print("   ↩️  Cancelling order...")
                    orders.update_one(
                        {'saga_id': saga_id},
                        {'$set': {'status': 'cancelled'}}
                    )
                
                elif step == 'reserve_inventory':
                    print("   ↩️  Releasing inventory reservation...")
                    inventory.update_one(
                        {'product_id': 'Mango'},
                        {'$inc': {'stock': 1, 'reserved': -1}}
                    )
                
                saga_log.insert_one({
                    'saga_id': saga_id,
                    'step': f'compensate_{step}',
                    'status': 'completed',
                    'timestamp': datetime.now()
                })
                time.sleep(0.05)
            
            print("   ✅ COMPENSATION COMPLETED - System restored to consistent state")
        
        print("\n💡 Saga Pattern Characteristics:")
        print("   ✓ Each step is independent transaction")
        print("   ✓ Compensating actions for rollback")
        print("   ✓ Eventual consistency")
        print("   ✓ Better availability and scalability")
        
        print("\n📈 Advantages:")
        print("   + No distributed locks")
        print("   + Better scalability")
        print("   + Works across microservices")
        print("   + Tolerates network partitions")
        
        print("\n📉 Disadvantages:")
        print("   - Complex to implement")
        print("   - Temporary inconsistency")
        print("   - Must design compensating transactions")
        print("   - Eventual consistency only")
    
    def comparison_analysis(self):
        """Print side-by-side comparison"""
        print("\n" + "=" * 70)
        print("COMPARISON: ACID vs Saga Pattern")
        print("=" * 70)
        
        print("\n┌─────────────────────┬──────────────────────┬──────────────────────┐")
        print("│ Characteristic      │ ACID Transactions    │ Saga Pattern         │")
        print("├─────────────────────┼──────────────────────┼──────────────────────┤")
        print("│ Consistency Model   │ Strong (Immediate)   │ Eventual             │")
        print("│ Isolation           │ Full isolation       │ No isolation         │")
        print("│ Atomicity           │ All-or-nothing       │ Compensating actions │")
        print("│ Durability          │ Guaranteed           │ Guaranteed           │")
        print("│ Performance         │ Slower (locks)       │ Faster (no locks)    │")
        print("│ Scalability         │ Limited              │ High                 │")
        print("│ Complexity          │ Low                  │ High                 │")
        print("│ Failure Handling    │ Automatic rollback   │ Manual compensation  │")
        print("│ Cross-service       │ Difficult            │ Natural fit          │")
        print("│ Partition Tolerance │ Lower                │ Higher               │")
        print("└─────────────────────┴──────────────────────┴──────────────────────┘")
        
        print("\n🎯 When to use ACID:")
        print("   • Financial transactions (banking, payments)")
        print("   • Inventory with strict accuracy requirements")
        print("   • Single database / monolithic architecture")
        print("   • Strong consistency is critical")
        
        print("\n🎯 When to use Saga:")
        print("   • Microservices architecture")
        print("   • Long-running business processes")
        print("   • High availability requirements")
        print("   • Cross-system workflows")
        print("   • Eventual consistency acceptable")
        
        print("\n📊 CAP Theorem Context:")
        print("   ACID:  Prioritizes Consistency (CP)")
        print("   Saga:  Prioritizes Availability (AP)")
    
    def close(self):
        if self.client:
            self.client.close()

def main():
    print("=" * 70)
    print("MongoDB Distributed Systems Lab - Transaction Patterns Analysis")
    print("=" * 70)
    
    analysis = TransactionAnalysis()
    
    try:
        analysis.demonstrate_acid_transaction()
        time.sleep(1)
        analysis.demonstrate_saga_pattern()
        time.sleep(1)
        analysis.comparison_analysis()
        
        print("\n" + "=" * 70)
        print("✅ Transaction analysis completed!")
        print("=" * 70)
        
        
    except KeyboardInterrupt:
        print("\n\n⚠️  Analysis interrupted by user")
    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()
    finally:
        analysis.close()

if __name__ == "__main__":
    main()