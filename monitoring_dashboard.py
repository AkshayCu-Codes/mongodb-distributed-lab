"""
MongoDB Distributed Systems Lab - Monitoring Dashboard (Bonus)
Real-time visualization of replica set status and metrics
"""

import time
import matplotlib.pyplot as plt
import matplotlib.animation as animation
from pymongo import MongoClient
from datetime import datetime
from collections import deque

class MongoDBMonitor:
    def __init__(self, max_points=100):
        self.client = MongoClient(
            'mongodb://localhost:27017,localhost:27018,localhost:27019/?replicaSet=rs0',
            serverSelectionTimeoutMS=5000
        )
        self.max_points = max_points
        
        # Data storage
        self.timestamps = deque(maxlen=max_points)
        self.replication_lag = {
            'mongo1': deque(maxlen=max_points),
            'mongo2': deque(maxlen=max_points),
            'mongo3': deque(maxlen=max_points)
        }
        self.node_status = {'mongo1': [], 'mongo2': [], 'mongo3': []}
        
        # Setup plot
        self.fig, (self.ax1, self.ax2) = plt.subplots(2, 1, figsize=(12, 8))
        self.fig.suptitle('MongoDB Replica Set Monitor', fontsize=16, fontweight='bold')
    
    def get_replica_status(self):
        """Get current replica set status"""
        try:
            status = self.client.admin.command('replSetGetStatus')
            return status
        except Exception as e:
            print(f"Error getting replica status: {e}")
            return None
    
    def update_data(self, frame):
        """Update monitoring data (called by animation)"""
        status = self.get_replica_status()
        if not status:
            return
        
        current_time = datetime.now()
        self.timestamps.append(current_time)
        
        # Determine current primary
        primary_name = None
        primary_optime = None
        for member in status['members']:
            if member['stateStr'] == 'PRIMARY':
                primary_name = member['name'].split(':')[0]
                primary_optime = member.get('optimeDate')
                break
        
        # Update replication lag for all nodes
        for member in status['members']:
            node_name = member['name'].split(':')[0]
            if node_name not in self.replication_lag:
                self.replication_lag[node_name] = deque(maxlen=self.max_points)
            
            if node_name == primary_name:
                self.replication_lag[node_name].append(0)
            else:
                # Secondary lag relative to current primary
                secondary_optime = member.get('optimeDate')
                lag = max(0, (primary_optime - secondary_optime).total_seconds()) if primary_optime and secondary_optime else 0
                self.replication_lag[node_name].append(lag)
        
        # Update node status
        for member in status['members']:
            node_name = member['name'].split(':')[0]
            state = member['stateStr']
            health = member['health']
            if node_name not in self.node_status:
                self.node_status[node_name] = []
            
            # Status values: 1=PRIMARY, 0.5=SECONDARY, 0=DOWN, 0.25=OTHER
            if health == 0:
                status_value = 0
            elif state == 'PRIMARY':
                status_value = 1
            elif state == 'SECONDARY':
                status_value = 0.5
            else:
                status_value = 0.25
            
            self.node_status[node_name].append(status_value)
            if len(self.node_status[node_name]) > self.max_points:
                self.node_status[node_name] = self.node_status[node_name][-self.max_points:]
        
        # Redraw plots
        self.update_plots(primary_name)
    
    def update_plots(self, primary_name=None):
        """Redraw plots with current data"""
        self.ax1.clear()
        self.ax2.clear()
        
        colors = {'mongo1': '#2E86AB', 'mongo2': '#A23B72', 'mongo3': '#F18F01'}
        
        # Plot 1: Replication Lag
        if len(self.timestamps) > 0:
            times = list(self.timestamps)
            for node, lags in self.replication_lag.items():
                if len(lags) > 0:
                    lags_ms = [l * 1000 for l in lags]
                    linestyle = '--' if node == primary_name else '-'
                    self.ax1.plot(times[:len(lags_ms)], lags_ms,
                                  label=node, linewidth=2, marker='o', markersize=3,
                                  color=colors.get(node, '#666666'),
                                  linestyle=linestyle)
            
            self.ax1.set_ylabel('Replication Lag (ms)', fontsize=10, fontweight='bold')
            self.ax1.set_title('Replication Lag Over Time', fontsize=12, fontweight='bold')
            self.ax1.legend(loc='upper right')
            self.ax1.grid(True, alpha=0.3)
            self.ax1.tick_params(axis='x', rotation=45)
            self.ax1.set_xlim(times[0], times[-1])
        
        # Plot 2: Node Status
        if len(self.timestamps) > 0:
            times = list(self.timestamps)
            for node in ['mongo1', 'mongo2', 'mongo3']:
                if node in self.node_status and len(self.node_status[node]) > 0:
                    status_values = self.node_status[node]
                    plot_times = times[:len(status_values)]
                    self.ax2.plot(plot_times, status_values,
                                  label=node, linewidth=3,
                                  color=colors.get(node, '#666666'))
            
            self.ax2.set_ylabel('Node Status', fontsize=10, fontweight='bold')
            self.ax2.set_title('Node Health Status', fontsize=12, fontweight='bold')
            self.ax2.set_yticks([0, 0.5, 1])
            self.ax2.set_yticklabels(['DOWN', 'SECONDARY', 'PRIMARY'])
            self.ax2.legend(loc='upper right')
            self.ax2.grid(True, alpha=0.3, axis='y')
            self.ax2.tick_params(axis='x', rotation=45)
            self.ax2.set_xlim(times[0], times[-1])
        
        plt.tight_layout()
    
    def run(self, interval=2000):
        """Start monitoring dashboard"""
        print("=" * 70)
        print("MongoDB Replica Set Monitoring Dashboard")
        print("=" * 70)
        print("\n🚀 Starting real-time monitoring...")
        print("📊 Dashboard will update every 2 seconds")
        print("⚠️  Close the plot window to stop monitoring\n")
        
        try:
            ani = animation.FuncAnimation(
                self.fig,
                self.update_data,
                interval=interval,
                cache_frame_data=False
            )
            plt.show()
        except KeyboardInterrupt:
            print("\n\n⚠️  Monitoring stopped by user")
        finally:
            self.client.close()
            print("\n✅ Dashboard closed")

def print_current_status():
    """Print a snapshot of current replica set status"""
    print("=" * 70)
    print("Current Replica Set Status Snapshot")
    print("=" * 70)
    
    try:
        client = MongoClient(
            'mongodb://localhost:27017,localhost:27018,localhost:27019/?replicaSet=rs0',
            serverSelectionTimeoutMS=5000
        )
        status = client.admin.command('replSetGetStatus')
        
        print(f"\n📋 Replica Set: {status['set']}")
        print(f"📅 Date: {status['date']}")
        print(f"\n{'Node':<15} {'State':<15} {'Health':<10} {'Uptime':<10} {'Lag':<10}")
        print("-" * 70)
        
        primary_optime = next((m['optimeDate'] for m in status['members'] if m['stateStr'] == 'PRIMARY'), None)
        
        for member in status['members']:
            name = member['name']
            state = member['stateStr']
            health = '🟢 Healthy' if member['health'] == 1 else '🔴 Down'
            uptime = member.get('uptime', 0)
            
            lag = "N/A"
            if state == 'SECONDARY' and primary_optime:
                secondary_optime = member.get('optimeDate')
                if secondary_optime:
                    lag_seconds = (primary_optime - secondary_optime).total_seconds()
                    lag = f"{lag_seconds:.2f}s"
            elif state == 'PRIMARY':
                lag = "PRIMARY"
            
            print(f"{name:<15} {state:<15} {health:<10} {uptime:<10} {lag:<10}")
        
        print("\n" + "=" * 70)
        client.close()
        
    except Exception as e:
        print(f"\n❌ Error: {e}")
        print("Make sure the MongoDB cluster is running!")

def main():
    import sys
    if len(sys.argv) > 1 and sys.argv[1] == '--status':
        print_current_status()
    else:
        print("\n💡 TIP: Use 'python monitoring_dashboard.py --status' for a quick status check\n")
        monitor = MongoDBMonitor(max_points=60)
        monitor.run(interval=2000)

if __name__ == "__main__":
    main()
