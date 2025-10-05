#!/usr/bin/env python3
"""
Test script for Keyhole Python
This script demonstrates how to use the Keyhole Python library programmatically
"""

import sys
import os

# Add src directory to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

from src.mongodb_client import MongoDBClient
from src.cluster_stats import ClusterStatsCollector
from src.report_generator import HTMLReportGenerator


def test_with_local_mongodb():
    """Test with local MongoDB instance"""
    
    # MongoDB connection string (adjust as needed)
    uri = "mongodb://localhost:27017"
    
    print("Testing Keyhole Python with local MongoDB...")
    print(f"Connection URI: {uri}")
    
    try:
        # Initialize client
        client = MongoDBClient(uri)
        
        # Test connection
        if not client.connect():
            print("❌ Failed to connect to MongoDB")
            print("Make sure MongoDB is running on localhost:27017")
            return False
        
        print("✅ Connected to MongoDB successfully")
        
        # Get cluster summary
        collector = ClusterStatsCollector(client, verbose=True)
        summary = collector.get_cluster_summary()
        print(f"📊 Cluster Summary: {summary}")
        
        # Collect basic stats (fast mode for testing)
        print("🔍 Collecting cluster statistics...")
        stats = collector.collect_all_stats(fast_mode=True)
        
        print(f"✅ Collected statistics for {len(stats.databases)} databases and {len(stats.collections)} collections")
        
        # Generate HTML report
        print("📝 Generating HTML report...")
        generator = HTMLReportGenerator()
        output_file = "test_report.html"
        generator.generate_cluster_report(stats, output_file)
        
        print(f"✅ Report generated: {output_file}")
        print(f"📂 Open {os.path.abspath(output_file)} in your browser to view the report")
        
        # Close connection
        client.disconnect()
        print("✅ Disconnected from MongoDB")
        
        return True
        
    except Exception as e:
        print(f"❌ Error during testing: {e}")
        return False


def main():
    """Main test function"""
    print("🚀 Keyhole Python Test Script")
    print("=" * 50)
    
    success = test_with_local_mongodb()
    
    if success:
        print("\n🎉 Test completed successfully!")
        print("You can now use Keyhole Python with your MongoDB clusters.")
    else:
        print("\n💥 Test failed!")
        print("Please check your MongoDB connection and try again.")
    
    return 0 if success else 1


if __name__ == '__main__':
    sys.exit(main())
