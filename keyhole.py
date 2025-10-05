#!/usr/bin/env python3
"""
Keyhole Python - MongoDB Cluster Analysis Tool
A Python implementation of the Keyhole MongoDB analysis tool with HTML output

Usage:
    python keyhole.py --allinfo "mongodb://user:pass@host:port/db" --output report.html
    python keyhole.py --index "mongodb://..." --output index-report.html
    python keyhole.py --cardinality collection_name "mongodb://..." --output cardinality.html
"""

import argparse
import logging
import sys
import os
from datetime import datetime
from typing import List, Optional

# Add src directory to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

from src.mongodb_client import MongoDBClient
from src.cluster_stats import ClusterStatsCollector
from src.report_generator import HTMLReportGenerator


def setup_logging(verbose: bool = False):
    """Setup logging configuration"""
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )


def main():
    """Main entry point"""
    parser = argparse.ArgumentParser(
        description='Keyhole Python - MongoDB Cluster Analysis Tool',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Generate comprehensive cluster analysis report
  python keyhole.py --allinfo "mongodb://user:pass@host:port/db" --output cluster-report.html
  
  # Analyze indexes
  python keyhole.py --index "mongodb://user:pass@host:port/db" --output index-report.html
  
  # Check collection cardinality
  python keyhole.py --cardinality collection_name "mongodb://user:pass@host:port/db" --output cardinality.html
  
  # Analyze specific databases only
  python keyhole.py --allinfo "mongodb://user:pass@host:port/db" --db database1 --db database2 --output report.html
  
  # Analyze specific collection only
  python keyhole.py --allinfo "mongodb://user:pass@host:port/db" --collection database.collection_name --output collection-report.html
  
  # Fast mode (skip detailed collection analysis)
  python keyhole.py --allinfo "mongodb://user:pass@host:port/db" --fast --output report.html
        """
    )
    
    # Connection options
    parser.add_argument('uri', nargs='?', help='MongoDB connection string')
    parser.add_argument('--allinfo', help='Database connection string for comprehensive analysis')
    parser.add_argument('--index', help='Database connection string for index analysis')
    parser.add_argument('--cardinality', help='Collection name for cardinality analysis')
    
    # Analysis options
    parser.add_argument('--db', action='append', help='Specific database(s) to analyze (can be used multiple times)')
    parser.add_argument('--collection', help='Specific collection to analyze (format: database.collection)')
    parser.add_argument('--fast', action='store_true', help='Fast mode - skip detailed collection analysis')
    parser.add_argument('--verbose', '-v', action='store_true', help='Enable verbose logging')
    
    # Output options
    parser.add_argument('--output', '-o', help='Output HTML file path')
    parser.add_argument('--no-open', action='store_true', help='Do not open the report in browser after generation')
    
    # Connection options
    parser.add_argument('--timeout', type=int, default=30, help='Connection timeout in seconds (default: 30)')
    
    args = parser.parse_args()
    
    # Setup logging
    setup_logging(args.verbose)
    logger = logging.getLogger(__name__)
    
    # Determine which analysis to perform
    analysis_type = None
    uri = None
    
    if args.allinfo:
        analysis_type = 'allinfo'
        uri = args.allinfo
    elif args.index:
        analysis_type = 'index'
        uri = args.index
    elif args.cardinality:
        analysis_type = 'cardinality'
        uri = args.uri
        if not uri:
            logger.error("MongoDB URI is required for cardinality analysis")
            sys.exit(1)
    elif args.uri:
        analysis_type = 'allinfo'
        uri = args.uri
    else:
        parser.print_help()
        sys.exit(1)
    
    # Generate output filename if not provided
    if not args.output:
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        if analysis_type == 'allinfo':
            args.output = f"cluster_report_{timestamp}.html"
        elif analysis_type == 'index':
            args.output = f"index_report_{timestamp}.html"
        elif analysis_type == 'cardinality':
            args.output = f"cardinality_report_{timestamp}.html"
    
    try:
        # Initialize MongoDB client
        logger.info(f"Connecting to MongoDB: {uri}")
        client = MongoDBClient(uri, timeout=args.timeout)
        
        if not client.connect():
            logger.error("Failed to connect to MongoDB")
            sys.exit(1)
        
        # Perform analysis based on type
        if analysis_type == 'allinfo':
            perform_cluster_analysis(client, args)
        elif analysis_type == 'index':
            perform_index_analysis(client, args)
        elif analysis_type == 'cardinality':
            perform_cardinality_analysis(client, args)
        
        # Close connection
        client.disconnect()
        
        logger.info(f"Analysis completed successfully. Report saved to: {args.output}")
        
        # Open report in browser unless disabled
        if not args.no_open:
            try:
                import webbrowser
                webbrowser.open(f"file://{os.path.abspath(args.output)}")
                logger.info("Report opened in browser")
            except Exception as e:
                logger.warning(f"Could not open report in browser: {e}")
        
    except KeyboardInterrupt:
        logger.info("Analysis interrupted by user")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Analysis failed: {e}")
        if args.verbose:
            import traceback
            traceback.print_exc()
        sys.exit(1)


def perform_cluster_analysis(client: MongoDBClient, args):
    """Perform comprehensive cluster analysis"""
    logger = logging.getLogger(__name__)
    
    logger.info("Starting comprehensive cluster analysis")
    
    # Get cluster summary first
    collector = ClusterStatsCollector(client, verbose=args.verbose)
    summary = collector.get_cluster_summary()
    logger.info(f"Cluster: {summary}")
    
    # Handle collection-specific analysis
    include_databases = args.db
    if args.collection:
        # Extract database name from collection parameter (format: database.collection)
        if '.' not in args.collection:
            logger.error("Collection parameter must be in format 'database.collection'")
            sys.exit(1)
        
        db_name = args.collection.split('.')[0]
        include_databases = [db_name]
        logger.info(f"Analyzing specific collection: {args.collection}")
    
    # Collect all statistics
    stats = collector.collect_all_stats(
        include_databases=include_databases,
        fast_mode=args.fast
    )
    
    # Filter to specific collection if requested
    if args.collection:
        db_name, coll_name = args.collection.split('.', 1)
        # Filter collections to only include the specified one
        original_collections = stats.collections[:]
        stats.collections = [coll for coll in stats.collections if coll.namespace == args.collection]
        
        if not stats.collections:
            logger.warning(f"Collection {args.collection} not found or no data available")
        
        # Also filter databases to only include the one containing the collection
        stats.databases = [db for db in stats.databases if db.name == db_name]
        
        logger.info(f"Filtered analysis to collection: {args.collection}")
        logger.info(f"Found {len(stats.collections)} collections matching criteria")
    
    # Generate HTML report
    logger.info("Generating HTML report")
    generator = HTMLReportGenerator()
    generator.generate_cluster_report(stats, args.output)
    
    logger.info("Cluster analysis completed")


def perform_index_analysis(client: MongoDBClient, args):
    """Perform index analysis"""
    logger = logging.getLogger(__name__)
    
    logger.info("Starting index analysis")
    
    # This would implement index analysis similar to the Go version
    # For now, we'll create a placeholder
    logger.warning("Index analysis not yet implemented - placeholder")
    
    # Placeholder data
    index_data = {
        'host': client.uri,
        'version': 'Unknown',
        'message': 'Index analysis feature coming soon'
    }
    
    # Generate placeholder report
    generator = HTMLReportGenerator()
    with open(args.output, 'w') as f:
        f.write(f"""
        <html>
        <head><title>Index Analysis Report</title></head>
        <body>
            <h1>Index Analysis Report</h1>
            <p>This feature is not yet implemented.</p>
            <p>Host: {index_data['host']}</p>
        </body>
        </html>
        """)
    
    logger.info("Index analysis completed (placeholder)")


def perform_cardinality_analysis(client: MongoDBClient, args):
    """Perform cardinality analysis"""
    logger = logging.getLogger(__name__)
    
    logger.info(f"Starting cardinality analysis for collection: {args.cardinality}")
    
    # This would implement cardinality analysis similar to the Go version
    # For now, we'll create a placeholder
    logger.warning("Cardinality analysis not yet implemented - placeholder")
    
    # Placeholder data
    cardinality_data = {
        'host': client.uri,
        'version': 'Unknown',
        'collection': args.cardinality,
        'message': 'Cardinality analysis feature coming soon'
    }
    
    # Generate placeholder report
    generator = HTMLReportGenerator()
    with open(args.output, 'w') as f:
        f.write(f"""
        <html>
        <head><title>Cardinality Analysis Report</title></head>
        <body>
            <h1>Cardinality Analysis Report</h1>
            <p>This feature is not yet implemented.</p>
            <p>Host: {cardinality_data['host']}</p>
            <p>Collection: {cardinality_data['collection']}</p>
        </body>
        </html>
        """)
    
    logger.info("Cardinality analysis completed (placeholder)")


if __name__ == '__main__':
    main()
