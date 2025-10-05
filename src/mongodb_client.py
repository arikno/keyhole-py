"""
MongoDB Client for Keyhole Python
Handles MongoDB connections and basic operations
"""

import logging
import re
import sys
from typing import Dict, List, Optional, Any
from urllib.parse import urlparse
import pymongo
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure, ServerSelectionTimeoutError


class MongoDBClient:
    """MongoDB client wrapper with connection management and error handling"""
    
    def __init__(self, uri: str, timeout: int = 30):
        """
        Initialize MongoDB client
        
        Args:
            uri: MongoDB connection string
            timeout: Connection timeout in seconds
        """
        self.uri = uri
        self.timeout = timeout
        self.client: Optional[MongoClient] = None
        self.logger = logging.getLogger(__name__)
        
    def connect(self) -> bool:
        """
        Establish connection to MongoDB
        
        Returns:
            True if connection successful, False otherwise
        """
        try:
            # Parse and validate URI
            parsed = urlparse(self.uri)
            if parsed.scheme not in ['mongodb', 'mongodb+srv']:
                self.logger.error(f"Invalid MongoDB URI scheme: {parsed.scheme}")
                return False
                
            # Create client with timeout and SSL settings
            # For Atlas connections, we need to handle SSL properly
            client_options = {
                'serverSelectionTimeoutMS': self.timeout * 1000,
                'connectTimeoutMS': self.timeout * 1000,
                'socketTimeoutMS': self.timeout * 1000
            }
            
            # Add SSL options for Atlas connections
            if 'mongodb+srv://' in self.uri or '.mongodb.net' in self.uri:
                client_options['tls'] = True
                # For testing purposes, allow invalid certificates
                # In production, you should use proper certificate validation
                client_options['tlsAllowInvalidCertificates'] = True
                client_options['tlsAllowInvalidHostnames'] = True
            
            self.client = MongoClient(self.uri, **client_options)
            
            # Test connection
            self.client.admin.command('ping')
            self.logger.info(f"Successfully connected to MongoDB: {parsed.netloc}")
            return True
            
        except ConnectionFailure as e:
            self.logger.error(f"Failed to connect to MongoDB: {e}")
            return False
        except ServerSelectionTimeoutError as e:
            self.logger.error(f"MongoDB server selection timeout: {e}")
            return False
        except Exception as e:
            self.logger.error(f"Unexpected error connecting to MongoDB: {e}")
            return False
    
    def disconnect(self):
        """Close MongoDB connection"""
        if self.client:
            self.client.close()
            self.logger.info("Disconnected from MongoDB")
    
    def get_server_info(self) -> Dict[str, Any]:
        """
        Get basic server information
        
        Returns:
            Dictionary containing server information
        """
        if not self.client:
            return {}
            
        try:
            # Get build info
            build_info = self.client.admin.command('buildInfo')
            
            # Get host info
            host_info = self.client.admin.command('hostInfo')
            
            # Get server status
            server_status = self.client.admin.command('serverStatus')
            
            # Get command line options
            cmd_line_opts = self.client.admin.command('getCmdLineOpts')
            
            return {
                'buildInfo': build_info,
                'hostInfo': host_info,
                'serverStatus': server_status,
                'cmdLineOpts': cmd_line_opts
            }
            
        except Exception as e:
            self.logger.error(f"Error getting server info: {e}")
            return {}
    
    def get_cluster_type(self) -> str:
        """
        Determine cluster type (standalone, replica, sharded)
        
        Returns:
            Cluster type string
        """
        if not self.client:
            return "unknown"
            
        try:
            server_status = self.client.admin.command('serverStatus')
            
            # Check for sharding
            if 'sharding' in server_status and server_status['sharding'].get('configsvrConnectionString'):
                return "sharded"
            
            # Check for replica set
            if 'repl' in server_status and server_status['repl'].get('setName'):
                return "replica"
            
            return "standalone"
            
        except Exception as e:
            self.logger.error(f"Error determining cluster type: {e}")
            return "unknown"
    
    def get_databases(self) -> List[str]:
        """
        Get list of database names
        
        Returns:
            List of database names
        """
        if not self.client:
            return []
            
        try:
            return self.client.list_database_names()
        except Exception as e:
            self.logger.error(f"Error getting database list: {e}")
            return []
    
    def get_database_stats(self, db_name: str) -> Dict[str, Any]:
        """
        Get database statistics
        
        Args:
            db_name: Database name
            
        Returns:
            Database statistics dictionary
        """
        if not self.client:
            return {}
            
        try:
            return self.client[db_name].command('dbStats')
        except Exception as e:
            self.logger.error(f"Error getting database stats for {db_name}: {e}")
            return {}
    
    def get_collections(self, db_name: str) -> List[str]:
        """
        Get list of collection names in a database
        
        Args:
            db_name: Database name
            
        Returns:
            List of collection names
        """
        if not self.client:
            return []
            
        try:
            return self.client[db_name].list_collection_names()
        except Exception as e:
            self.logger.error(f"Error getting collections for {db_name}: {e}")
            return []
    
    def get_collection_stats(self, db_name: str, collection_name: str) -> Dict[str, Any]:
        """
        Get collection statistics
        
        Args:
            db_name: Database name
            collection_name: Collection name
            
        Returns:
            Collection statistics dictionary
        """
        if not self.client:
            return {}
            
        try:
            return self.client[db_name].command('collStats', collection_name)
        except Exception as e:
            self.logger.error(f"Error getting collection stats for {db_name}.{collection_name}: {e}")
            return {}
    
    def get_indexes(self, db_name: str, collection_name: str) -> List[Dict[str, Any]]:
        """
        Get collection indexes
        
        Args:
            db_name: Database name
            collection_name: Collection name
            
        Returns:
            List of index documents
        """
        if not self.client:
            return []
            
        try:
            collection = self.client[db_name][collection_name]
            return list(collection.list_indexes())
        except Exception as e:
            self.logger.error(f"Error getting indexes for {db_name}.{collection_name}: {e}")
            return []
    
    def get_index_stats(self, db_name: str, collection_name: str) -> List[Dict[str, Any]]:
        """
        Get index usage statistics
        
        Args:
            db_name: Database name
            collection_name: Collection name
            
        Returns:
            List of index statistics
        """
        if not self.client:
            return []
            
        try:
            collection = self.client[db_name][collection_name]
            pipeline = [{"$indexStats": {}}]
            return list(collection.aggregate(pipeline))
        except Exception as e:
            self.logger.error(f"Error getting index stats for {db_name}.{collection_name}: {e}")
            return []
    
    def get_sample_documents(self, db_name: str, collection_name: str, limit: int = 5) -> List[Dict[str, Any]]:
        """
        Get sample documents from a collection
        
        Args:
            db_name: Database name
            collection_name: Collection name
            limit: Number of sample documents to retrieve
            
        Returns:
            List of sample documents
        """
        if not self.client:
            return []
            
        try:
            collection = self.client[db_name][collection_name]
            return list(collection.find().limit(limit))
        except Exception as e:
            self.logger.error(f"Error getting sample documents for {db_name}.{collection_name}: {e}")
            return []
    
    def get_replica_set_status(self) -> Dict[str, Any]:
        """
        Get replica set status
        
        Returns:
            Replica set status dictionary
        """
        if not self.client:
            return {}
            
        try:
            return self.client.admin.command('replSetGetStatus')
        except Exception as e:
            self.logger.error(f"Error getting replica set status: {e}")
            return {}
    
    def get_sharding_status(self) -> Dict[str, Any]:
        """
        Get sharding status
        
        Returns:
            Sharding status dictionary
        """
        if not self.client:
            return {}
            
        try:
            return self.client.admin.command('sh.status')
        except Exception as e:
            self.logger.error(f"Error getting sharding status: {e}")
            return {}
    
    def get_shards(self) -> List[Dict[str, Any]]:
        """
        Get shard information
        
        Returns:
            List of shard information
        """
        if not self.client:
            return []
            
        try:
            result = self.client.admin.command('listShards')
            return result.get('shards', [])
        except Exception as e:
            self.logger.error(f"Error getting shards: {e}")
            return []
    
    def is_connected(self) -> bool:
        """
        Check if client is connected
        
        Returns:
            True if connected, False otherwise
        """
        if not self.client:
            return False
            
        try:
            self.client.admin.command('ping')
            return True
        except:
            return False
