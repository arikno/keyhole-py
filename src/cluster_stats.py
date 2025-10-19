"""
Cluster Statistics Collection for Keyhole Python
Collects comprehensive MongoDB cluster information
"""

import logging
import time
from typing import Dict, List, Any, Optional
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from datetime import datetime
import json
from .enhanced_analysis import EnhancedAnalyzer, DetailedIndex, CollectionStructureAnalysis, PerformanceMetrics


@dataclass
class BuildInfo:
    """Build information structure"""
    version: str = ""
    git_version: str = ""
    modules: List[str] = field(default_factory=list)


@dataclass
class HostInfo:
    """Host information structure"""
    hostname: str = ""
    os_name: str = ""
    os_type: str = ""
    cpu_arch: str = ""
    num_cores: int = 0
    mem_size_mb: int = 0
    mem_limit_mb: int = 0


@dataclass
class ServerStatus:
    """Server status structure"""
    host: str = ""
    version: str = ""
    process: str = ""
    uptime: int = 0
    local_time: datetime = field(default_factory=datetime.now)
    
    # Connections
    connections_current: int = 0
    connections_available: int = 0
    connections_total_created: int = 0
    connections_active: int = 0
    
    # Memory
    mem_resident: int = 0
    mem_virtual: int = 0
    
    # Operations
    opcounters_command: int = 0
    opcounters_insert: int = 0
    opcounters_query: int = 0
    opcounters_update: int = 0
    opcounters_delete: int = 0
    opcounters_getmore: int = 0
    
    # Replication
    repl_set_name: str = ""
    repl_is_master: bool = False
    repl_secondary: bool = False
    repl_hosts: List[str] = field(default_factory=list)
    
    # Sharding
    sharding_configsvr: str = ""
    sharding_max_chunk_size: int = 0
    
    # Storage Engine
    storage_engine: str = ""


@dataclass
class DatabaseStats:
    """Database statistics structure"""
    name: str = ""
    size_on_disk: int = 0
    empty: bool = False
    data_size: int = 0
    storage_size: int = 0
    index_size: int = 0
    objects: int = 0
    indexes: int = 0
    avg_obj_size: float = 0.0


@dataclass
class CollectionStats:
    """Collection statistics structure"""
    name: str = ""
    namespace: str = ""
    count: int = 0
    size: int = 0
    storage_size: int = 0
    total_index_size: int = 0
    avg_obj_size: float = 0.0
    capped: bool = False
    sharded: bool = False
    nindexes: int = 0
    indexes: List[Dict[str, Any]] = field(default_factory=list)
    sample_docs: List[Dict[str, Any]] = field(default_factory=list)
    
    # Enhanced analysis data
    detailed_indexes: List[DetailedIndex] = field(default_factory=list)
    structure_analysis: Optional[CollectionStructureAnalysis] = None


@dataclass
class ClusterStats:
    """Complete cluster statistics structure"""
    # Basic info
    cluster_type: str = ""
    host: str = ""
    version: str = ""
    collection_time: datetime = field(default_factory=datetime.now)
    
    # Server information
    build_info: BuildInfo = field(default_factory=BuildInfo)
    host_info: HostInfo = field(default_factory=HostInfo)
    server_status: ServerStatus = field(default_factory=ServerStatus)
    
    # Cluster topology
    replica_set_status: Dict[str, Any] = field(default_factory=dict)
    sharding_status: Dict[str, Any] = field(default_factory=dict)
    shards: List[Dict[str, Any]] = field(default_factory=list)
    
    # Database and collection statistics
    databases: List[DatabaseStats] = field(default_factory=list)
    collections: List[CollectionStats] = field(default_factory=list)
    
    # Performance metrics
    total_operations_per_second: float = 0.0
    total_memory_usage_mb: int = 0
    total_storage_size_gb: float = 0.0
    
    # Enhanced analysis data
    detailed_performance_metrics: Optional[PerformanceMetrics] = None
    cluster_health: Dict[str, Any] = field(default_factory=dict)


class ClusterStatsCollector:
    """Collects comprehensive MongoDB cluster statistics"""
    
    def __init__(self, mongo_client, verbose: bool = False):
        """
        Initialize cluster stats collector
        
        Args:
            mongo_client: MongoDBClient instance
            verbose: Enable verbose logging
        """
        self.client = mongo_client
        self.verbose = verbose
        self.logger = logging.getLogger(__name__)
        self.enhanced_analyzer = EnhancedAnalyzer(mongo_client, verbose)
        
    def collect_all_stats(self, include_databases: List[str] = None, fast_mode: bool = False) -> ClusterStats:
        """
        Collect comprehensive cluster statistics
        
        Args:
            include_databases: List of specific databases to include (None for all)
            fast_mode: Skip detailed collection analysis for faster execution
            
        Returns:
            Complete ClusterStats object
        """
        self.logger.info("Starting comprehensive cluster statistics collection")
        start_time = time.time()
        
        stats = ClusterStats()
        
        # Collect basic server information
        self._collect_server_info(stats)
        
        # Determine cluster type and collect topology-specific info
        cluster_type = self.client.get_cluster_type()
        stats.cluster_type = cluster_type
        
        self._collect_topology_info(stats, cluster_type)
        
        # Collect database and collection statistics
        self._collect_database_stats(stats, include_databases, fast_mode)
        
        # Calculate performance metrics
        self._calculate_performance_metrics(stats)
        
        # Enhanced analysis (skip if fast mode)
        if not fast_mode:
            self._perform_enhanced_analysis(stats)
        
        collection_time = time.time() - start_time
        self.logger.info(f"Cluster statistics collection completed in {collection_time:.2f} seconds")
        
        return stats
    
    def _collect_server_info(self, stats: ClusterStats):
        """Collect basic server information"""
        self.logger.info("Collecting server information")
        
        try:
            server_info = self.client.get_server_info()
            
            # Build info
            if 'buildInfo' in server_info:
                build = server_info['buildInfo']
                stats.build_info.version = build.get('version', '')
                stats.build_info.git_version = build.get('gitVersion', '')
                stats.build_info.modules = build.get('modules', [])
            
            # Host info
            if 'hostInfo' in server_info:
                host = server_info['hostInfo']
                stats.host_info.hostname = host.get('hostname', '')
                stats.host_info.os_name = host.get('os', {}).get('name', '')
                stats.host_info.os_type = host.get('os', {}).get('type', '')
                stats.host_info.cpu_arch = host.get('system', {}).get('cpuArch', '')
                stats.host_info.num_cores = host.get('system', {}).get('numCores', 0)
                stats.host_info.mem_size_mb = host.get('system', {}).get('memSizeMB', 0)
                stats.host_info.mem_limit_mb = host.get('system', {}).get('memLimitMB', 0)
            
            # Server status
            if 'serverStatus' in server_info:
                status = server_info['serverStatus']
                stats.server_status.host = status.get('host', '')
                stats.server_status.version = status.get('version', '')
                stats.server_status.process = status.get('process', '')
                stats.server_status.uptime = status.get('uptime', 0)
                stats.server_status.local_time = datetime.now()
                
                # Connections
                conn = status.get('connections', {})
                stats.server_status.connections_current = conn.get('current', 0)
                stats.server_status.connections_available = conn.get('available', 0)
                stats.server_status.connections_total_created = conn.get('totalCreated', 0)
                stats.server_status.connections_active = conn.get('active', 0)
                
                # Memory
                mem = status.get('mem', {})
                self.logger.debug(f"Memory data from serverStatus: {mem}")
                stats.server_status.mem_resident = mem.get('resident', 0)
                stats.server_status.mem_virtual = mem.get('virtual', 0)
                self.logger.debug(f"Extracted mem_resident: {stats.server_status.mem_resident}, mem_virtual: {stats.server_status.mem_virtual}")
                
                # If resident is 0, try to use virtual as fallback
                if stats.server_status.mem_resident == 0 and stats.server_status.mem_virtual > 0:
                    self.logger.debug(f"mem_resident is 0, using mem_virtual as fallback: {stats.server_status.mem_virtual}")
                    stats.server_status.mem_resident = stats.server_status.mem_virtual
                
                # Operation counters
                ops = status.get('opcounters', {})
                stats.server_status.opcounters_command = ops.get('command', 0)
                stats.server_status.opcounters_insert = ops.get('insert', 0)
                stats.server_status.opcounters_query = ops.get('query', 0)
                stats.server_status.opcounters_update = ops.get('update', 0)
                stats.server_status.opcounters_delete = ops.get('delete', 0)
                stats.server_status.opcounters_getmore = ops.get('getmore', 0)
                
                # Replication
                repl = status.get('repl', {})
                stats.server_status.repl_set_name = repl.get('setName', '')
                stats.server_status.repl_is_master = repl.get('isMaster', False)
                stats.server_status.repl_secondary = repl.get('secondary', False)
                stats.server_status.repl_hosts = repl.get('hosts', [])
                
                # Sharding
                sharding = status.get('sharding', {})
                stats.server_status.sharding_configsvr = sharding.get('configsvrConnectionString', '')
                stats.server_status.sharding_max_chunk_size = sharding.get('maxChunkSizeInBytes', 0)
                
                # Storage engine
                storage = status.get('storageEngine', {})
                stats.server_status.storage_engine = storage.get('name', '')
            
            # Set basic cluster info
            stats.host = stats.host_info.hostname
            stats.version = stats.build_info.version
            
        except Exception as e:
            self.logger.error(f"Error collecting server info: {e}")
    
    def _collect_topology_info(self, stats: ClusterStats, cluster_type: str):
        """Collect topology-specific information"""
        self.logger.info(f"Collecting topology information for {cluster_type} cluster")
        
        try:
            if cluster_type == "replica":
                # Get replica set status
                stats.replica_set_status = self.client.get_replica_set_status()
                
            elif cluster_type == "sharded":
                # Get sharding status and shard information
                stats.sharding_status = self.client.get_sharding_status()
                stats.shards = self.client.get_shards()
                
        except Exception as e:
            self.logger.error(f"Error collecting topology info: {e}")
    
    def _collect_database_stats(self, stats: ClusterStats, include_databases: List[str] = None, fast_mode: bool = False):
        """Collect database and collection statistics"""
        self.logger.info("Collecting database and collection statistics")
        
        try:
            # Get list of databases
            all_databases = self.client.get_databases()
            
            # Filter databases if specified
            if include_databases:
                databases_to_analyze = [db for db in all_databases if db in include_databases]
            else:
                # Exclude system databases unless explicitly included
                databases_to_analyze = [db for db in all_databases 
                                     if not db.startswith('admin') and not db.startswith('local') and not db.startswith('config')]
            
            self.logger.info(f"Analyzing {len(databases_to_analyze)} databases: {databases_to_analyze}")
            
            # Collect database statistics in parallel
            with ThreadPoolExecutor(max_workers=5) as executor:
                future_to_db = {
                    executor.submit(self._collect_single_database_stats, db_name, fast_mode): db_name 
                    for db_name in databases_to_analyze
                }
                
                for future in as_completed(future_to_db):
                    db_name = future_to_db[future]
                    try:
                        db_stats, collection_stats = future.result()
                        if db_stats:
                            stats.databases.append(db_stats)
                        stats.collections.extend(collection_stats)
                    except Exception as e:
                        self.logger.error(f"Error collecting stats for database {db_name}: {e}")
                        
        except Exception as e:
            self.logger.error(f"Error collecting database stats: {e}")
    
    def _collect_single_database_stats(self, db_name: str, fast_mode: bool = False) -> tuple:
        """Collect statistics for a single database"""
        try:
            # Get database stats
            db_stats_raw = self.client.get_database_stats(db_name)
            
            # Determine if database is empty based on multiple indicators
            objects_count = db_stats_raw.get('objects', 0)
            collections_count = db_stats_raw.get('collections', 0)
            data_size = db_stats_raw.get('dataSize', 0)
            
            # Database is empty if it has no objects, no collections, and no data
            is_empty = (objects_count == 0 and collections_count == 0 and data_size == 0)
            
            self.logger.debug(f"Database {db_name} empty check: objects={objects_count}, collections={collections_count}, dataSize={data_size}, is_empty={is_empty}")
            
            db_stats = DatabaseStats(
                name=db_name,
                size_on_disk=db_stats_raw.get('dataSize', 0) + db_stats_raw.get('indexSize', 0),
                empty=is_empty,
                data_size=data_size,
                storage_size=db_stats_raw.get('storageSize', 0),
                index_size=db_stats_raw.get('indexSize', 0),
                objects=objects_count,
                indexes=db_stats_raw.get('indexes', 0),
                avg_obj_size=db_stats_raw.get('avgObjSize', 0)
            )
            
            # Get collections
            collections = self.client.get_collections(db_name)
            collection_stats = []
            
            # Collect collection statistics in parallel
            if not fast_mode and collections:
                with ThreadPoolExecutor(max_workers=10) as executor:
                    future_to_coll = {
                        executor.submit(self._collect_single_collection_stats, db_name, coll_name): coll_name
                        for coll_name in collections
                        if not coll_name.startswith('system.')
                    }
                    
                    for future in as_completed(future_to_coll):
                        coll_name = future_to_coll[future]
                        try:
                            coll_stats = future.result()
                            if coll_stats:
                                collection_stats.append(coll_stats)
                        except Exception as e:
                            # Check if this might be a view that wasn't filtered out
                            if self.client.is_view(db_name, coll_name):
                                self.logger.debug(f"Skipping view (detected during analysis): {db_name}.{coll_name}")
                            else:
                                self.logger.error(f"Error collecting stats for collection {db_name}.{coll_name}: {e}")
            
            return db_stats, collection_stats
            
        except Exception as e:
            self.logger.error(f"Error collecting stats for database {db_name}: {e}")
            return None, []
    
    def _collect_single_collection_stats(self, db_name: str, collection_name: str) -> Optional[CollectionStats]:
        """Collect statistics for a single collection"""
        try:
            # Check if this is a view first
            if self.client.is_view(db_name, collection_name):
                self.logger.debug(f"Skipping view: {db_name}.{collection_name}")
                return None
            
            # Get collection stats
            coll_stats_raw = self.client.get_collection_stats(db_name, collection_name)
            
            coll_stats = CollectionStats(
                name=collection_name,
                namespace=f"{db_name}.{collection_name}",
                count=coll_stats_raw.get('count', 0),
                size=coll_stats_raw.get('size', 0),
                storage_size=coll_stats_raw.get('storageSize', 0),
                total_index_size=coll_stats_raw.get('totalIndexSize', 0),
                avg_obj_size=coll_stats_raw.get('avgObjSize', 0),
                capped=coll_stats_raw.get('capped', False),
                sharded=coll_stats_raw.get('sharded', False),
                nindexes=coll_stats_raw.get('nindexes', 0)
            )
            
            # Get indexes
            indexes = self.client.get_indexes(db_name, collection_name)
            coll_stats.indexes = indexes
            
            # Get sample documents (limit to 3 for performance)
            sample_docs = self.client.get_sample_documents(db_name, collection_name, limit=3)
            coll_stats.sample_docs = sample_docs
            
            return coll_stats
            
        except Exception as e:
            self.logger.error(f"Error collecting stats for collection {db_name}.{collection_name}: {e}")
            return None
    
    def _calculate_performance_metrics(self, stats: ClusterStats):
        """Calculate performance metrics"""
        self.logger.info("Calculating performance metrics")
        
        try:
            # Calculate total operations per second (if uptime is available)
            if stats.server_status.uptime > 0:
                total_ops = (stats.server_status.opcounters_command + 
                           stats.server_status.opcounters_insert +
                           stats.server_status.opcounters_query +
                           stats.server_status.opcounters_update +
                           stats.server_status.opcounters_delete +
                           stats.server_status.opcounters_getmore)
                
                stats.total_operations_per_second = total_ops / stats.server_status.uptime
            
            # Calculate total memory usage
            stats.total_memory_usage_mb = stats.server_status.mem_resident
            
            # Calculate total storage size
            total_storage_bytes = sum(db.storage_size for db in stats.databases)
            stats.total_storage_size_gb = total_storage_bytes / (1024 ** 3)
            
        except Exception as e:
            self.logger.error(f"Error calculating performance metrics: {e}")
    
    def _perform_enhanced_analysis(self, stats: ClusterStats):
        """Perform enhanced analysis on collections and cluster"""
        self.logger.info("Performing enhanced analysis")
        
        try:
            # Get detailed performance metrics
            stats.detailed_performance_metrics = self.enhanced_analyzer.get_detailed_performance_metrics()
            
            # Analyze cluster health
            stats.cluster_health = self.enhanced_analyzer.analyze_cluster_health()
            
            # Enhanced collection analysis
            for collection in stats.collections:
                if not collection.name.startswith('system.'):
                    self._enhance_collection_analysis(collection)
            
        except Exception as e:
            self.logger.error(f"Error performing enhanced analysis: {e}")
    
    def _enhance_collection_analysis(self, collection: CollectionStats):
        """Enhance collection analysis with detailed index and structure analysis"""
        try:
            db_name, coll_name = collection.namespace.split('.', 1)
            
            # Cache collection stats for fragmentation analysis
            collection_stats = {
                'storageSize': collection.storage_size,
                'size': collection.size,
                'count': collection.count,
                'avg_obj_size': collection.avg_obj_size
            }
            self.enhanced_analyzer.cache_collection_stats(collection.namespace, collection_stats)
            
            # Detailed index analysis
            self.logger.debug(f"Analyzing indexes for {collection.namespace}")
            collection.detailed_indexes = self.enhanced_analyzer.analyze_collection_indexes(db_name, coll_name)
            
            # Collection structure analysis
            self.logger.debug(f"Analyzing structure for {collection.namespace}")
            collection.structure_analysis = self.enhanced_analyzer.analyze_collection_structure(
                db_name, coll_name, sample_size=50
            )
            
        except Exception as e:
            self.logger.error(f"Error enhancing analysis for {collection.namespace}: {e}")
    
    def get_cluster_summary(self) -> str:
        """
        Get a one-line cluster summary
        
        Returns:
            Formatted cluster summary string
        """
        try:
            # Get basic info without full collection
            server_info = self.client.get_server_info()
            cluster_type = self.client.get_cluster_type()
            
            build_info = server_info.get('buildInfo', {})
            host_info = server_info.get('hostInfo', {})
            server_status = server_info.get('serverStatus', {})
            
            version = build_info.get('version', 'Unknown')
            hostname = host_info.get('hostname', 'Unknown')
            os_name = host_info.get('os', {}).get('name', 'Unknown')
            process = server_status.get('process', 'Unknown')
            num_cores = host_info.get('system', {}).get('numCores', 0)
            mem_size_mb = host_info.get('system', {}).get('memSizeMB', 0)
            
            # Get shard count for sharded clusters
            shard_info = ""
            if cluster_type == "sharded":
                shards = self.client.get_shards()
                shard_info = f" ({len(shards)} shards)"
            
            return (f"MongoDB v{version} {hostname} ({os_name}) {process} {cluster_type}{shard_info} "
                   f"{num_cores} cores {mem_size_mb}MB memory")
                   
        except Exception as e:
            self.logger.error(f"Error getting cluster summary: {e}")
            return f"Error getting cluster summary: {e}"
