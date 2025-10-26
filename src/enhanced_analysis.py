"""
Enhanced Analysis for Keyhole Python
Adds detailed index analysis, collection structure analysis, and performance metrics
"""

import json
import logging
import re
from typing import Dict, List, Any, Optional, Tuple
from dataclasses import dataclass, field
from datetime import datetime
import pymongo
from pymongo import MongoClient
from pymongo.errors import OperationFailure


@dataclass
class IndexUsageStats:
    """Index usage statistics"""
    name: str = ""
    total_ops: int = 0
    accesses: List[Dict[str, Any]] = field(default_factory=list)
    since: Optional[datetime] = None
    host: str = ""
    shard: str = ""


@dataclass
class DetailedIndex:
    """Detailed index information with usage stats"""
    name: str = ""
    key: Dict[str, int] = field(default_factory=dict)
    key_string: str = ""
    fields: List[str] = field(default_factory=list)
    effective_key: str = ""
    
    # Index properties
    unique: bool = False
    sparse: bool = False
    background: bool = False
    partial_filter_expression: Dict[str, Any] = field(default_factory=dict)
    collation: Dict[str, Any] = field(default_factory=dict)
    expire_after_seconds: int = -1
    weights: Dict[str, int] = field(default_factory=dict)
    version: int = 2
    
    # Analysis results
    is_shard_key: bool = False
    is_duplicate: bool = False
    total_ops: int = 0
    usage_stats: List[IndexUsageStats] = field(default_factory=list)
    
    # Recommendations
    recommendation: str = ""
    issues: List[str] = field(default_factory=list)


@dataclass
class CollectionStructureAnalysis:
    """Analysis of collection document structure"""
    namespace: str = ""
    
    # Document structure analysis
    max_nesting_depth: int = 0
    avg_nesting_depth: float = 0.0
    has_deep_nesting: bool = False
    deep_nesting_samples: List[Dict[str, Any]] = field(default_factory=list)
    
    # Array analysis
    has_large_arrays: bool = False
    max_array_size: int = 0
    avg_array_size: float = 0.0
    large_array_samples: List[Dict[str, Any]] = field(default_factory=list)
    
    # Field analysis
    total_fields: int = 0
    total_queryable_paths: int = 0  # Includes array indices
    common_fields: List[Tuple[str, int]] = field(default_factory=list)
    field_types: Dict[str, str] = field(default_factory=dict)
    
    # Fragmentation analysis
    fragmentation_percentage: float = 0.0
    storage_efficiency: float = 0.0
    wasted_space_bytes: int = 0
    fragmentation_level: str = "low"  # low, medium, high, critical
    
    # Recommendations
    recommendations: List[str] = field(default_factory=list)
    issues: List[str] = field(default_factory=list)


@dataclass
class PerformanceMetrics:
    """Detailed performance metrics"""
    # Operation rates
    ops_per_second: float = 0.0
    reads_per_second: float = 0.0
    writes_per_second: float = 0.0
    
    # Index performance
    index_hit_ratio: float = 0.0
    unused_indexes: List[str] = field(default_factory=list)
    overused_indexes: List[str] = field(default_factory=list)
    
    # Memory metrics
    working_set_size: int = 0
    cache_hit_ratio: float = 0.0
    
    # Connection metrics
    current_connections: int = 0
    available_connections: int = 0
    connection_pool_utilization: float = 0.0


class EnhancedAnalyzer:
    """Enhanced analysis engine for MongoDB clusters"""
    
    def __init__(self, mongo_client, verbose: bool = False):
        """
        Initialize enhanced analyzer
        
        Args:
            mongo_client: MongoDBClient instance
            verbose: Enable verbose logging
        """
        self.client = mongo_client
        self.verbose = verbose
        self.logger = logging.getLogger(__name__)
        self._collection_stats_cache = {}
    
    def analyze_collection_indexes(self, db_name: str, collection_name: str) -> List[DetailedIndex]:
        """
        Analyze indexes for a collection with detailed usage statistics
        
        Args:
            db_name: Database name
            collection_name: Collection name
            
        Returns:
            List of detailed index information
        """
        self.logger.debug(f"Analyzing indexes for {db_name}.{collection_name}")
        
        try:
            collection = self.client.client[db_name][collection_name]
            
            # Get index usage statistics
            index_usage_stats = self._get_index_usage_stats(collection)
            
            # Get index definitions
            indexes = self.client.get_indexes(db_name, collection_name)
            
            detailed_indexes = []
            for index in indexes:
                detailed_index = self._create_detailed_index(index, index_usage_stats)
                detailed_index = self._analyze_index_properties(detailed_index, db_name, collection_name)
                detailed_indexes.append(detailed_index)
            
            # Sort by effective key for consistent ordering
            detailed_indexes.sort(key=lambda x: x.effective_key)
            
            # Check for duplicate and redundant indexes
            self._check_duplicate_indexes(detailed_indexes)
            self._check_redundant_indexes(detailed_indexes)
            
            # Analyze each index for additional properties
            for detailed_index in detailed_indexes:
                detailed_index = self._analyze_index_properties(detailed_index, db_name, collection_name)
            
            return detailed_indexes
            
        except Exception as e:
            self.logger.error(f"Error analyzing indexes for {db_name}.{collection_name}: {e}")
            return []
    
    def _get_index_usage_stats(self, collection) -> Dict[str, IndexUsageStats]:
        """Get index usage statistics using $indexStats aggregation"""
        try:
            pipeline = [{"$indexStats": {}}]
            usage_stats = {}
            
            for result in collection.aggregate(pipeline):
                index_name = result.get('name', '')
                accesses = result.get('accesses', {})
                
                usage_stats[index_name] = IndexUsageStats(
                    name=index_name,
                    total_ops=accesses.get('ops', 0),
                    since=accesses.get('since'),
                    host=result.get('host', ''),
                    shard=result.get('shard', '')
                )
                
            return usage_stats
            
        except OperationFailure as e:
            # $indexStats might not be available in all MongoDB versions
            self.logger.debug(f"$indexStats not available: {e}")
            return {}
        except Exception as e:
            self.logger.error(f"Error getting index usage stats: {e}")
            return {}
    
    def _create_detailed_index(self, index: Dict[str, Any], usage_stats: Dict[str, IndexUsageStats]) -> DetailedIndex:
        """Create detailed index from basic index definition"""
        key = index.get('key', {})
        fields = list(key.keys()) if key else []
        
        # Create key string representation
        key_parts = []
        for field, direction in key.items():
            key_parts.append(f"{field}: {direction}")
        key_string = "{" + ", ".join(key_parts) + "}"
        
        # Create effective key (replace -1 with 1 for sorting)
        effective_key = key_string.replace(": -1", ": 1")
        
        detailed_index = DetailedIndex(
            name=index.get('name', ''),
            key=key,
            key_string=key_string,
            fields=fields,
            effective_key=effective_key,
            unique=index.get('unique', False),
            sparse=index.get('sparse', False),
            background=index.get('background', False),
            partial_filter_expression=index.get('partialFilterExpression', {}),
            collation=index.get('collation', {}),
            expire_after_seconds=index.get('expireAfterSeconds', -1),
            weights=index.get('weights', {}),
            version=index.get('v', 2)
        )
        
        # Add usage statistics if available
        if detailed_index.name in usage_stats:
            usage_stat = usage_stats[detailed_index.name]
            detailed_index.total_ops = usage_stat.total_ops
            detailed_index.usage_stats = [usage_stat]
        
        return detailed_index
    
    def _analyze_index_properties(self, index: DetailedIndex, db_name: str, collection_name: str) -> DetailedIndex:
        """Analyze index properties and add recommendations"""
        issues = []
        recommendations = []
        
        # Check if index is a shard key
        try:
            ns = f"{db_name}.{collection_name}"
            shard_key_query = {"_id": ns, "key": index.key}
            
            # This would require access to config database
            # For now, we'll skip this check
            index.is_shard_key = False
            
        except Exception as e:
            self.logger.debug(f"Could not check shard key for {index.name}: {e}")
        
        # Analyze TTL indexes
        if index.expire_after_seconds > 0:
            if index.expire_after_seconds < 60:
                issues.append(f"TTL index with very short expiration ({index.expire_after_seconds}s)")
                recommendations.append("Consider if TTL expiration is appropriate for your use case")
        
        # Analyze partial indexes
        if index.partial_filter_expression:
            recommendations.append("Partial index detected - ensure queries match the filter expression")
        
        # Analyze sparse indexes
        if index.sparse:
            recommendations.append("Sparse index detected - ensure queries handle null values correctly")
        
        # Analyze index usage
        if index.total_ops == 0 and index.name != "_id_":
            issues.append("Unused index")
            recommendations.append("Consider dropping unused indexes to improve write performance")
        
        # Analyze compound indexes
        if len(index.fields) > 1:
            # Check for optimal field ordering (equality fields first, then range fields)
            recommendations.append("Compound index - ensure optimal field ordering (equality before range)")
        
        # Analyze text indexes
        if index.weights:
            recommendations.append("Text index detected - monitor performance for text search queries")
        
        # Preserve existing issues (from redundant/duplicate detection) and add new ones
        existing_issues = getattr(index, 'issues', [])
        all_issues = existing_issues + [issue for issue in issues if issue not in existing_issues]
        index.issues = all_issues
        
        # Preserve existing recommendation if it exists, otherwise use new ones
        if hasattr(index, 'recommendation') and index.recommendation:
            existing_rec = index.recommendation
            new_rec = "; ".join(recommendations) if recommendations else "No issues detected"
            index.recommendation = f"{existing_rec}; {new_rec}" if new_rec != "No issues detected" else existing_rec
        else:
            index.recommendation = "; ".join(recommendations) if recommendations else "No issues detected"
        
        return index
    
    def _check_duplicate_indexes(self, indexes: List[DetailedIndex]):
        """Check for duplicate indexes"""
        for i, index1 in enumerate(indexes):
            if index1.key_string == "{ _id: 1 }" or index1.is_shard_key:
                continue
                
            for j, index2 in enumerate(indexes[i+1:], i+1):
                if index1.effective_key == index2.effective_key:
                    index1.is_duplicate = True
                    index2.is_duplicate = True
                    index1.issues.append(f"Duplicate of index {index2.name}")
                    index2.issues.append(f"Duplicate of index {index1.name}")
    
    def _check_redundant_indexes(self, indexes: List[DetailedIndex]):
        """Check for redundant indexes (compound prefix redundancy)"""
        for i, index1 in enumerate(indexes):
            if index1.key_string == "{ _id: 1 }" or index1.is_shard_key:
                continue
                
            # Check if any other index is a prefix of this compound index
            for j, index2 in enumerate(indexes):
                if i == j or index2.key_string == "{ _id: 1 }" or index2.is_shard_key:
                    continue
                
                # Check if index2 is a prefix of index1 (compound index)
                if len(index2.fields) < len(index1.fields):
                    # Check if all fields in index2 are the first fields in index1
                    if index2.fields == index1.fields[:len(index2.fields)]:
                        # Check if the sort directions match (but allow -1 to match 1 for redundancy)
                        directions_match = True
                        for k, field in enumerate(index2.fields):
                            field1_dir = index1.key.get(field)
                            field2_dir = index2.key.get(field)
                            # For redundancy purposes, -1 and 1 are equivalent (both can serve range queries)
                            if field1_dir != field2_dir and not (abs(field1_dir) == 1 and abs(field2_dir) == 1):
                                directions_match = False
                                break
                        
                        if directions_match:
                            # index2 is redundant because index1 can serve the same queries
                            index2.is_duplicate = True
                            issue_msg = f"Redundant prefix of compound index {index1.name}"
                            if issue_msg not in index2.issues:  # Avoid duplicates
                                index2.issues.append(issue_msg)
                            index2.recommendation = f"Consider dropping this index - {index1.name} can serve the same queries"
                            self.logger.debug(f"Marked index {index2.name} as redundant due to {index1.name}")
    
    def analyze_collection_structure(self, db_name: str, collection_name: str, sample_size: int = 100) -> CollectionStructureAnalysis:
        """
        Analyze collection document structure for nesting depth, arrays, etc.
        
        Args:
            db_name: Database name
            collection_name: Collection name
            sample_size: Number of documents to sample for analysis
            
        Returns:
            Collection structure analysis
        """
        self.logger.debug(f"Analyzing structure for {db_name}.{collection_name}")
        
        analysis = CollectionStructureAnalysis(namespace=f"{db_name}.{collection_name}")
        
        try:
            collection = self.client.client[db_name][collection_name]
            
            # Get sample documents with deterministic ordering for consistency
            # Sort by _id descending to get latest documents
            sample_docs = list(collection.aggregate([
                {"$sort": {"_id": -1}},  # Sort by _id descending for latest docs
                {"$limit": sample_size}
            ]))
            
            if not sample_docs:
                return analysis
            
            # Analyze document structure
            nesting_depths = []
            array_sizes = []
            field_counts = []
            queryable_path_counts = []
            all_fields = {}
            field_types = {}
            
            for doc in sample_docs:
                # Analyze nesting depth
                depth = self._calculate_nesting_depth(doc)
                nesting_depths.append(depth)
                
                if depth > analysis.max_nesting_depth:
                    analysis.max_nesting_depth = depth
                    if depth > 5:  # Consider deep nesting if > 5 levels
                        analysis.has_deep_nesting = True
                        if len(analysis.deep_nesting_samples) < 3:
                            analysis.deep_nesting_samples.append(doc)
                
                # Analyze arrays
                doc_array_sizes = self._find_large_arrays(doc)
                array_sizes.extend(doc_array_sizes)
                
                if doc_array_sizes and max(doc_array_sizes) > analysis.max_array_size:
                    analysis.max_array_size = max(doc_array_sizes)
                    if max(doc_array_sizes) > 1000:  # Consider large if > 1000 elements
                        analysis.has_large_arrays = True
                        if len(analysis.large_array_samples) < 3:
                            analysis.large_array_samples.append(doc)
                
                # Analyze fields using correct counting method
                field_counts_data = self._extract_fields_alternative(doc)
                field_counts.append(field_counts_data['distinct_fields'])
                
                # Analyze queryable paths (includes array indices)
                doc_queryable_paths = self._extract_queryable_paths(doc)
                queryable_path_counts.append(len(doc_queryable_paths))
                
                # Extract field names for common fields analysis
                doc_fields = self._extract_fields(doc)
                for field in doc_fields:
                    all_fields[field] = all_fields.get(field, 0) + 1
                    if field not in field_types:
                        # Try to get the field type more accurately
                        try:
                            field_value = self._get_nested_value(doc, field)
                            field_types[field] = type(field_value).__name__
                        except:
                            field_types[field] = 'unknown'
            
            # Calculate averages and statistics
            analysis.avg_nesting_depth = sum(nesting_depths) / len(nesting_depths) if nesting_depths else 0
            analysis.avg_array_size = sum(array_sizes) / len(array_sizes) if array_sizes else 0
            
            # Field counting - use consistent methodology
            # total_fields = unique fields found across latest sampled documents
            # This provides deterministic results by always sampling the same latest documents
            analysis.total_fields = len(all_fields)
            
            # total_queryable_paths = average queryable paths per document
            analysis.total_queryable_paths = sum(queryable_path_counts) / len(queryable_path_counts) if queryable_path_counts else 0
            
            # Add additional field statistics for better analysis
            analysis.avg_fields_per_doc = sum(field_counts) / len(field_counts) if field_counts else 0
            analysis.max_fields_per_doc = max(field_counts) if field_counts else 0
            analysis.min_fields_per_doc = min(field_counts) if field_counts else 0
            
            # Get common fields
            analysis.common_fields = sorted(all_fields.items(), key=lambda x: x[1], reverse=True)[:10]
            analysis.field_types = field_types
            
            # Calculate fragmentation (if collection stats available)
            collection_namespace = f"{db_name}.{collection_name}"
            if collection_namespace in self._collection_stats_cache:
                stats = self._collection_stats_cache[collection_namespace]
                storage_size = stats.get('storageSize', 0)
                data_size = stats.get('size', 0)  # dataSize in collection stats
                
                frag_metrics = self.calculate_fragmentation(storage_size, data_size)
                analysis.fragmentation_percentage = frag_metrics['fragmentation_percentage']
                analysis.storage_efficiency = frag_metrics['storage_efficiency']
                analysis.wasted_space_bytes = frag_metrics['wasted_space_bytes']
                analysis.fragmentation_level = frag_metrics['fragmentation_level']
            
            # Generate recommendations
            analysis = self._generate_structure_recommendations(analysis)
            
            return analysis
            
        except Exception as e:
            self.logger.error(f"Error analyzing collection structure for {db_name}.{collection_name}: {e}")
            return analysis
    
    def _calculate_nesting_depth(self, obj: Any, current_depth: int = 0) -> int:
        """Calculate maximum nesting depth of a document"""
        if not isinstance(obj, dict):
            return current_depth
        
        if not obj:
            return current_depth
        
        max_depth = current_depth
        for value in obj.values():
            if isinstance(value, (dict, list)):
                depth = self._calculate_nesting_depth(value, current_depth + 1)
                max_depth = max(max_depth, depth)
        
        return max_depth
    
    def _find_large_arrays(self, obj: Any, path: str = "") -> List[int]:
        """Find arrays and return their sizes"""
        array_sizes = []
        
        if isinstance(obj, list):
            array_sizes.append(len(obj))
            # Also check nested arrays
            for i, item in enumerate(obj):
                array_sizes.extend(self._find_large_arrays(item, f"{path}[{i}]"))
        elif isinstance(obj, dict):
            for key, value in obj.items():
                array_sizes.extend(self._find_large_arrays(value, f"{path}.{key}" if path else key))
        
        return array_sizes
    
    def _extract_fields(self, obj: Any, path: str = "") -> List[str]:
        """Extract all field paths from a document (arrays counted as single fields)"""
        fields = []
        
        if isinstance(obj, dict):
            for key, value in obj.items():
                field_path = f"{path}.{key}" if path else key
                fields.append(field_path)
                
                # Recursively extract fields from nested objects
                if isinstance(value, dict):
                    fields.extend(self._extract_fields(value, field_path))
                elif isinstance(value, list) and value:
                    # For arrays, analyze the structure of the first element
                    # but only add the array field itself, not individual elements
                    if isinstance(value[0], (dict, list)):
                        fields.extend(self._extract_fields(value[0], f"{field_path}[]"))
        
        return fields
    
    def _extract_fields_alternative(self, obj: Any, path: str = "") -> Dict[str, int]:
        """
        Extract field counts using correct counting methodology
        
        Returns:
            Dict with different field counting approaches:
            - 'top_level': Only top-level fields
            - 'distinct_fields': Top-level fields + distinct fields from array objects (once per field type)
            - 'array_fields': Fields that are arrays
            - 'nested_fields': Fields in nested objects
        """
        counts = {
            'top_level': 0,
            'distinct_fields': 0,
            'array_fields': 0,
            'nested_fields': 0
        }
        
        if isinstance(obj, dict):
            # Count top-level fields
            counts['top_level'] = len(obj)
            counts['distinct_fields'] = len(obj)
            
            for key, value in obj.items():
                field_path = f"{path}.{key}" if path else key
                
                if isinstance(value, dict):
                    counts['nested_fields'] += 1
                    # Recursively count nested fields
                    nested_counts = self._extract_fields_alternative(value, field_path)
                    counts['distinct_fields'] += nested_counts['distinct_fields']
                    counts['nested_fields'] += nested_counts['nested_fields']
                elif isinstance(value, list):
                    counts['array_fields'] += 1
                    # Array itself counts as one field
                    
                    # Analyze array structure if it contains objects
                    if value and isinstance(value[0], dict):
                        # Count distinct fields from array objects (once per field type)
                        distinct_array_fields = self._extract_distinct_array_fields(value, field_path)
                        counts['distinct_fields'] += distinct_array_fields
                    # For primitive arrays, don't count individual items as fields
        
        return counts
    
    def _extract_distinct_array_fields(self, array: List[Any], base_path: str) -> int:
        """
        Extract distinct field count from array objects (count each field type once)
        
        Args:
            array: List of objects
            base_path: Base path for the array
            
        Returns:
            Number of distinct fields found in array objects
        """
        if not array or not isinstance(array[0], dict):
            return 0
        
        # Collect all unique field names from all objects in the array
        distinct_fields = set()
        
        for obj in array:
            if isinstance(obj, dict):
                for key in obj.keys():
                    distinct_fields.add(key)
        
        return len(distinct_fields)
    
    def _get_nested_value(self, obj: Any, field_path: str) -> Any:
        """Get value from nested field path like 'class.history'"""
        try:
            keys = field_path.split('.')
            current = obj
            for key in keys:
                if '[]' in key:
                    # Handle array notation
                    array_key = key.replace('[]', '')
                    if isinstance(current, dict) and array_key in current:
                        current = current[array_key]
                        if isinstance(current, list) and current:
                            current = current[0]  # Get first element
                    else:
                        return None
                else:
                    current = current[key]
            return current
        except (KeyError, TypeError, IndexError):
            return None
    
    def _extract_queryable_paths(self, obj: Any, path: str = "") -> List[str]:
        """Extract all queryable field paths including array indices"""
        fields = []
        
        if isinstance(obj, dict):
            for key, value in obj.items():
                field_path = f"{path}.{key}" if path else key
                fields.append(field_path)
                
                # Recursively extract paths from nested objects
                if isinstance(value, dict):
                    fields.extend(self._extract_queryable_paths(value, field_path))
                elif isinstance(value, list) and value:
                    # For arrays, add paths for each array element
                    for i, item in enumerate(value):
                        if isinstance(item, (dict, list)):
                            fields.extend(self._extract_queryable_paths(item, f"{field_path}[{i}]"))
                        else:
                            # For primitive array elements, just add the indexed path
                            fields.append(f"{field_path}[{i}]")
        
        return fields
    
    def cache_collection_stats(self, namespace: str, stats: Dict[str, Any]):
        """Cache collection stats for fragmentation analysis"""
        self._collection_stats_cache[namespace] = stats
    
    def calculate_fragmentation(self, storage_size: int, data_size: int) -> Dict[str, Any]:
        """Calculate fragmentation metrics based on storage size vs data size"""
        if storage_size <= 0:
            return {
                'fragmentation_percentage': 0.0,
                'storage_efficiency': 100.0,
                'wasted_space_bytes': 0,
                'fragmentation_level': 'unknown'
            }
        
        # Handle case where storage_size < data_size (compression, cloud metrics, etc.)
        if storage_size < data_size:
            # This can happen with compression or different MongoDB configurations
            return {
                'fragmentation_percentage': 0.0,
                'storage_efficiency': round((data_size / storage_size) * 100, 2),
                'wasted_space_bytes': 0,
                'fragmentation_level': 'compressed'
            }
        
        wasted_space = storage_size - data_size
        fragmentation_percentage = (wasted_space / storage_size) * 100
        storage_efficiency = (data_size / storage_size) * 100
        
        # Determine fragmentation level
        if fragmentation_percentage < 10:
            level = 'low'
        elif fragmentation_percentage < 25:
            level = 'medium'
        elif fragmentation_percentage < 50:
            level = 'high'
        else:
            level = 'critical'
        
        return {
            'fragmentation_percentage': round(fragmentation_percentage, 2),
            'storage_efficiency': round(storage_efficiency, 2),
            'wasted_space_bytes': wasted_space,
            'fragmentation_level': level
        }
    
    def _generate_structure_recommendations(self, analysis: CollectionStructureAnalysis) -> CollectionStructureAnalysis:
        """Generate recommendations based on structure analysis"""
        recommendations = []
        issues = []
        
        # Deep nesting analysis
        if analysis.has_deep_nesting:
            issues.append(f"Deep nesting detected (max depth: {analysis.max_nesting_depth})")
            recommendations.append("Consider denormalizing deeply nested documents for better query performance")
            recommendations.append("Use aggregation pipelines to flatten nested data when needed")
        
        # Large arrays analysis
        if analysis.has_large_arrays:
            issues.append(f"Large arrays detected (max size: {analysis.max_array_size})")
            recommendations.append("Consider splitting large arrays into separate collections")
            recommendations.append("Use pagination when working with large arrays")
        
        # Field analysis
        if analysis.total_fields > 50:
            issues.append(f"High field count ({analysis.total_fields})")
            recommendations.append("Consider document schema optimization")
        
        # Array size recommendations
        if analysis.avg_array_size > 100:
            recommendations.append("Monitor array growth patterns")
            recommendations.append("Consider using capped arrays or separate collections")
        
        # Fragmentation analysis
        if analysis.fragmentation_percentage > 0:
            if analysis.fragmentation_level == 'critical':
                issues.append(f"Critical fragmentation detected ({analysis.fragmentation_percentage}% wasted space)")
                recommendations.append("Consider compacting the collection to reclaim space")
                recommendations.append("Review data deletion patterns and consider archiving old data")
            elif analysis.fragmentation_level == 'high':
                issues.append(f"High fragmentation detected ({analysis.fragmentation_percentage}% wasted space)")
                recommendations.append("Monitor fragmentation trends and consider compaction if it worsens")
                recommendations.append("Review update patterns that may cause document growth")
            elif analysis.fragmentation_level == 'medium':
                recommendations.append("Fragmentation is moderate - monitor for trends")
        
        analysis.recommendations = recommendations
        analysis.issues = issues
        
        return analysis
    
    def get_detailed_performance_metrics(self) -> PerformanceMetrics:
        """Get detailed performance metrics"""
        try:
            server_status = self.client.client.admin.command('serverStatus')
            
            metrics = PerformanceMetrics()
            
            # Connection metrics
            connections = server_status.get('connections', {})
            metrics.current_connections = connections.get('current', 0)
            metrics.available_connections = connections.get('available', 0)
            
            if metrics.current_connections + metrics.available_connections > 0:
                metrics.connection_pool_utilization = (
                    metrics.current_connections / 
                    (metrics.current_connections + metrics.available_connections)
                )
            
            # Memory metrics
            mem = server_status.get('mem', {})
            metrics.working_set_size = mem.get('resident', 0)
            
            # Cache metrics (WiredTiger specific)
            wiredtiger = server_status.get('wiredTiger', {})
            if wiredtiger:
                cache = wiredtiger.get('cache', {})
                bytes_read = cache.get('bytes read into cache', 0)
                bytes_written = cache.get('bytes written from cache', 0)
                
                if bytes_read > 0:
                    metrics.cache_hit_ratio = 1.0 - (bytes_written / bytes_read)
            
            return metrics
            
        except Exception as e:
            self.logger.error(f"Error getting performance metrics: {e}")
            return PerformanceMetrics()
    
    def analyze_cluster_health(self, collections_data: List = None) -> Dict[str, Any]:
        """Analyze overall cluster health"""
        health_report = {
            'timestamp': datetime.now(),
            'overall_health': 'healthy',
            'issues': [],
            'recommendations': [],
            'metrics': {}
        }
        
        try:
            # Get server status
            server_status = self.client.client.admin.command('serverStatus')
            
            # Check memory usage (>75%)
            mem = server_status.get('mem', {})
            # Memory usage ratio: resident / virtual
            if mem.get('virtual', 0) > 0 and mem.get('resident', 0) > 0:
                memory_usage_ratio = mem.get('resident', 0) / mem.get('virtual', 0)
                if memory_usage_ratio > 0.75:
                    health_report['issues'].append(f"High memory usage detected ({memory_usage_ratio*100:.1f}%)")
                    health_report['recommendations'].append("Consider increasing memory or optimizing queries")
            
            # Check connection usage (>75%)
            connections = server_status.get('connections', {})
            current = connections.get('current', 0)
            available = connections.get('available', 0)
            
            if current + available > 0:
                connection_usage = current / (current + available)
                if connection_usage > 0.75:
                    health_report['issues'].append(f"High connection usage detected ({connection_usage*100:.1f}%)")
                    health_report['recommendations'].append("Consider connection pooling optimization")
            
            # Check nesting level per table (>6)
            if collections_data:
                for coll in collections_data:
                    if hasattr(coll, 'structure_analysis') and coll.structure_analysis:
                        nesting = coll.structure_analysis.max_nesting_depth
                        if nesting > 6:
                            health_report['issues'].append(f"Deep nesting detected in {coll.namespace} (depth: {nesting})")
                            health_report['recommendations'].append(f"Consider denormalizing collection {coll.namespace}")
                    
                    # Check array items (>1000)
                    if hasattr(coll, 'structure_analysis') and coll.structure_analysis:
                        array_size = coll.structure_analysis.max_array_size
                        if array_size > 1000:
                            health_report['issues'].append(f"Large array detected in {coll.namespace} ({array_size} items)")
                            health_report['recommendations'].append(f"Consider splitting large arrays in collection {coll.namespace}")
                    
                    # Check number of indexes (>10)
                    if hasattr(coll, 'nindexes') and coll.nindexes > 10:
                        health_report['issues'].append(f"Too many indexes in {coll.namespace} ({coll.nindexes} indexes)")
                        health_report['recommendations'].append(f"Review and remove unused/redundant indexes in {coll.namespace}")
            
            # Check operation counters for anomalies
            opcounters = server_status.get('opcounters', {})
            self.logger.debug(f"Opcounters structure: {opcounters}")
            
            # Handle both simple opcounters and nested opcountersRepl structures
            total_ops = 0
            if isinstance(opcounters, dict):
                for key, value in opcounters.items():
                    self.logger.debug(f"Opcounters key '{key}': {type(value)} = {value}")
                    if isinstance(value, (int, float)):
                        total_ops += value
                    elif isinstance(value, dict):
                        # Handle nested structures like opcountersRepl
                        nested_sum = sum(v for v in value.values() if isinstance(v, (int, float)))
                        total_ops += nested_sum
                        self.logger.debug(f"Nested opcounters sum for '{key}': {nested_sum}")
            
            self.logger.debug(f"Total operations calculated: {total_ops}")
            
            if total_ops > 0:
                # Check for high delete ratio (might indicate data churn)
                delete_ops = 0
                if isinstance(opcounters, dict):
                    # Try to get delete operations from both simple and nested structures
                    delete_ops = opcounters.get('delete', 0)
                    if isinstance(delete_ops, dict):
                        delete_ops = sum(v for v in delete_ops.values() if isinstance(v, (int, float)))
                    elif not isinstance(delete_ops, (int, float)):
                        delete_ops = 0
                
                delete_ratio = delete_ops / total_ops
                if delete_ratio > 0.1:
                    health_report['issues'].append("High delete operation ratio detected")
                    health_report['recommendations'].append("Review data lifecycle management")
            
            # Set overall health
            if health_report['issues']:
                health_report['overall_health'] = 'warning' if len(health_report['issues']) < 3 else 'critical'
            
            health_report['metrics'] = {
                'memory_usage_mb': mem.get('resident', 0),
                'current_connections': current,
                'total_operations': total_ops
            }
            
            return health_report
            
        except Exception as e:
            self.logger.error(f"Error analyzing cluster health: {e}")
            health_report['overall_health'] = 'error'
            health_report['issues'].append(f"Health check failed: {e}")
            return health_report
