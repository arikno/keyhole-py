#!/usr/bin/env python3
"""
CSV Export functionality for Keyhole Python
Exports consolidated data from all collections to CSV files
"""

import csv
import os
from datetime import datetime
from typing import List, Dict, Any
from dataclasses import dataclass

from .cluster_stats import ClusterStats, CollectionStats, DetailedIndex
from .enhanced_analysis import CollectionStructureAnalysis


@dataclass
class ConsolidatedIndex:
    """Consolidated index information for CSV export"""
    database: str
    collection: str
    namespace: str
    index_name: str
    key_string: str
    fields: str
    unique: bool
    sparse: bool
    background: bool
    ttl: int
    is_shard_key: bool
    is_duplicate: bool
    total_ops: int
    issues: str
    recommendation: str


@dataclass
class ConsolidatedCollection:
    """Consolidated collection information for CSV export"""
    database: str
    collection: str
    namespace: str
    document_count: int
    total_size_bytes: int
    storage_size_bytes: int
    index_size_bytes: int
    avg_document_size: float
    
    # Structure analysis - simplified to match HTML template
    max_nesting_depth: int
    max_array_size: int
    
    # Fragmentation analysis
    fragmentation_percentage: float
    storage_efficiency: float
    fragmentation_level: str
    
    # Index summary
    total_indexes: int
    unused_indexes: int
    redundant_indexes: int
    ttl_indexes: int


class CSVExporter:
    """Exports consolidated cluster data to CSV files"""
    
    def __init__(self, output_dir: str = "."):
        """
        Initialize CSV exporter
        
        Args:
            output_dir: Directory to save CSV files
        """
        self.output_dir = output_dir
    
    def export_cluster_data(self, stats: ClusterStats, base_filename: str = None) -> Dict[str, str]:
        """
        Export cluster data to CSV files
        
        Args:
            stats: Cluster statistics object
            base_filename: Base filename for output files (without extension)
            
        Returns:
            Dictionary with file paths of generated CSV files
        """
        if not base_filename:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            base_filename = f"keyhole_analysis_{timestamp}"
        
        # Export indexes
        indexes_file = os.path.join(self.output_dir, f"{base_filename}_indexes.csv")
        self._export_indexes_csv(stats, indexes_file)
        
        # Export collections
        collections_file = os.path.join(self.output_dir, f"{base_filename}_collections.csv")
        self._export_collections_csv(stats, collections_file)
        
        return {
            'indexes': indexes_file,
            'collections': collections_file
        }
    
    def _export_indexes_csv(self, stats: ClusterStats, filename: str):
        """Export all indexes to CSV file"""
        consolidated_indexes = []
        
        for collection in stats.collections:
            db_name, coll_name = collection.namespace.split('.', 1)
            
            # Process detailed indexes if available
            if hasattr(collection, 'detailed_indexes') and collection.detailed_indexes:
                for index in collection.detailed_indexes:
                    consolidated_index = ConsolidatedIndex(
                        database=db_name,
                        collection=coll_name,
                        namespace=collection.namespace,
                        index_name=index.name,
                        key_string=index.key_string,
                        fields=', '.join(index.fields),
                        unique=index.unique,
                        sparse=index.sparse,
                        background=index.background,
                        ttl=index.expire_after_seconds if index.expire_after_seconds > 0 else 0,
                        is_shard_key=index.is_shard_key,
                        is_duplicate=index.is_duplicate,
                        total_ops=index.total_ops,
                        issues='; '.join(index.issues) if index.issues else '',
                        recommendation=index.recommendation or ''
                    )
                    consolidated_indexes.append(consolidated_index)
            
            # Process basic indexes if detailed indexes not available
            elif hasattr(collection, 'indexes') and collection.indexes:
                for index in collection.indexes:
                    consolidated_index = ConsolidatedIndex(
                        database=db_name,
                        collection=coll_name,
                        namespace=collection.namespace,
                        index_name=index.get('name', ''),
                        key_string=str(index.get('key', {})),
                        fields=', '.join(index.get('key', {}).keys()),
                        unique=index.get('unique', False),
                        sparse=index.get('sparse', False),
                        background=index.get('background', False),
                        ttl=index.get('expireAfterSeconds', 0) if index.get('expireAfterSeconds', 0) > 0 else 0,
                        is_shard_key=False,  # Not available in basic index info
                        is_duplicate=False,  # Not available in basic index info
                        total_ops=0,  # Not available in basic index info
                        issues='',
                        recommendation=''
                    )
                    consolidated_indexes.append(consolidated_index)
        
        # Write to CSV
        with open(filename, 'w', newline='', encoding='utf-8') as csvfile:
            fieldnames = [
                'database', 'collection', 'namespace', 'index_name', 'key_string', 'fields',
                'unique', 'sparse', 'background', 'ttl', 'is_shard_key', 'is_duplicate',
                'total_ops', 'issues', 'recommendation'
            ]
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            
            for index in consolidated_indexes:
                writer.writerow({
                    'database': index.database,
                    'collection': index.collection,
                    'namespace': index.namespace,
                    'index_name': index.index_name,
                    'key_string': index.key_string,
                    'fields': index.fields,
                    'unique': index.unique,
                    'sparse': index.sparse,
                    'background': index.background,
                    'ttl': index.ttl,
                    'is_shard_key': index.is_shard_key,
                    'is_duplicate': index.is_duplicate,
                    'total_ops': index.total_ops,
                    'issues': index.issues,
                    'recommendation': index.recommendation
                })
    
    def _export_collections_csv(self, stats: ClusterStats, filename: str):
        """Export all collection metrics to CSV file"""
        consolidated_collections = []
        
        for collection in stats.collections:
            db_name, coll_name = collection.namespace.split('.', 1)
            
            # Get structure analysis data
            structure_analysis = getattr(collection, 'structure_analysis', None)
            
            # Count indexes by type
            detailed_indexes = getattr(collection, 'detailed_indexes', [])
            basic_indexes = getattr(collection, 'indexes', [])
            
            total_indexes = len(detailed_indexes) if detailed_indexes else len(basic_indexes)
            unused_indexes = 0
            redundant_indexes = 0
            ttl_indexes = 0
            
            if detailed_indexes:
                for index in detailed_indexes:
                    if index.total_ops == 0 and index.name != "_id_":
                        unused_indexes += 1
                    if index.is_duplicate:
                        redundant_indexes += 1
                    if index.expire_after_seconds > 0:
                        ttl_indexes += 1
            elif basic_indexes:
                for index in basic_indexes:
                    if index.get('expireAfterSeconds', 0) > 0:
                        ttl_indexes += 1
            
            
            consolidated_collection = ConsolidatedCollection(
                database=db_name,
                collection=coll_name,
                namespace=collection.namespace,
                document_count=collection.count,
                total_size_bytes=collection.size,
                storage_size_bytes=collection.storage_size,
                index_size_bytes=collection.total_index_size,
                avg_document_size=collection.avg_obj_size,
                
                # Structure analysis - simplified to match HTML template
                max_nesting_depth=structure_analysis.max_nesting_depth if structure_analysis else 0,
                max_array_size=structure_analysis.max_array_size if structure_analysis else 0,
                
                # Fragmentation analysis
                fragmentation_percentage=structure_analysis.fragmentation_percentage if structure_analysis else 0,
                storage_efficiency=structure_analysis.storage_efficiency if structure_analysis else 100,
                fragmentation_level=structure_analysis.fragmentation_level if structure_analysis else 'unknown',
                
                # Index summary
                total_indexes=total_indexes,
                unused_indexes=unused_indexes,
                redundant_indexes=redundant_indexes,
                ttl_indexes=ttl_indexes
            )
            
            consolidated_collections.append(consolidated_collection)
        
        # Write to CSV
        with open(filename, 'w', newline='', encoding='utf-8') as csvfile:
            fieldnames = [
                'database', 'collection', 'namespace', 'document_count', 'total_size_bytes',
                'storage_size_bytes', 'index_size_bytes', 'avg_document_size',
                'max_nesting_depth', 'max_array_size',
                'fragmentation_percentage', 'storage_efficiency', 'fragmentation_level',
                'total_indexes', 'unused_indexes', 'redundant_indexes', 'ttl_indexes'
            ]
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            
            for collection in consolidated_collections:
                writer.writerow({
                    'database': collection.database,
                    'collection': collection.collection,
                    'namespace': collection.namespace,
                    'document_count': collection.document_count,
                    'total_size_bytes': collection.total_size_bytes,
                    'storage_size_bytes': collection.storage_size_bytes,
                    'index_size_bytes': collection.index_size_bytes,
                    'avg_document_size': collection.avg_document_size,
                    'max_nesting_depth': collection.max_nesting_depth,
                    'max_array_size': collection.max_array_size,
                    'fragmentation_percentage': collection.fragmentation_percentage,
                    'storage_efficiency': collection.storage_efficiency,
                    'fragmentation_level': collection.fragmentation_level,
                    'total_indexes': collection.total_indexes,
                    'unused_indexes': collection.unused_indexes,
                    'redundant_indexes': collection.redundant_indexes,
                    'ttl_indexes': collection.ttl_indexes
                })
