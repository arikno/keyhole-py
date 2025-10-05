"""
HTML Report Generator for Keyhole Python
Generates beautiful HTML reports from MongoDB cluster statistics
"""

import os
import json
import logging
from datetime import datetime
from typing import Dict, Any, List
from jinja2 import Environment, FileSystemLoader, Template
from .cluster_stats import ClusterStats


class HTMLReportGenerator:
    """Generates HTML reports from cluster statistics"""
    
    def __init__(self, template_dir: str = "templates"):
        """
        Initialize HTML report generator
        
        Args:
            template_dir: Directory containing Jinja2 templates
        """
        self.template_dir = template_dir
        self.logger = logging.getLogger(__name__)
        
        # Setup Jinja2 environment
        self.env = Environment(
            loader=FileSystemLoader(template_dir),
            autoescape=True
        )
        
        # Add custom filters
        self.env.filters['filesizeformat'] = self._filesizeformat
        self.env.filters['floatformat'] = self._floatformat
        self.env.filters['date'] = self._date_format
        
    def generate_cluster_report(self, stats: ClusterStats, output_file: str = None) -> str:
        """
        Generate comprehensive cluster analysis report
        
        Args:
            stats: ClusterStats object containing all collected data
            output_file: Output file path (optional)
            
        Returns:
            Generated HTML content
        """
        self.logger.info("Generating cluster analysis report")
        
        # Prepare template data
        template_data = self._prepare_cluster_data(stats)
        
        # Render template
        template = self.env.get_template('cluster_report.html')
        html_content = template.render(**template_data)
        
        # Write to file if specified
        if output_file:
            self._write_html_file(html_content, output_file)
            self.logger.info(f"Report written to {output_file}")
        
        return html_content
    
    def _prepare_cluster_data(self, stats: ClusterStats) -> Dict[str, Any]:
        """Prepare data for cluster report template"""
        
        # Calculate derived metrics
        uptime_days = stats.server_status.uptime / (24 * 3600) if stats.server_status.uptime else 0
        total_connections = (stats.server_status.connections_current + 
                           stats.server_status.connections_available)
        memory_usage_gb = stats.server_status.mem_resident / (1024 * 1024) if stats.server_status.mem_resident else 0
        
        # Prepare operation counters
        opcounters = {
            'command': stats.server_status.opcounters_command,
            'insert': stats.server_status.opcounters_insert,
            'query': stats.server_status.opcounters_query,
            'update': stats.server_status.opcounters_update,
            'delete': stats.server_status.opcounters_delete,
            'getmore': stats.server_status.opcounters_getmore
        }
        
        # Prepare replica set data
        replica_set_members = []
        if stats.replica_set_status and 'members' in stats.replica_set_status:
            for member in stats.replica_set_status['members']:
                replica_set_members.append({
                    'name': member.get('name', ''),
                    'state': member.get('state', 0),
                    'stateStr': member.get('stateStr', ''),
                    'health': member.get('health', 0),
                    'uptime': member.get('uptime', 0),
                    'lastHeartbeat': member.get('lastHeartbeat', datetime.now())
                })
        
        # Prepare shards data
        shards_data = []
        if stats.shards:
            for shard in stats.shards:
                shards_data.append({
                    '_id': shard.get('_id', ''),
                    'host': shard.get('host', ''),
                    'state': shard.get('state', 0)
                })
        
        return {
            # Basic info
            'generation_time': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'cluster_type': stats.cluster_type,
            'cluster_summary': self._get_cluster_summary(stats),
            'host_name': stats.host,
            'mongodb_version': stats.version,
            
            # Performance metrics
            'ops_per_second': stats.total_operations_per_second,
            'memory_usage_gb': memory_usage_gb,
            'storage_size_gb': stats.total_storage_size_gb,
            'uptime_days': uptime_days,
            'total_connections': total_connections,
            
            # Operation counters
            'opcounters': opcounters,
            
            # Database and collection data
            'databases': [
                {
                    'name': db.name,
                    'size_on_disk': db.size_on_disk,
                    'empty': db.empty,
                    'data_size': db.data_size,
                    'storage_size': db.storage_size,
                    'index_size': db.index_size,
                    'objects': db.objects,
                    'indexes': db.indexes,
                    'avg_obj_size': db.avg_obj_size
                }
                for db in stats.databases
            ],
            
            'collections': [
                {
                    'name': coll.name,
                    'namespace': coll.namespace,
                    'count': coll.count,
                    'size': coll.size,
                    'storage_size': coll.storage_size,
                    'total_index_size': coll.total_index_size,
                    'avg_obj_size': coll.avg_obj_size,
                    'capped': coll.capped,
                    'sharded': coll.sharded,
                    'nindexes': coll.nindexes,
                    'indexes': self._format_indexes(coll.indexes),
                    'sample_docs': self._format_sample_docs(coll.sample_docs),
                    'detailed_indexes': self._format_detailed_indexes(getattr(coll, 'detailed_indexes', [])),
                    'structure_analysis': self._format_structure_analysis(getattr(coll, 'structure_analysis', None))
                }
                for coll in stats.collections
            ],
            
            # Topology data
            'replica_set_status': {
                'members': replica_set_members
            } if replica_set_members else None,
            
            'shards': shards_data if shards_data else None,
            'sharding_status': self._format_sharding_status(stats.sharding_status),
            
            # System information
            'host_info': {
                'hostname': stats.host_info.hostname,
                'os_name': stats.host_info.os_name,
                'os_type': stats.host_info.os_type,
                'cpu_arch': stats.host_info.cpu_arch,
                'num_cores': stats.host_info.num_cores,
                'mem_size_mb': stats.host_info.mem_size_mb,
                'mem_limit_mb': stats.host_info.mem_limit_mb
            },
            'storage_engine': stats.server_status.storage_engine,
            'process_type': stats.server_status.process,
            
            # Enhanced analysis data
            'detailed_performance_metrics': self._format_performance_metrics(getattr(stats, 'detailed_performance_metrics', None)),
            'cluster_health': getattr(stats, 'cluster_health', {})
        }
    
    def _get_cluster_summary(self, stats: ClusterStats) -> str:
        """Generate cluster summary string"""
        try:
            shard_info = ""
            if stats.cluster_type == "sharded" and stats.shards:
                shard_info = f" ({len(stats.shards)} shards)"
            
            return (f"MongoDB v{stats.version} {stats.host_info.hostname} "
                   f"({stats.host_info.os_name}) {stats.server_status.process} "
                   f"{stats.cluster_type}{shard_info} {stats.host_info.num_cores} cores "
                   f"{stats.host_info.mem_size_mb}MB memory")
        except Exception as e:
            self.logger.error(f"Error generating cluster summary: {e}")
            return f"MongoDB cluster on {stats.host}"
    
    def _format_indexes(self, indexes: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Format indexes for template display"""
        formatted_indexes = []
        for index in indexes:
            formatted_indexes.append({
                'name': index.get('name', ''),
                'key': json.dumps(index.get('key', {}), indent=2),
                'unique': index.get('unique', False),
                'background': index.get('background', False),
                'sparse': index.get('sparse', False)
            })
        return formatted_indexes
    
    def _format_sample_docs(self, sample_docs: List[Dict[str, Any]]) -> List[str]:
        """Format sample documents for template display"""
        formatted_docs = []
        for doc in sample_docs:
            # Limit document size for display
            doc_str = json.dumps(doc, indent=2, default=str)
            if len(doc_str) > 1000:
                doc_str = doc_str[:1000] + "... (truncated)"
            formatted_docs.append(doc_str)
        return formatted_docs
    
    def _format_sharding_status(self, sharding_status: Dict[str, Any]) -> str:
        """Format sharding status for display"""
        if not sharding_status:
            return ""
        return json.dumps(sharding_status, indent=2, default=str)
    
    def _format_detailed_indexes(self, detailed_indexes) -> List[Dict[str, Any]]:
        """Format detailed indexes for template display"""
        if not detailed_indexes:
            return []
        
        formatted_indexes = []
        for index in detailed_indexes:
            formatted_indexes.append({
                'name': index.name,
                'key_string': index.key_string,
                'fields': index.fields,
                'effective_key': index.effective_key,
                'unique': index.unique,
                'sparse': index.sparse,
                'background': index.background,
                'expire_after_seconds': index.expire_after_seconds,
                'is_shard_key': index.is_shard_key,
                'is_duplicate': index.is_duplicate,
                'total_ops': index.total_ops,
                'recommendation': index.recommendation,
                'issues': index.issues,
                'usage_stats': [
                    {
                        'total_ops': usage.total_ops,
                        'since': usage.since.isoformat() if usage.since else None,
                        'host': usage.host,
                        'shard': usage.shard
                    }
                    for usage in index.usage_stats
                ]
            })
        return formatted_indexes
    
    def _format_structure_analysis(self, structure_analysis) -> Dict[str, Any]:
        """Format structure analysis for template display"""
        if not structure_analysis:
            return {}
        
        return {
            'max_nesting_depth': structure_analysis.max_nesting_depth,
            'avg_nesting_depth': structure_analysis.avg_nesting_depth,
            'has_deep_nesting': structure_analysis.has_deep_nesting,
            'has_large_arrays': structure_analysis.has_large_arrays,
            'max_array_size': structure_analysis.max_array_size,
            'avg_array_size': structure_analysis.avg_array_size,
            'total_fields': structure_analysis.total_fields,
            'total_queryable_paths': getattr(structure_analysis, 'total_queryable_paths', 0),
            'fragmentation_percentage': getattr(structure_analysis, 'fragmentation_percentage', 0),
            'storage_efficiency': getattr(structure_analysis, 'storage_efficiency', 100),
            'wasted_space_bytes': getattr(structure_analysis, 'wasted_space_bytes', 0),
            'fragmentation_level': getattr(structure_analysis, 'fragmentation_level', 'unknown'),
            'common_fields': structure_analysis.common_fields,
            'field_types': structure_analysis.field_types,
            'recommendations': structure_analysis.recommendations,
            'issues': structure_analysis.issues,
            'deep_nesting_samples': [
                json.dumps(sample, indent=2, default=str)
                for sample in structure_analysis.deep_nesting_samples[:2]
            ],
            'large_array_samples': [
                json.dumps(sample, indent=2, default=str)
                for sample in structure_analysis.large_array_samples[:2]
            ]
        }
    
    def _format_performance_metrics(self, performance_metrics) -> Dict[str, Any]:
        """Format performance metrics for template display"""
        if not performance_metrics:
            return {}
        
        return {
            'ops_per_second': performance_metrics.ops_per_second,
            'reads_per_second': performance_metrics.reads_per_second,
            'writes_per_second': performance_metrics.writes_per_second,
            'index_hit_ratio': performance_metrics.index_hit_ratio,
            'unused_indexes': performance_metrics.unused_indexes,
            'overused_indexes': performance_metrics.overused_indexes,
            'working_set_size': performance_metrics.working_set_size,
            'cache_hit_ratio': performance_metrics.cache_hit_ratio,
            'current_connections': performance_metrics.current_connections,
            'available_connections': performance_metrics.available_connections,
            'connection_pool_utilization': performance_metrics.connection_pool_utilization
        }
    
    def _write_html_file(self, html_content: str, output_file: str):
        """Write HTML content to file"""
        try:
            # Create output directory if it doesn't exist
            output_dir = os.path.dirname(output_file)
            if output_dir and not os.path.exists(output_dir):
                os.makedirs(output_dir)
            
            # Write HTML file
            with open(output_file, 'w', encoding='utf-8') as f:
                f.write(html_content)
                
        except Exception as e:
            self.logger.error(f"Error writing HTML file {output_file}: {e}")
            raise
    
    def _filesizeformat(self, value: int) -> str:
        """Format bytes as human readable file size"""
        if value is None:
            return "0 B"
        
        try:
            value = int(value)
            if value == 0:
                return "0 B"
            
            size_names = ["B", "KB", "MB", "GB", "TB", "PB"]
            import math
            i = int(math.floor(math.log(value, 1024)))
            p = math.pow(1024, i)
            s = round(value / p, 2)
            return f"{s} {size_names[i]}"
        except (ValueError, TypeError):
            return "0 B"
    
    def _floatformat(self, value: float, decimals: int = 1) -> str:
        """Format float with specified decimal places"""
        if value is None:
            return "0"
        
        try:
            return f"{float(value):.{decimals}f}"
        except (ValueError, TypeError):
            return "0"
    
    def _date_format(self, value, format_string: str = "%Y-%m-%d %H:%M:%S") -> str:
        """Format datetime object"""
        if value is None:
            return ""
        
        try:
            if isinstance(value, datetime):
                return value.strftime(format_string)
            elif isinstance(value, str):
                # Try to parse and format
                dt = datetime.fromisoformat(value.replace('Z', '+00:00'))
                return dt.strftime(format_string)
            else:
                return str(value)
        except Exception:
            return str(value) if value else ""


class IndexReportGenerator(HTMLReportGenerator):
    """Generates index analysis reports"""
    
    def generate_index_report(self, index_data: Dict[str, Any], output_file: str = None) -> str:
        """
        Generate index analysis report
        
        Args:
            index_data: Index analysis data
            output_file: Output file path (optional)
            
        Returns:
            Generated HTML content
        """
        self.logger.info("Generating index analysis report")
        
        # Prepare template data
        template_data = {
            'generation_time': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'host_name': index_data.get('host', 'Unknown'),
            'mongodb_version': index_data.get('version', 'Unknown'),
            **index_data
        }
        
        # Render template (would need index_report.html template)
        template = self.env.get_template('index_report.html')
        html_content = template.render(**template_data)
        
        # Write to file if specified
        if output_file:
            self._write_html_file(html_content, output_file)
            self.logger.info(f"Index report written to {output_file}")
        
        return html_content


class CardinalityReportGenerator(HTMLReportGenerator):
    """Generates cardinality analysis reports"""
    
    def generate_cardinality_report(self, cardinality_data: Dict[str, Any], output_file: str = None) -> str:
        """
        Generate cardinality analysis report
        
        Args:
            cardinality_data: Cardinality analysis data
            output_file: Output file path (optional)
            
        Returns:
            Generated HTML content
        """
        self.logger.info("Generating cardinality analysis report")
        
        # Prepare template data
        template_data = {
            'generation_time': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'host_name': cardinality_data.get('host', 'Unknown'),
            'mongodb_version': cardinality_data.get('version', 'Unknown'),
            **cardinality_data
        }
        
        # Render template (would need cardinality_report.html template)
        template = self.env.get_template('cardinality_report.html')
        html_content = template.render(**template_data)
        
        # Write to file if specified
        if output_file:
            self._write_html_file(html_content, output_file)
            self.logger.info(f"Cardinality report written to {output_file}")
        
        return html_content
