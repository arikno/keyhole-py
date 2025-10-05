# Keyhole Python - MongoDB Cluster Analysis Tool

A Python implementation of the popular [Keyhole](https://github.com/simagix/keyhole) MongoDB analysis tool, designed to provide comprehensive cluster insights with beautiful HTML reports instead of BSON files.

## Features

### ✅ Implemented
- **Comprehensive Cluster Analysis** (`--allinfo`): Complete MongoDB cluster statistics
- **Single Collection Analysis** (`--collection`): Focused analysis of specific collections
- **Beautiful HTML Reports**: Responsive, interactive reports with Bootstrap styling
- **Database & Collection Analysis**: Detailed statistics for all databases and collections
- **Enhanced Index Analysis**: Redundant index detection, usage statistics, TTL indexes
- **Fragmentation Analysis**: Storage efficiency, wasted space, compression detection
- **Collection Structure Analysis**: Deep nesting detection, large array analysis
- **Performance Metrics**: Operations per second, memory usage, storage analysis
- **Cluster Topology Detection**: Automatic detection of standalone, replica set, and sharded clusters
- **Replica Set Monitoring**: Member status, health, and replication lag
- **Sharding Analysis**: Shard distribution and configuration
- **Sample Document Display**: Preview of collection documents
- **Index Information**: Detailed index statistics and configuration
- **Parallel Data Collection**: Fast, concurrent data gathering

### 🚧 Coming Soon
- **Index Analysis** (`--index`): Detailed index usage and optimization recommendations
- **Cardinality Analysis** (`--cardinality`): Field uniqueness and distribution analysis
- **Log Analysis** (`--loginfo`): MongoDB log parsing and performance analysis
- **Query Explanation** (`--explain`): Query execution plan analysis
- **Atlas Integration**: Direct MongoDB Atlas API integration
- **FTDC Data Analysis**: Full Time Diagnostic Capture data processing

## Installation

### Prerequisites
- Python 3.7 or higher
- MongoDB instance (local or remote)
- Required Python packages (see requirements.txt)

### Setup
```bash
# Clone or download the project
cd keyhole-python

# Install dependencies
pip install -r requirements.txt

# Make the script executable (optional)
chmod +x keyhole.py
```

## Usage

### Basic Usage

```bash
# Generate comprehensive cluster analysis
python keyhole.py --allinfo "mongodb://user:pass@host:port/db" --output cluster-report.html

# Analyze specific databases only
python keyhole.py --allinfo "mongodb://user:pass@host:port/db" --db database1 --db database2

# Analyze specific collection only (NEW!)
python keyhole.py --allinfo "mongodb://user:pass@host:port/db" --collection "database.collection_name" --output collection-report.html

# Fast mode (skip detailed collection analysis)
python keyhole.py --allinfo "mongodb://user:pass@host:port/db" --fast

# Verbose logging
python keyhole.py --allinfo "mongodb://user:pass@host:port/db" --verbose
```

### Single Collection Analysis

```bash
# Analyze a specific collection with full enhanced analysis
python keyhole.py --allinfo "mongodb://user:pass@host:port/db" --collection "database.collection_name" --output collection-report.html

# Fast analysis of a single collection (skip detailed structure analysis)
python keyhole.py --allinfo "mongodb://user:pass@host:port/db" --collection "database.collection_name" --fast --output fast-collection-report.html

# Analyze collection with verbose logging
python keyhole.py --allinfo "mongodb://user:pass@host:port/db" --collection "database.collection_name" --verbose --output verbose-collection-report.html

# Real-world examples
python keyhole.py --allinfo "mongodb+srv://user:pass@cluster.mongodb.net/" --collection "sample_airbnb.listingsAndReviews" --output airbnb-analysis.html
python keyhole.py --allinfo "mongodb://localhost:27017" --collection "mydb.users" --output users-analysis.html
```

**Benefits of Single Collection Analysis:**
- ⚡ **Faster execution** - Only analyzes the specified collection
- 🎯 **Focused reports** - Shows only relevant database and collection data
- 🔍 **Full enhanced analysis** - Includes redundant index detection, fragmentation analysis, and structure analysis
- 📊 **Detailed insights** - Index usage, document structure, performance metrics

### Connection Examples

```bash
# Local MongoDB
python keyhole.py --allinfo "mongodb://localhost:27017"

# MongoDB with authentication
python keyhole.py --allinfo "mongodb://username:password@host:27017/database"

# MongoDB Atlas
python keyhole.py --allinfo "mongodb+srv://username:password@cluster.mongodb.net/database"

# MongoDB with SSL
python keyhole.py --allinfo "mongodb://username:password@host:27017/database?ssl=true"
```

### Command Line Options

```
positional arguments:
  uri                   MongoDB connection string

optional arguments:
  -h, --help            show this help message and exit
  --allinfo URI         Database connection string for comprehensive analysis
  --index URI           Database connection string for index analysis
  --cardinality COLL    Collection name for cardinality analysis
  --db DB               Specific database(s) to analyze (can be used multiple times)
  --collection COLL     Specific collection to analyze (format: database.collection)
  --fast                Fast mode - skip detailed collection analysis
  -v, --verbose         Enable verbose logging
  -o OUTPUT, --output OUTPUT
                        Output HTML file path
  --no-open             Do not open the report in browser after generation
  --timeout TIMEOUT     Connection timeout in seconds (default: 30)
```

## Report Features

### 📊 Cluster Overview
- Cluster type detection (standalone, replica set, sharded)
- MongoDB version and build information
- System information (OS, CPU, memory)
- Uptime and connection statistics

### 📈 Performance Metrics
- Operations per second calculation
- Memory usage analysis
- Storage size breakdown
- Operation counter summaries

### 💾 Database Analysis
- Database size and document counts
- Index statistics and usage
- Collection-level analysis
- Storage engine information

### 🔍 Collection Details
- Document counts and sizes
- **Enhanced Index Analysis**: Redundant index detection, usage statistics, TTL indexes
- **Fragmentation Analysis**: Storage efficiency, wasted space, compression detection
- **Structure Analysis**: Deep nesting detection, large array analysis, field distribution
- Sample document previews
- Sharding information (for sharded collections)
- Performance recommendations and optimization suggestions

### ⚖️ Topology Information
- **Replica Sets**: Member status, health, replication lag
- **Sharded Clusters**: Shard distribution, configuration
- **Standalone**: Single server analysis

## Comparison with Go Keyhole

| Feature | Go Keyhole | Python Keyhole | Notes |
|---------|------------|----------------|-------|
| Cluster Analysis | ✅ BSON output | ✅ HTML output | Python version provides better visualization |
| Single Collection Analysis | ❌ | ✅ | Python version advantage |
| Enhanced Index Analysis | ✅ | ✅ | Redundant detection, usage stats, TTL analysis |
| Fragmentation Analysis | ✅ | ✅ | Storage efficiency, wasted space analysis |
| Collection Structure Analysis | ✅ | ✅ | Deep nesting, large array detection |
| Cardinality Analysis | ✅ | 🚧 Coming Soon | Planned for next release |
| Log Analysis | ✅ | 🚧 Coming Soon | Planned for next release |
| Atlas Integration | ✅ | 🚧 Coming Soon | Planned for next release |
| HTML Reports | ❌ | ✅ | Python version advantage |
| Interactive UI | ❌ | ✅ | Sortable tables, collapsible sections |
| Cross-platform | ✅ | ✅ | Both work on all platforms |

## Architecture

### Core Components

1. **MongoDBClient** (`src/mongodb_client.py`)
   - Handles MongoDB connections and basic operations
   - Provides methods for all MongoDB commands used by Keyhole
   - Error handling and connection management

2. **ClusterStatsCollector** (`src/cluster_stats.py`)
   - Collects comprehensive cluster statistics
   - Parallel data collection for performance
   - Data structures for organized statistics

3. **HTMLReportGenerator** (`src/report_generator.py`)
   - Generates beautiful HTML reports using Jinja2 templates
   - Bootstrap styling and responsive design
   - Custom filters for data formatting

4. **Templates** (`templates/`)
   - Jinja2 templates for HTML report generation
   - Bootstrap CSS framework
   - Interactive JavaScript components

### Data Flow

```
MongoDB Cluster → MongoDBClient → ClusterStatsCollector → HTMLReportGenerator → HTML Report
```

## Development

### Project Structure
```
keyhole-python/
├── keyhole.py              # Main CLI script
├── requirements.txt        # Python dependencies
├── README.md              # This file
├── src/                   # Source code
│   ├── __init__.py
│   ├── mongodb_client.py  # MongoDB connection and operations
│   ├── cluster_stats.py   # Statistics collection
│   └── report_generator.py # HTML report generation
├── templates/             # Jinja2 templates
│   ├── base.html         # Base template
│   └── cluster_report.html # Cluster analysis template
├── static/               # Static assets (future use)
│   ├── css/
│   └── js/
└── tests/                # Test files (future use)
```

### Adding New Features

1. **New Analysis Type**: Add new collector class in `src/`
2. **New Report Type**: Add new template in `templates/` and generator in `report_generator.py`
3. **New MongoDB Commands**: Extend `MongoDBClient` class

## Enhanced Analysis Features

### 🔍 Redundant Index Detection
Automatically identifies redundant indexes that can be safely removed:
- **Compound Index Prefixes**: Detects when a single-field index is redundant due to a compound index
- **Direction Matching**: Recognizes that `{field: -1}` and `{field: 1}` are equivalent for redundancy
- **Usage Statistics**: Shows which indexes are actually being used
- **Recommendations**: Provides specific guidance on which indexes to drop

**Example**: If you have both `property_type_1` and `property_type_1_room_type_1_beds_1`, the single-field index will be marked as redundant.

### 📊 Fragmentation Analysis
Comprehensive storage efficiency analysis:
- **Fragmentation Percentage**: Calculates wasted space as percentage of total storage
- **Storage Efficiency**: Shows what percentage of allocated space contains actual data
- **Compression Detection**: Handles cases where storage is compressed (storageSize < dataSize)
- **Fragmentation Levels**: Categorizes as low, medium, high, critical, or compressed
- **Recommendations**: Suggests compaction when fragmentation is high

**Calculation**: `Fragmentation = (Storage Size - Data Size) / Storage Size × 100%`

### 🏗️ Collection Structure Analysis
Deep analysis of document structure and patterns:
- **Nesting Depth**: Detects deeply nested documents that may impact performance
- **Array Analysis**: Identifies collections with large arrays that could benefit from restructuring
- **Field Distribution**: Analyzes field usage patterns and types
- **Queryable Paths**: Counts all possible query paths including array indices
- **Performance Impact**: Provides recommendations for schema optimization

### ⚡ Single Collection Analysis
Focused analysis for specific collections:
- **Faster Execution**: Only analyzes the specified collection (1.5s vs 3s+ for full cluster)
- **Focused Reports**: Shows only relevant database and collection data
- **Full Feature Set**: Includes all enhanced analysis features
- **Format**: `--collection "database.collection_name"`

## Contributing

Contributions are welcome! Please feel free to submit pull requests or open issues for:

- Bug fixes
- New features
- Performance improvements
- Documentation updates

## License

This project is licensed under the Apache-2.0 License - see the LICENSE file for details.

## Acknowledgments

- Original [Keyhole](https://github.com/simagix/keyhole) by Kuei-chun Chen for the inspiration and reference implementation
- MongoDB community for excellent documentation and tools
- Bootstrap and Chart.js for beautiful UI components

## Disclaimer

This software is not officially supported by MongoDB, Inc. Any usage is at your own risk.
