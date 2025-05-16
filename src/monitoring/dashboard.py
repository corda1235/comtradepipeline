# File: src/monitoring/dashboard.py

# -*- coding: utf-8 -*-

"""
Monitoring dashboard for the Comtrade Data Pipeline.
Provides a simple web dashboard to monitor the pipeline status.
"""

import os
import json
import glob
from datetime import datetime, timedelta
from typing import Dict, Any, List, Optional
from flask import Flask, render_template, jsonify

from src.utils.config_loader import load_config
from src.utils.logging_utils import setup_logger, get_module_logger

# Module logger
logger = get_module_logger("dashboard")

# Flask app
app = Flask(__name__)

# Configuration
config = load_config()

# Setup logger
setup_logger(config)


def get_stats_files(days: int = 7) -> List[str]:
    """
    Get statistics files from the last N days.
    
    Args:
        days: Number of days to look back.
        
    Returns:
        list: List of file paths.
    """
    stats_dir = os.path.join(config.get('logging', {}).get('log_dir', 'logs'), 'stats')
    if not os.path.exists(stats_dir):
        return []
    
    # Get all execution files
    files = glob.glob(os.path.join(stats_dir, 'execution_*.json'))
    
    # Filter by date
    cutoff_date = datetime.now() - timedelta(days=days)
    
    return [
        f for f in files 
        if os.path.exists(f) and datetime.fromtimestamp(os.path.getmtime(f)) >= cutoff_date
    ]


def load_stats_data(files: List[str]) -> List[Dict[str, Any]]:
    """
    Load statistics data from files.
    
    Args:
        files: List of file paths.
        
    Returns:
        list: List of statistics data.
    """
    stats_data = []
    
    for file_path in files:
        try:
            with open(file_path, 'r') as f:
                data = json.load(f)
                stats_data.append(data)
        except (json.JSONDecodeError, IOError) as e:
            logger.error(f"Error reading stats file {file_path}: {str(e)}")
    
    # Sort by timestamp
    return sorted(stats_data, key=lambda x: x.get('timestamp', ''), reverse=True)


def aggregate_stats(stats_data: List[Dict[str, Any]]) -> Dict[str, Any]:
    """
    Aggregate statistics data.
    
    Args:
        stats_data: List of statistics data.
        
    Returns:
        dict: Aggregated statistics.
    """
    if not stats_data:
        return {
            'total_executions': 0,
            'successful_executions': 0,
            'failed_executions': 0,
            'success_rate': 0,
            'total_api_calls': 0,
            'total_cache_hits': 0,
            'total_api_calls_ratio': 0,
            'total_records_processed': 0,
            'total_records_stored': 0,
            'total_execution_time': 0,
            'avg_execution_time': 0,
            'countries_processed': set()
        }
    
    # Initialize aggregation
    aggregated = {
        'total_executions': len(stats_data),
        'successful_executions': sum(1 for s in stats_data if s.get('success', False)),
        'failed_executions': sum(1 for s in stats_data if not s.get('success', False)),
        'total_api_calls': sum(s.get('statistics', {}).get('api_calls', 0) for s in stats_data),
        'total_cache_hits': sum(s.get('statistics', {}).get('cache_hits', 0) for s in stats_data),
        'total_records_processed': sum(s.get('statistics', {}).get('processed_records', 0) for s in stats_data),
        'total_records_stored': sum(s.get('statistics', {}).get('stored_records', 0) for s in stats_data),
        'total_execution_time': sum(s.get('execution_time_seconds', 0) for s in stats_data),
        'countries_processed': set()
    }
    
    # Aggregate countries
    for stats in stats_data:
        if 'countries' in stats:
            aggregated['countries_processed'].update(stats['countries'])
    
    # Calculate derived metrics
    if aggregated['total_executions'] > 0:
        aggregated['success_rate'] = (aggregated['successful_executions'] / aggregated['total_executions']) * 100
        aggregated['avg_execution_time'] = aggregated['total_execution_time'] / aggregated['total_executions']
    else:
        aggregated['success_rate'] = 0
        aggregated['avg_execution_time'] = 0
    
    total_calls = aggregated['total_api_calls'] + aggregated['total_cache_hits']
    if total_calls > 0:
        aggregated['total_api_calls_ratio'] = (aggregated['total_api_calls'] / total_calls) * 100
    else:
        aggregated['total_api_calls_ratio'] = 0
    
    # Convert countries set to list for JSON serialization
    aggregated['countries_processed'] = sorted(list(aggregated['countries_processed']))
    
    return aggregated


@app.route('/')
def index():
    """Render the dashboard index page."""
    # Get stats data
    stats_files = get_stats_files(days=7)
    stats_data = load_stats_data(stats_files)
    aggregated_stats = aggregate_stats(stats_data)
    
    # Return HTML template
    return render_template(
        'dashboard.html',
        stats=aggregated_stats,
        recent_executions=stats_data[:10],
        current_time=datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    )


@app.route('/api/stats')
def api_stats():
    """Return statistics data as JSON."""
    stats_files = get_stats_files(days=7)
    stats_data = load_stats_data(stats_files)
    aggregated_stats = aggregate_stats(stats_data)
    
    return jsonify({
        'aggregated': aggregated_stats,
        'recent_executions': stats_data[:10]
    })


def create_app():
    """Create the Flask app."""
    return app


if __name__ == '__main__':
    # Create templates directory if it doesn't exist
    templates_dir = os.path.join(os.path.dirname(__file__), 'templates')
    os.makedirs(templates_dir, exist_ok=True)
    
    # Create dashboard.html template if it doesn't exist
    template_path = os.path.join(templates_dir, 'dashboard.html')
    if not os.path.exists(template_path):
        with open(template_path, 'w') as f:
            f.write("""
<!DOCTYPE html>
<html>
<head>
    <title>Comtrade Pipeline Dashboard</title>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/bootstrap@5.2.3/dist/css/bootstrap.min.css">
    <style>
        .card { margin-bottom: 20px; }
        .stats-value { font-size: 24px; font-weight: bold; }
    </style>
</head>
<body>
    <div class="container mt-4">
        <h1>Comtrade Data Pipeline Dashboard</h1>
        <p class="text-muted">Last updated: {{ current_time }}</p>
        
        <div class="row mt-4">
            <div class="col-md-3">
                <div class="card">
                    <div class="card-body text-center">
                        <h5 class="card-title">Executions</h5>
                        <p class="stats-value">{{ stats.total_executions }}</p>
                        <p class="text-muted">Success rate: {{ "%.1f"|format(stats.success_rate) }}%</p>
                    </div>
                </div>
            </div>
            
            <div class="col-md-3">
                <div class="card">
                    <div class="card-body text-center">
                        <h5 class="card-title">API Calls</h5>
                        <p class="stats-value">{{ stats.total_api_calls }}</p>
                        <p class="text-muted">Cache hits: {{ stats.total_cache_hits }}</p>
                    </div>
                </div>
            </div>
            
            <div class="col-md-3">
                <div class="card">
                    <div class="card-body text-center">
                        <h5 class="card-title">Records Processed</h5>
                        <p class="stats-value">{{ stats.total_records_processed }}</p>
                        <p class="text-muted">Stored: {{ stats.total_records_stored }}</p>
                    </div>
                </div>
            </div>
            
            <div class="col-md-3">
                <div class="card">
                    <div class="card-body text-center">
                        <h5 class="card-title">Avg. Execution Time</h5>
                        <p class="stats-value">{{ "%.1f"|format(stats.avg_execution_time) }}s</p>
                        <p class="text-muted">Total: {{ "%.1f"|format(stats.total_execution_time) }}s</p>
                    </div>
                </div>
            </div>
        </div>
        
        <div class="row mt-4">
            <div class="col-md-12">
                <div class="card">
                    <div class="card-header">
                        <h5>Countries Processed</h5>
                    </div>
                    <div class="card-body">
                        <p>
                            {% for country in stats.countries_processed %}
                                <span class="badge bg-primary me-1">{{ country }}</span>
                            {% endfor %}
                        </p>
                    </div>
                </div>
            </div>
        </div>
        
        <div class="row mt-4">
            <div class="col-md-12">
                <div class="card">
                    <div class="card-header">
                        <h5>Recent Executions</h5>
                    </div>
                    <div class="card-body">
                        <table class="table table-striped">
                            <thead>
                                <tr>
                                    <th>Timestamp</th>
                                    <th>Countries</th>
                                    <th>Period</th>
                                    <th>Records</th>
                                    <th>API Calls</th>
                                    <th>Duration</th>
                                    <th>Status</th>
                                </tr>
                            </thead>
                            <tbody>
                                {% for execution in recent_executions %}
                                <tr>
                                    <td>{{ execution.timestamp }}</td>
                                    <td>
                                        {% if execution.countries|length <= 3 %}
                                            {{ ", ".join(execution.countries) }}
                                        {% else %}
                                            {{ execution.countries|length }} countries
                                        {% endif %}
                                    </td>
                                    <td>
                                        {% if execution.date_range %}
                                            {{ execution.date_range.start }} to {{ execution.date_range.end }}
                                        {% else %}
                                            N/A
                                        {% endif %}
                                    </td>
                                    <td>
                                        {% if execution.statistics %}
                                            {{ execution.statistics.processed_records }}
                                        {% else %}
                                            0
                                        {% endif %}
                                    </td>
                                    <td>
                                        {% if execution.statistics %}
                                            {{ execution.statistics.api_calls }}
                                            {% if execution.statistics.cache_hits > 0 %}
                                                (+{{ execution.statistics.cache_hits }} cache)
                                            {% endif %}
                                        {% else %}
                                            0
                                        {% endif %}
                                    </td>
                                    <td>{{ "%.1f"|format(execution.execution_time_seconds) }}s</td>
                                    <td>
                                        {% if execution.success %}
                                            <span class="badge bg-success">Success</span>
                                        {% else %}
                                            <span class="badge bg-danger">Failed</span>
                                        {% endif %}
                                    </td>
                                </tr>
                                {% endfor %}
                            </tbody>
                        </table>
                    </div>
                </div>
            </div>
        </div>
    </div>
    
    <script src="https://cdn.jsdelivr.net/npm/bootstrap@5.2.3/dist/js/bootstrap.bundle.min.js"></script>
</body>
</html>
            """)
    
    # Run the app
    app.run(host='0.0.0.0', port=5000, debug=True)
