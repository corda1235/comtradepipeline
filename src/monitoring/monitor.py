# File: src/monitoring/monitor.py

# -*- coding: utf-8 -*-

"""
Monitoring module for the Comtrade Data Pipeline.
Provides functionality for tracking pipeline statistics and sending alerts.
"""

import os
import smtplib
import json
from datetime import datetime
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from typing import Dict, Any, List, Optional

from loguru import logger


class PipelineMonitor:
    """Monitor for tracking pipeline statistics and sending alerts."""

    def __init__(self, config: Dict[str, Any]):
        """
        Initialize the pipeline monitor.
        
        Args:
            config: Configuration dictionary.
        """
        self.config = config
        self.stats_dir = os.path.join(config.get('logging', {}).get('log_dir', 'logs'), 'stats')
        self.alert_config = config.get('monitoring', {}).get('alerts', {})
        
        # Create stats directory if it doesn't exist
        os.makedirs(self.stats_dir, exist_ok=True)
        
        logger.info("PipelineMonitor initialized")
    
    def save_execution_stats(
        self, 
        stats: Dict[str, Any], 
        countries: List[str], 
        start_date: str, 
        end_date: str, 
        execution_time: float,
        success: bool
    ) -> None:
        """
        Save pipeline execution statistics to a JSON file.
        
        Args:
            stats: Pipeline statistics dictionary.
            countries: List of countries processed.
            start_date: Start date of the data.
            end_date: End date of the data.
            execution_time: Total execution time in seconds.
            success: Whether the execution was successful.
        """
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        
        # Build stats object
        execution_stats = {
            'timestamp': datetime.now().isoformat(),
            'countries': countries,
            'date_range': {
                'start': start_date,
                'end': end_date
            },
            'execution_time_seconds': execution_time,
            'success': success,
            'statistics': stats
        }
        
        # Save to file
        filename = f"execution_{timestamp}.json"
        file_path = os.path.join(self.stats_dir, filename)
        
        with open(file_path, 'w') as f:
            json.dump(execution_stats, f, indent=2)
        
        logger.info(f"Execution statistics saved to {file_path}")
        
        # Check if we need to send an alert
        if not success and self.alert_config.get('enabled', False):
            self._send_failure_alert(execution_stats)
    
    def generate_daily_report(self) -> Optional[str]:
        """
        Generate a daily report from execution statistics.
        
        Returns:
            str: Path to the generated report file, or None if no data available.
        """
        today = datetime.now().strftime('%Y%m%d')
        report_file = os.path.join(self.stats_dir, f"daily_report_{today}.txt")
        
        # Get all execution files for today
        stats_files = [
            f for f in os.listdir(self.stats_dir) 
            if f.startswith('execution_') and f.endswith('.json') and today in f
        ]
        
        if not stats_files:
            logger.warning("No execution statistics found for today. No report generated.")
            return None
        
        # Aggregate statistics
        total_executions = 0
        successful_executions = 0
        total_api_calls = 0
        total_cache_hits = 0
        total_records_processed = 0
        total_records_stored = 0
        total_time = 0
        countries_processed = set()
        
        for stats_file in stats_files:
            file_path = os.path.join(self.stats_dir, stats_file)
            
            try:
                with open(file_path, 'r') as f:
                    data = json.load(f)
                
                total_executions += 1
                successful_executions += 1 if data.get('success', False) else 0
                total_time += data.get('execution_time_seconds', 0)
                
                statistics = data.get('statistics', {})
                total_api_calls += statistics.get('api_calls', 0)
                total_cache_hits += statistics.get('cache_hits', 0)
                total_records_processed += statistics.get('processed_records', 0)
                total_records_stored += statistics.get('stored_records', 0)
                
                countries_processed.update(data.get('countries', []))
                
            except (json.JSONDecodeError, FileNotFoundError, KeyError) as e:
                logger.error(f"Error processing stats file {stats_file}: {str(e)}")
        
        # Generate report content
        report_content = [
            f"Comtrade Data Pipeline - Daily Report {today}",
            "=" * 50,
            f"Total Executions: {total_executions}",
            f"Successful Executions: {successful_executions}",
            f"Failed Executions: {total_executions - successful_executions}",
            f"Success Rate: {(successful_executions / total_executions * 100) if total_executions > 0 else 0:.2f}%",
            f"Total Execution Time: {total_time:.2f} seconds",
            f"Total API Calls: {total_api_calls}",
            f"Total Cache Hits: {total_cache_hits}",
            f"Total Records Processed: {total_records_processed}",
            f"Total Records Stored: {total_records_stored}",
            f"Countries Processed: {', '.join(sorted(countries_processed))}"
        ]
        
        # Write report to file
        with open(report_file, 'w') as f:
            f.write('\n'.join(report_content))
        
        logger.info(f"Daily report generated: {report_file}")
        
        # Send report if configured
        if self.alert_config.get('daily_report', False):
            self._send_daily_report('\n'.join(report_content))
        
        return report_file
    
    def _send_failure_alert(self, stats: Dict[str, Any]) -> bool:
        """
        Send an alert for pipeline execution failure.
        
        Args:
            stats: Execution statistics.
            
        Returns:
            bool: True if alert sent successfully, False otherwise.
        """
        if not self.alert_config.get('smtp', {}).get('enabled', False):
            logger.warning("SMTP alerts not configured. No alert sent.")
            return False
        
        try:
            smtp_config = self.alert_config.get('smtp', {})
            
            # Create message
            msg = MIMEMultipart()
            msg['From'] = smtp_config.get('from', 'comtrade@pipeline.alert')
            msg['To'] = smtp_config.get('to', 'admin@example.com')
            msg['Subject'] = f"ALERT: Comtrade Pipeline Execution Failed"
            
            # Message body
            countries = ', '.join(stats.get('countries', ['unknown']))
            date_range = f"{stats.get('date_range', {}).get('start', 'unknown')} to {stats.get('date_range', {}).get('end', 'unknown')}"
            
            body = f"""
            Comtrade Data Pipeline execution has failed.
            
            Timestamp: {stats.get('timestamp', 'unknown')}
            Countries: {countries}
            Date Range: {date_range}
            Execution Time: {stats.get('execution_time_seconds', 0):.2f} seconds
            
            Statistics:
            - API Calls: {stats.get('statistics', {}).get('api_calls', 0)}
            - Cache Hits: {stats.get('statistics', {}).get('cache_hits', 0)}
            - Failed Calls: {stats.get('statistics', {}).get('failed_calls', 0)}
            - Processed Records: {stats.get('statistics', {}).get('processed_records', 0)}
            - Stored Records: {stats.get('statistics', {}).get('stored_records', 0)}
            
            Please check the logs for more details.
            """
            
            msg.attach(MIMEText(body, 'plain'))
            
            # Send message
            server = smtplib.SMTP(
                smtp_config.get('server', 'localhost'),
                smtp_config.get('port', 25)
            )
            
            if smtp_config.get('use_tls', False):
                server.starttls()
            
            if smtp_config.get('username') and smtp_config.get('password'):
                server.login(smtp_config.get('username'), smtp_config.get('password'))
            
            server.send_message(msg)
            server.quit()
            
            logger.info("Failure alert email sent successfully")
            return True
            
        except Exception as e:
            logger.error(f"Failed to send alert email: {str(e)}")
            return False
    
    def _send_daily_report(self, report_content: str) -> bool:
        """
        Send daily report via email.
        
        Args:
            report_content: Report content.
            
        Returns:
            bool: True if report sent successfully, False otherwise.
        """
        if not self.alert_config.get('smtp', {}).get('enabled', False):
            logger.warning("SMTP alerts not configured. No report sent.")
            return False
        
        try:
            smtp_config = self.alert_config.get('smtp', {})
            today = datetime.now().strftime('%Y-%m-%d')
            
            # Create message
            msg = MIMEMultipart()
            msg['From'] = smtp_config.get('from', 'comtrade@pipeline.report')
            msg['To'] = smtp_config.get('to', 'admin@example.com')
            msg['Subject'] = f"Comtrade Pipeline Daily Report - {today}"
            
            # Message body
            msg.attach(MIMEText(report_content, 'plain'))
            
            # Send message
            server = smtplib.SMTP(
                smtp_config.get('server', 'localhost'),
                smtp_config.get('port', 25)
            )
            
            if smtp_config.get('use_tls', False):
                server.starttls()
            
            if smtp_config.get('username') and smtp_config.get('password'):
                server.login(smtp_config.get('username'), smtp_config.get('password'))
            
            server.send_message(msg)
            server.quit()
            
            logger.info("Daily report email sent successfully")
            return True
            
        except Exception as e:
            logger.error(f"Failed to send daily report email: {str(e)}")
            return False