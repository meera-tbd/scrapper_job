#!/usr/bin/env python3
"""
Job Sync Scheduler
==================

Automated scheduler for job data synchronization that runs continuously
and syncs data at specified intervals.
"""

import time
import schedule
import threading
import logging
import json
import signal
import sys
from datetime import datetime, timedelta
from job_data_sync import JobDataSynchronizer

class JobSyncScheduler:
    """Scheduler for automated job synchronization."""
    
    def __init__(self, config_file=None):
        self.synchronizer = JobDataSynchronizer(config_file)
        self.running = False
        self.scheduler_thread = None
        self.setup_signal_handlers()
        
        # Load scheduler config
        self.scheduler_config = self.synchronizer.config.get('scheduler', {
            'incremental_interval_minutes': 60,
            'full_sync_interval_hours': 24,
            'max_failures': 3,
            'failure_backoff_minutes': 10
        })
        
        self.failure_count = 0
        self.last_sync_time = None
        self.last_full_sync_time = None
        
        logging.info("Job Sync Scheduler initialized")
    
    def setup_signal_handlers(self):
        """Setup signal handlers for graceful shutdown."""
        signal.signal(signal.SIGINT, self.signal_handler)
        signal.signal(signal.SIGTERM, self.signal_handler)
    
    def signal_handler(self, signum, frame):
        """Handle shutdown signals."""
        logging.info(f"Received signal {signum}, shutting down gracefully...")
        self.stop()
        sys.exit(0)
    
    def incremental_sync_job(self):
        """Run incremental sync job."""
        try:
            logging.info("Starting scheduled incremental sync")
            results = self.synchronizer.sync_jobs(incremental=True)
            
            if results['status'] == 'success':
                self.failure_count = 0
                self.last_sync_time = datetime.now()
                
                total_synced = results['total_synced']
                logging.info(f"Incremental sync completed: {total_synced} jobs synced")
                
                # Log portal stats
                for portal, stats in results['portals'].items():
                    logging.info(f"  {portal}: {stats['success']} success, {stats['failed']} failed")
            else:
                self.failure_count += 1
                logging.error(f"Incremental sync failed: {results['error']}")
                self.handle_sync_failure()
                
        except Exception as e:
            self.failure_count += 1
            logging.error(f"Exception during incremental sync: {e}")
            self.handle_sync_failure()
    
    def full_sync_job(self):
        """Run full sync job."""
        try:
            logging.info("Starting scheduled full sync")
            results = self.synchronizer.sync_jobs(incremental=False)
            
            if results['status'] == 'success':
                self.failure_count = 0
                self.last_full_sync_time = datetime.now()
                
                total_synced = results['total_synced']
                logging.info(f"Full sync completed: {total_synced} jobs synced")
                
                # Log portal stats
                for portal, stats in results['portals'].items():
                    logging.info(f"  {portal}: {stats['success']} success, {stats['failed']} failed")
            else:
                self.failure_count += 1
                logging.error(f"Full sync failed: {results['error']}")
                self.handle_sync_failure()
                
        except Exception as e:
            self.failure_count += 1
            logging.error(f"Exception during full sync: {e}")
            self.handle_sync_failure()
    
    def handle_sync_failure(self):
        """Handle sync failures with backoff."""
        max_failures = self.scheduler_config['max_failures']
        backoff_minutes = self.scheduler_config['failure_backoff_minutes']
        
        if self.failure_count >= max_failures:
            logging.warning(f"Max failures ({max_failures}) reached, backing off for {backoff_minutes} minutes")
            time.sleep(backoff_minutes * 60)
    
    def status_report_job(self):
        """Generate periodic status report."""
        try:
            now = datetime.now()
            
            # Calculate uptime
            if hasattr(self, 'start_time'):
                uptime = now - self.start_time
                uptime_str = str(uptime).split('.')[0]  # Remove microseconds
            else:
                uptime_str = "Unknown"
            
            # Last sync times
            last_sync_str = self.last_sync_time.strftime("%Y-%m-%d %H:%M:%S") if self.last_sync_time else "Never"
            last_full_sync_str = self.last_full_sync_time.strftime("%Y-%m-%d %H:%M:%S") if self.last_full_sync_time else "Never"
            
            # Portal status
            enabled_portals = [name for name, config in self.synchronizer.config['portals'].items() if config['enabled']]
            
            logging.info("=== JOB SYNC SCHEDULER STATUS ===")
            logging.info(f"Uptime: {uptime_str}")
            logging.info(f"Consecutive Failures: {self.failure_count}")
            logging.info(f"Last Incremental Sync: {last_sync_str}")
            logging.info(f"Last Full Sync: {last_full_sync_str}")
            logging.info(f"Active Portals: {', '.join(enabled_portals)}")
            logging.info("================================")
            
        except Exception as e:
            logging.error(f"Error generating status report: {e}")
    
    def setup_schedule(self):
        """Setup the sync schedule."""
        incremental_interval = self.scheduler_config['incremental_interval_minutes']
        full_sync_interval = self.scheduler_config['full_sync_interval_hours']
        
        # Schedule incremental syncs
        schedule.every(incremental_interval).minutes.do(self.incremental_sync_job)
        logging.info(f"Scheduled incremental sync every {incremental_interval} minutes")
        
        # Schedule full sync
        schedule.every(full_sync_interval).hours.do(self.full_sync_job)
        logging.info(f"Scheduled full sync every {full_sync_interval} hours")
        
        # Schedule status reports (every 6 hours)
        schedule.every(6).hours.do(self.status_report_job)
        logging.info("Scheduled status reports every 6 hours")
        
        # Run initial status report
        self.status_report_job()
    
    def run_scheduler(self):
        """Run the scheduler in a separate thread."""
        self.running = True
        self.start_time = datetime.now()
        
        logging.info("Job sync scheduler started")
        
        while self.running:
            schedule.run_pending()
            time.sleep(1)
        
        logging.info("Job sync scheduler stopped")
    
    def start(self, run_initial_sync=True):
        """Start the scheduler."""
        if self.running:
            logging.warning("Scheduler is already running")
            return
        
        # Setup schedule
        self.setup_schedule()
        
        # Run initial sync if requested
        if run_initial_sync:
            logging.info("Running initial incremental sync...")
            self.incremental_sync_job()
        
        # Start scheduler in background thread
        self.scheduler_thread = threading.Thread(target=self.run_scheduler, daemon=True)
        self.scheduler_thread.start()
        
        logging.info("Scheduler started successfully")
    
    def stop(self):
        """Stop the scheduler."""
        if not self.running:
            return
        
        self.running = False
        if self.scheduler_thread:
            self.scheduler_thread.join(timeout=5)
        
        logging.info("Scheduler stopped")
    
    def run_forever(self):
        """Run scheduler indefinitely (blocking)."""
        self.start()
        
        try:
            # Keep main thread alive
            while self.running:
                time.sleep(10)
        except KeyboardInterrupt:
            logging.info("Keyboard interrupt received")
        finally:
            self.stop()

def main():
    """Main function for scheduler."""
    import argparse
    
    parser = argparse.ArgumentParser(description='Run job sync scheduler')
    parser.add_argument('--config', help='Path to configuration file')
    parser.add_argument('--no-initial-sync', action='store_true', help='Skip initial sync on startup')
    parser.add_argument('--daemon', action='store_true', help='Run as daemon (background)')
    
    args = parser.parse_args()
    
    # Setup enhanced logging for scheduler
    log_format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    logging.basicConfig(
        level=logging.INFO,
        format=log_format,
        handlers=[
            logging.FileHandler('job_sync_scheduler.log'),
            logging.StreamHandler(sys.stdout)
        ]
    )
    
    # Create and start scheduler
    scheduler = JobSyncScheduler(config_file=args.config)
    
    print("="*60)
    print("JOB SYNC SCHEDULER")
    print("="*60)
    print("Starting automated job data synchronization...")
    print(f"Logs: job_sync_scheduler.log")
    print("Press Ctrl+C to stop")
    print("="*60)
    
    try:
        if args.daemon:
            # Run as daemon
            scheduler.start(run_initial_sync=not args.no_initial_sync)
            
            # Keep main process alive
            while True:
                time.sleep(3600)  # Sleep for 1 hour
        else:
            # Run interactively
            scheduler.run_forever()
            
    except KeyboardInterrupt:
        print("\nShutdown requested...")
    except Exception as e:
        logging.error(f"Scheduler error: {e}")
    finally:
        scheduler.stop()
        print("Job sync scheduler stopped.")

if __name__ == "__main__":
    main()
