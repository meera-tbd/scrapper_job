#!/usr/bin/env python3
"""
Simple Job Sync Runner
======================

Easy-to-use script for running job data synchronization between your scrapper and job portals.
"""

import os
import sys
import json
import argparse
from datetime import datetime

# Ensure this file works both as a script and as an importable module
CURRENT_DIR = os.path.dirname(__file__)
PROJECT_ROOT = os.path.abspath(os.path.join(CURRENT_DIR, os.pardir))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

# Use package-qualified import to avoid ModuleNotFoundError under Celery
from script.job_data_sync import JobDataSynchronizer

def display_config_summary(config):
    """Display current configuration summary."""
    print("\n" + "="*60)
    print("CURRENT CONFIGURATION")
    print("="*60)
    
    # Database info
    db_config = config['database']
    print(f"Database: {db_config['type'].upper()}")
    if db_config['type'] == 'sqlite':
        print(f"  File: {db_config['database']}")
    elif db_config['type'] == 'django':
        print("  Source: Django ORM (apps.jobs.models.JobPosting)")
    else:
        print(f"  Host: {db_config['host']}:{db_config['port']}")
        print(f"  Database: {db_config['database']}")
    
    # Portal info
    print(f"\nEnabled Portals:")
    for portal_name, portal_config in config['portals'].items():
        is_enabled = isinstance(portal_config, dict) and bool(portal_config.get('enabled', False))
        if is_enabled:
            base_url = portal_config.get('base_url', '(no base_url)') if isinstance(portal_config, dict) else '(invalid config)'
            print(f"  ✓ {portal_name.title()}: {base_url}")
        else:
            print(f"  ✗ {portal_name.title()}: Disabled")
    
    # Sync settings
    sync_config = config['sync']
    print(f"\nSync Settings:")
    print(f"  Batch Size: {sync_config['batch_size']} jobs")
    print(f"  Incremental: {'Yes' if sync_config['incremental'] else 'No'}")
    print(f"  Interval: {sync_config['sync_interval_minutes']} minutes")

def main():
    """Interactive job sync runner."""
    print("="*60)
    print("JOB DATA SYNCHRONIZER")
    print("Scrapper Database → Job Portals")
    print("="*60)
    
    # Parse optional --config argument
    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument('--config', help='Path to configuration file')
    args, _ = parser.parse_known_args()

    # Resolve config file
    config_file = None
    if args.config and os.path.exists(args.config):
        config_file = args.config
        print(f"✓ Using config: {config_file}")
    elif os.path.exists('job_sync_config.json'):
        config_file = 'job_sync_config.json'
        print("✓ Found job_sync_config.json")
    elif os.path.exists('.env'):
        print("✓ Found .env file")
    else:
        print("⚠ No configuration found. Using defaults.")
        print("  Create job_sync_config.json or use --config path for custom settings.")
    
    # Initialize synchronizer
    try:
        sync = JobDataSynchronizer(config_file=config_file)
        display_config_summary(sync.config)
    except Exception as e:
        print(f"\n❌ Failed to initialize synchronizer: {e}")
        print("\nPlease check your configuration and database connection.")
        return
    
    # Interactive options
    while True:
        print("\n" + "-"*40)
        print("SYNC OPTIONS:")
        print("1. Full Sync (All jobs)")
        print("2. Incremental Sync (New/updated jobs only)")
        print("3. Test Sync (Limit to 10 jobs)")
        print("4. Custom Sync")
        print("5. Exit")
        
        choice = input("\nSelect option (1-5): ").strip()
        
        if choice == '5':
            print("Goodbye!")
            break
        elif choice not in ['1', '2', '3', '4']:
            print("Invalid choice. Please select 1-5.")
            continue
        
        # Configure sync parameters
        limit = None
        incremental = True
        
        if choice == '1':  # Full sync
            incremental = False
            print("\n🔄 Starting FULL SYNC...")
        elif choice == '2':  # Incremental
            incremental = True
            print("\n🔄 Starting INCREMENTAL SYNC...")
        elif choice == '3':  # Test
            limit = 10
            incremental = False
            print("\n🧪 Starting TEST SYNC (10 jobs)...")
        elif choice == '4':  # Custom
            print("\nCustom Sync Configuration:")
            full_input = input("Full sync? (y/N): ").strip().lower()
            incremental = full_input != 'y'
            
            limit_input = input("Limit number of jobs (Enter for no limit): ").strip()
            if limit_input.isdigit():
                limit = int(limit_input)
        
        # Confirm before proceeding
        sync_type = "Full" if not incremental else "Incremental"
        limit_text = f" (limit: {limit})" if limit else ""
        print(f"\n📋 {sync_type} Sync{limit_text}")
        
        enabled_portals = [
            name for name, p_cfg in sync.config['portals'].items()
            if isinstance(p_cfg, dict) and p_cfg.get('enabled', False)
        ]
        print(f"📤 Target Portals: {', '.join(enabled_portals)}")
        
        confirm = input("\nProceed? (Y/n): ").strip().lower()
        if confirm in ['', 'y', 'yes']:
            # Run sync
            print(f"\n⏳ Synchronizing...")
            start_time = datetime.now()
            
            results = sync.sync_jobs(limit=limit, incremental=incremental)
            
            # Display results
            if results['status'] == 'success':
                print("\n✅ SYNC COMPLETED SUCCESSFULLY!")
                print(f"📊 Jobs Fetched: {results['jobs_fetched']}")
                print(f"📤 Total Synced: {results['total_synced']}")
                print(f"⏱ Duration: {results['duration_seconds']:.2f} seconds")
                
                print(f"\n📋 Portal Results:")
                for portal, stats in results['portals'].items():
                    success_rate = stats['success_rate'] * 100
                    print(f"  {portal.title()}: {stats['success']}/{stats['success'] + stats['failed']} ({success_rate:.1f}%)")
                
                # Save results
                result_file = f"sync_results_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
                with open(result_file, 'w') as f:
                    json.dump(results, f, indent=2)
                print(f"\n💾 Detailed results saved to: {result_file}")
                
            else:
                print(f"\n❌ SYNC FAILED: {results['error']}")
                print("Check the log file for more details.")
        else:
            print("Sync cancelled.")

if __name__ == "__main__":
    main()


# Non-interactive entrypoint for Celery scheduler
def run():
    """Run a non-interactive sync using config in this directory.

    Designed to be called as `script.run_job_sync:run` from the scheduler.
    - Uses `script/job_sync_config.json` if present
    - Respects env var `SYNC_FULL=true` to perform a full sync
    - Otherwise performs incremental sync
    """
    # Prefer explicit env-provided config path if given
    env_cfg = os.getenv('JOB_SYNC_CONFIG')
    config_path = env_cfg if env_cfg and os.path.exists(env_cfg) else os.path.join(CURRENT_DIR, 'job_sync_config.json')
    config_file = config_path if os.path.exists(config_path) else None

    sync = JobDataSynchronizer(config_file=config_file)
    full_env = (os.getenv('SYNC_FULL', 'false').lower() == 'true')
    return sync.sync_jobs(incremental=not full_env)
