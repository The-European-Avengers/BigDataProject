#!/usr/bin/env python3
"""
Migration Script: Historical Streaming Data to New Structure

Migrates data from:
  /historical/2025/weather-{type}/12_historical_streaming/live_from_2025-12-05_16-30_batch-0_forecast-a94e5561
To:
  /historical/2025/forecast-{type}/12/05-16-30_batch-0_a94e5561

Usage:
  python migrate_historical_streaming.py --dry-run  # Preview changes
  python migrate_historical_streaming.py            # Execute migration
"""

import subprocess
import re
import sys
from datetime import datetime
from typing import List, Tuple, Optional


# Configuration
HDFS_BASE = "hdfs://namenode-g5:9000"
TOPICS = ["sun", "temp", "wind"]
YEAR = "2025"
MONTH = "12"

# Paths
OLD_BASE = f"{HDFS_BASE}/historical/{YEAR}"
NEW_BASE = f"{HDFS_BASE}/historical/{YEAR}"


class MigrationLogger:
    """Handles logging to both console and file"""
    
    def __init__(self, log_file: str):
        self.log_file = log_file
        self.start_time = datetime.now()
        
    def log(self, message: str, level: str = "INFO"):
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        log_entry = f"[{timestamp}] [{level}] {message}"
        print(log_entry)
        
        with open(self.log_file, "a") as f:
            f.write(log_entry + "\n")
    
    def summary(self, total: int, success: int, failed: int, skipped: int):
        elapsed = datetime.now() - self.start_time
        self.log("=" * 80)
        self.log("MIGRATION SUMMARY")
        self.log("=" * 80)
        self.log(f"Total directories: {total}")
        self.log(f"Successfully migrated: {success}")
        self.log(f"Failed: {failed}")
        self.log(f"Skipped: {skipped}")
        self.log(f"Elapsed time: {elapsed}")
        self.log("=" * 80)


def run_hdfs_command(command: List[str]) -> Tuple[bool, str]:
    """Execute HDFS command and return success status and output"""
    try:
        result = subprocess.run(
            command,
            capture_output=True,
            text=True,
            check=True
        )
        return True, result.stdout
    except subprocess.CalledProcessError as e:
        return False, e.stderr


def list_directories(path: str) -> List[str]:
    """List directories in HDFS path"""
    success, output = run_hdfs_command(["hdfs", "dfs", "-ls", path])
    
    if not success:
        return []
    
    directories = []
    for line in output.strip().split("\n"):
        if line.startswith("d"):  # Directory line
            parts = line.split()
            if len(parts) >= 8:
                dir_path = parts[-1]  # Last part is the full path
                directories.append(dir_path)
    
    return directories


def parse_old_directory_name(old_name: str) -> Optional[dict]:
    """
    Parse old directory name and extract components
    
    Example:
      live_from_2025-12-05_16-30_batch-0_forecast-a94e5561
    Returns:
      {
        'day': '05',
        'hour': '16',
        'minute': '30',
        'batch': '0',
        'uuid': 'a94e5561'
      }
    """
    # Pattern: live_from_YYYY-MM-DD_HH-MM_batch-N_forecast-UUID
    pattern = r"live_from_\d{4}-\d{2}-(\d{2})_(\d{2})-(\d{2})_batch-(\d+)_forecast-([a-f0-9]{8})"
    match = re.search(pattern, old_name)
    
    if not match:
        return None
    
    return {
        'day': match.group(1),
        'hour': match.group(2),
        'minute': match.group(3),
        'batch': match.group(4),
        'uuid': match.group(5)
    }


def build_new_directory_name(parsed: dict) -> str:
    """
    Build new directory name from parsed components
    
    Example:
      {'day': '05', 'hour': '16', 'minute': '30', 'batch': '0', 'uuid': 'a94e5561'}
    Returns:
      05-16-30_batch-0_a94e5561
    """
    return f"{parsed['day']}-{parsed['hour']}-{parsed['minute']}_batch-{parsed['batch']}_{parsed['uuid']}"


def migrate_directory(old_path: str, new_path: str, dry_run: bool, logger: MigrationLogger) -> bool:
    """
    Migrate a single directory from old structure to new
    Returns True if successful, False otherwise
    """
    if dry_run:
        logger.log(f"DRY-RUN: Would copy {old_path} -> {new_path}", level="DRY-RUN")
        return True
    
    # Execute copy
    logger.log(f"Copying: {old_path} -> {new_path}")
    success, output = run_hdfs_command([
        "hdfs", "dfs", "-cp", "-p",  # -p preserves timestamps
        old_path,
        new_path
    ])
    
    if success:
        logger.log(f"✓ Success: {new_path}", level="SUCCESS")
        return True
    else:
        logger.log(f"✗ Failed: {output}", level="ERROR")
        return False


def migrate_topic(topic: str, dry_run: bool, logger: MigrationLogger) -> Tuple[int, int, int]:
    """
    Migrate all directories for a single topic
    Returns (success_count, failed_count, skipped_count)
    """
    logger.log("=" * 80)
    logger.log(f"MIGRATING TOPIC: weather-{topic} -> forecast-{topic}")
    logger.log("=" * 80)
    
    # Paths
    old_topic_path = f"{OLD_BASE}/weather-{topic}/{MONTH}_historical_streaming"
    new_topic_path = f"{NEW_BASE}/forecast-{topic}/{MONTH}"
    
    # Check if old path exists
    success, _ = run_hdfs_command(["hdfs", "dfs", "-test", "-d", old_topic_path])
    if not success:
        logger.log(f"Old path does not exist: {old_topic_path}", level="WARNING")
        return 0, 0, 0
    
    # Create new base directory if needed
    if not dry_run:
        run_hdfs_command(["hdfs", "dfs", "-mkdir", "-p", new_topic_path])
        logger.log(f"Ensured directory exists: {new_topic_path}")
    
    # List all directories in old path
    directories = list_directories(old_topic_path)
    logger.log(f"Found {len(directories)} directories to migrate")
    
    success_count = 0
    failed_count = 0
    skipped_count = 0
    
    for i, old_full_path in enumerate(directories, 1):
        # Extract directory name
        old_dir_name = old_full_path.split("/")[-1]
        
        # Parse old name
        parsed = parse_old_directory_name(old_dir_name)
        if not parsed:
            logger.log(f"Cannot parse directory name: {old_dir_name}", level="WARNING")
            skipped_count += 1
            continue
        
        # Build new name
        new_dir_name = build_new_directory_name(parsed)
        new_full_path = f"{new_topic_path}/{new_dir_name}"
        
        # Check if target already exists
        if not dry_run:
            exists, _ = run_hdfs_command(["hdfs", "dfs", "-test", "-d", new_full_path])
            if exists:
                logger.log(f"Target already exists, skipping: {new_full_path}", level="WARNING")
                skipped_count += 1
                continue
        
        # Progress indicator
        if i % 10 == 0:
            logger.log(f"Progress: {i}/{len(directories)} ({i*100//len(directories)}%)")
        
        # Migrate
        if migrate_directory(old_full_path, new_full_path, dry_run, logger):
            success_count += 1
        else:
            failed_count += 1
    
    logger.log(f"Topic {topic}: {success_count} success, {failed_count} failed, {skipped_count} skipped")
    return success_count, failed_count, skipped_count


def main():
    """Main migration function"""
    # Parse arguments
    dry_run = "--dry-run" in sys.argv
    
    # Setup logging
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file = f"/tmp/migration_historical_streaming_{timestamp}.log"
    logger = MigrationLogger(log_file)
    
    # Header
    logger.log("=" * 80)
    logger.log("HISTORICAL STREAMING DATA MIGRATION")
    logger.log("=" * 80)
    logger.log(f"Mode: {'DRY-RUN (no changes will be made)' if dry_run else 'LIVE (changes will be executed)'}")
    logger.log(f"Log file: {log_file}")
    logger.log(f"Topics: {', '.join(TOPICS)}")
    logger.log("=" * 80)
    
    if dry_run:
        logger.log("*** DRY-RUN MODE: No actual changes will be made ***", level="WARNING")
    else:
        logger.log("*** LIVE MODE: Changes will be executed ***", level="WARNING")
        logger.log("Press Ctrl+C within 5 seconds to abort...")
        import time
        time.sleep(5)
    
    # Migrate each topic
    total_success = 0
    total_failed = 0
    total_skipped = 0
    
    for topic in TOPICS:
        success, failed, skipped = migrate_topic(topic, dry_run, logger)
        total_success += success
        total_failed += failed
        total_skipped += skipped
    
    # Summary
    total = total_success + total_failed + total_skipped
    logger.summary(total, total_success, total_failed, total_skipped)
    
    # Exit code
    if total_failed > 0:
        logger.log("Migration completed with errors", level="ERROR")
        sys.exit(1)
    else:
        logger.log("Migration completed successfully", level="SUCCESS")
        sys.exit(0)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\nMigration aborted by user")
        sys.exit(130)
    except Exception as e:
        print(f"\n\nFATAL ERROR: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
