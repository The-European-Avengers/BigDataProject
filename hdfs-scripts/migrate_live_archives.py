#!/usr/bin/env python3
"""
HDFS Live Archives Migration Script
====================================
Migrates completed forecast cycles from old live-archives structure to new archives structure.

OLD STRUCTURE:
/historical/live-archives/<year>/weather-<topic>/<month>_historical_streaming/weather-<topic>_YYYY-MM-DD_HH-MM_UUID

NEW STRUCTURE:
/historical/archives/<year>/<month>/live/forecast-<topic>/DD-HH-MM-UUID

OPERATION: MOVE (copy + delete after all successful)
- Copies all directories first
- Only deletes source after ALL copies succeed
- Skips if target already exists

Topics: sun, temp, wind
"""

import subprocess
import sys
import re
import time
from datetime import datetime
from pathlib import Path


class MigrationLogger:
    """Handles both console and file logging"""
    
    def __init__(self, log_file):
        self.log_file = log_file
        self.console = True
        
    def log(self, level, message):
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        log_line = f"[{timestamp}] [{level}] {message}"
        
        if self.console:
            print(log_line)
        
        with open(self.log_file, 'a') as f:
            f.write(log_line + '\n')
    
    def info(self, msg):
        self.log("INFO", msg)
    
    def success(self, msg):
        self.log("SUCCESS", msg)
    
    def error(self, msg):
        self.log("ERROR", msg)
    
    def warning(self, msg):
        self.log("WARNING", msg)
    
    def dry_run(self, msg):
        self.log("DRY-RUN", msg)


def run_hdfs_command(command, logger, description=""):
    """Execute HDFS command and return success status"""
    try:
        if description:
            logger.info(f"Executing: {description}")
        
        result = subprocess.run(
            command,
            shell=True,
            capture_output=True,
            text=True,
            timeout=300
        )
        
        if result.returncode != 0:
            logger.error(f"Command failed: {command}")
            logger.error(f"Error: {result.stderr.strip()}")
            return False
        
        return True
    
    except subprocess.TimeoutExpired:
        logger.error(f"Command timed out: {command}")
        return False
    except Exception as e:
        logger.error(f"Command exception: {e}")
        return False


def parse_old_archive_name(directory_name):
    """
    Parse old live-archives directory name.
    
    Format: weather-<topic>_YYYY-MM-DD_HH-MM_UUID
    Example: weather-sun_2025-12-05_17-24_a94e5561
    
    Returns: (topic, year, month, day, hour, minute, uuid) or None
    """
    pattern = r'weather-(sun|temp|wind)_(\d{4})-(\d{2})-(\d{2})_(\d{2})-(\d{2})_([a-f0-9]{8})'
    match = re.match(pattern, directory_name)
    
    if match:
        topic, year, month, day, hour, minute, uuid = match.groups()
        return topic, year, month, day, hour, minute, uuid
    
    return None


def build_new_archive_name(day, hour, minute, uuid):
    """
    Build new archive directory name.
    
    Format: DD-HH-MM-UUID8
    Example: 05-17-24-a94e5561
    """
    return f"{day}-{hour}-{minute}-{uuid}"


def list_hdfs_directories(path, logger):
    """List all directories in HDFS path"""
    try:
        result = subprocess.run(
            f"hdfs dfs -ls {path}",
            shell=True,
            capture_output=True,
            text=True,
            timeout=30
        )
        
        if result.returncode != 0:
            return []
        
        directories = []
        for line in result.stdout.strip().split('\n'):
            if line.startswith('d'):
                parts = line.split()
                if len(parts) >= 8:
                    dir_path = parts[-1]
                    dir_name = dir_path.split('/')[-1]
                    directories.append(dir_name)
        
        return directories
    
    except Exception as e:
        logger.error(f"Failed to list directories in {path}: {e}")
        return []


def check_hdfs_exists(path, logger):
    """Check if HDFS path exists"""
    try:
        result = subprocess.run(
            f"hdfs dfs -test -e {path}",
            shell=True,
            capture_output=True,
            timeout=10
        )
        return result.returncode == 0
    except Exception:
        return False


def migrate_topic(topic, hdfs_namenode, logger, dry_run=False):
    """
    Migrate all archives for a single topic.
    
    Returns: (success_count, failed_count, skipped_count, directories_to_delete)
    """
    old_topic = f"weather-{topic}"
    new_topic = f"forecast-{topic}"
    
    logger.info("=" * 80)
    logger.info(f"MIGRATING TOPIC: {old_topic} -> {new_topic}")
    logger.info("=" * 80)
    
    success_count = 0
    failed_count = 0
    skipped_count = 0
    directories_to_delete = []
    
    # Find all month folders for this topic
    old_topic_base = f"{hdfs_namenode}/historical/live-archives/2025/{old_topic}"
    
    if not check_hdfs_exists(old_topic_base, logger):
        logger.warning(f"Topic base path does not exist: {old_topic_base}")
        return success_count, failed_count, skipped_count, directories_to_delete
    
    # List month folders (e.g., 12_historical_streaming)
    month_folders = list_hdfs_directories(old_topic_base, logger)
    
    if not month_folders:
        logger.warning(f"No month folders found in {old_topic_base}")
        return success_count, failed_count, skipped_count, directories_to_delete
    
    # Process each month folder
    for month_folder in month_folders:
        # Extract month number from folder name (e.g., "12_historical_streaming" -> "12")
        month_match = re.match(r'(\d{1,2})_historical_streaming', month_folder)
        if not month_match:
            logger.warning(f"Skipping non-matching folder: {month_folder}")
            continue
        
        month = month_match.group(1).zfill(2)  # Ensure 2-digit format
        
        old_month_path = f"{old_topic_base}/{month_folder}"
        logger.info(f"Processing month folder: {old_month_path}")
        
        # List all archive directories in this month
        archive_dirs = list_hdfs_directories(old_month_path, logger)
        
        if not archive_dirs:
            logger.warning(f"No archive directories found in {old_month_path}")
            continue
        
        logger.info(f"Found {len(archive_dirs)} archive directories to migrate")
        
        # Track this month folder for potential deletion
        month_folder_info = {
            'path': old_month_path,
            'count': len(archive_dirs),
            'migrated': []
        }
        
        # Process each archive directory
        for idx, archive_dir in enumerate(archive_dirs, 1):
            # Parse old directory name
            parsed = parse_old_archive_name(archive_dir)
            
            if not parsed:
                logger.error(f"Failed to parse directory name: {archive_dir}")
                failed_count += 1
                continue
            
            topic_parsed, year, month_parsed, day, hour, minute, uuid = parsed
            
            # Verify topic matches
            if topic_parsed != topic:
                logger.error(f"Topic mismatch: expected {topic}, got {topic_parsed} in {archive_dir}")
                failed_count += 1
                continue
            
            # Build paths
            old_full_path = f"{old_month_path}/{archive_dir}"
            new_dir_name = build_new_archive_name(day, hour, minute, uuid)
            new_full_path = f"{hdfs_namenode}/historical/archives/{year}/{month}/live/{new_topic}/{new_dir_name}"
            
            # Check if target already exists
            if check_hdfs_exists(new_full_path, logger):
                logger.warning(f"Target already exists, skipping: {new_full_path}")
                skipped_count += 1
                continue
            
            # Progress indicator
            if idx % 10 == 0 or idx == len(archive_dirs):
                logger.info(f"Progress: {idx}/{len(archive_dirs)} ({int(idx/len(archive_dirs)*100)}%)")
            
            if dry_run:
                logger.dry_run(f"DRY-RUN: Would move {old_full_path} -> {new_full_path}")
                success_count += 1
                month_folder_info['migrated'].append(archive_dir)
            else:
                # Create parent directories if needed
                new_parent = f"{hdfs_namenode}/historical/archives/{year}/{month}/live/{new_topic}"
                run_hdfs_command(f"hdfs dfs -mkdir -p {new_parent}", logger)
                
                # Copy the directory
                copy_cmd = f"hdfs dfs -cp -p {old_full_path} {new_full_path}"
                
                if run_hdfs_command(copy_cmd, logger, f"Copying {archive_dir}"):
                    logger.success(f"Copied {old_full_path} -> {new_full_path}")
                    success_count += 1
                    month_folder_info['migrated'].append(archive_dir)
                else:
                    logger.error(f"Failed to copy {old_full_path}")
                    failed_count += 1
        
        # If all archives in this month were migrated successfully, mark for deletion
        if len(month_folder_info['migrated']) == month_folder_info['count'] and month_folder_info['count'] > 0:
            directories_to_delete.append(month_folder_info['path'])
        else:
            logger.warning(f"Not all archives migrated for {old_month_path}, will NOT delete")
    
    logger.info(f"Topic {topic}: {success_count} success, {failed_count} failed, {skipped_count} skipped")
    
    return success_count, failed_count, skipped_count, directories_to_delete


def main():
    # Configuration
    hdfs_namenode = "hdfs://namenode-g5:9000"
    topics = ["sun", "temp", "wind"]
    
    # Check for dry-run mode
    dry_run = "--dry-run" in sys.argv
    
    # Setup logging
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file = f"/tmp/migration_live_archives_{timestamp}.log"
    logger = MigrationLogger(log_file)
    
    # Print header
    logger.info("=" * 80)
    logger.info("LIVE ARCHIVES MIGRATION")
    logger.info("=" * 80)
    logger.info(f"Mode: {'DRY-RUN (no changes will be made)' if dry_run else 'LIVE (changes WILL be made)'}")
    logger.info(f"Log file: {log_file}")
    logger.info(f"Topics: {', '.join(topics)}")
    logger.info("=" * 80)
    
    if dry_run:
        logger.info("*** DRY-RUN MODE: No actual changes will be made ***")
        logger.info("=" * 80)
    else:
        logger.info("!!! LIVE MODE: Changes WILL be made to HDFS !!!")
        logger.info("=" * 80)
        logger.info("Starting in 5 seconds... Press Ctrl+C to abort")
        for i in range(5, 0, -1):
            print(f"{i}...")
            time.sleep(1)
        print()
    
    # Track overall stats
    total_success = 0
    total_failed = 0
    total_skipped = 0
    all_directories_to_delete = []
    
    start_time = datetime.now()
    
    try:
        # Migrate each topic
        for topic in topics:
            success, failed, skipped, dirs_to_delete = migrate_topic(
                topic, hdfs_namenode, logger, dry_run
            )
            
            total_success += success
            total_failed += failed
            total_skipped += skipped
            all_directories_to_delete.extend(dirs_to_delete)
        
        # Delete old directories if all migrations succeeded
        if not dry_run and total_failed == 0 and len(all_directories_to_delete) > 0:
            logger.info("=" * 80)
            logger.info("DELETING OLD DIRECTORIES")
            logger.info("=" * 80)
            logger.info(f"All migrations successful. Deleting {len(all_directories_to_delete)} old directories...")
            
            for old_dir in all_directories_to_delete:
                delete_cmd = f"hdfs dfs -rm -r {old_dir}"
                if run_hdfs_command(delete_cmd, logger, f"Deleting {old_dir}"):
                    logger.success(f"Deleted {old_dir}")
                else:
                    logger.error(f"Failed to delete {old_dir}")
        
        elif dry_run and len(all_directories_to_delete) > 0:
            logger.info("=" * 80)
            logger.info("DIRECTORIES THAT WOULD BE DELETED")
            logger.info("=" * 80)
            for old_dir in all_directories_to_delete:
                logger.dry_run(f"Would delete: {old_dir}")
        
        elif total_failed > 0:
            logger.warning("=" * 80)
            logger.warning("SKIPPING DELETION DUE TO FAILURES")
            logger.warning("=" * 80)
            logger.warning(f"{total_failed} migrations failed. Old directories will NOT be deleted.")
            logger.warning("Fix errors and re-run the script.")
    
    except KeyboardInterrupt:
        logger.warning("\n\nMigration aborted by user!")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
    
    end_time = datetime.now()
    elapsed = end_time - start_time
    
    # Print summary
    logger.info("=" * 80)
    logger.info("MIGRATION SUMMARY")
    logger.info("=" * 80)
    logger.info(f"Total archives: {total_success + total_failed + total_skipped}")
    logger.info(f"Successfully migrated: {total_success}")
    logger.info(f"Failed: {total_failed}")
    logger.info(f"Skipped (already exist): {total_skipped}")
    
    if not dry_run and total_failed == 0:
        logger.info(f"Old directories deleted: {len(all_directories_to_delete)}")
    
    logger.info(f"Elapsed time: {elapsed}")
    logger.info("=" * 80)
    
    if total_failed == 0:
        logger.success("Migration completed successfully")
    else:
        logger.error(f"Migration completed with {total_failed} failures")
        sys.exit(1)


if __name__ == "__main__":
    main()
