#!/usr/bin/env python3
"""
Master Data Collection Orchestrator
Runs all three data collection scripts sequentially and reports results
"""

import subprocess
import sys
from datetime import datetime

# Scripts to run in order
SCRIPTS = [
    ("DMI Wind Data", "dmi_wind_collection.py"),
    ("DMI Sun Data", "dmi_sun_collection.py"),
    ("Heating Consumption Data", "Combined_Heating_Data_2022-2025.py"),
]

def run_script(name, script_path):
    """Run a single script and return success status."""
    print("\n" + "="*80)
    print(f"Starting: {name}")
    print(f"Script: {script_path}")
    print(f"Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("="*80 + "\n")
    
    start_time = datetime.now()
    
    try:
        result = subprocess.run(
            [sys.executable, script_path],
            check=True,
            capture_output=False,  # Show output in real-time
            text=True
        )
        
        end_time = datetime.now()
        duration = end_time - start_time
        
        print("\n" + "-"*80)
        print(f"✓ SUCCESS: {name}")
        print(f"Duration: {duration}")
        print("-"*80)
        
        return True, duration
        
    except subprocess.CalledProcessError as e:
        end_time = datetime.now()
        duration = end_time - start_time
        
        print("\n" + "-"*80)
        print(f"✗ FAILED: {name}")
        print(f"Error code: {e.returncode}")
        print(f"Duration: {duration}")
        print("-"*80)
        
        return False, duration
    except Exception as e:
        end_time = datetime.now()
        duration = end_time - start_time
        
        print("\n" + "-"*80)
        print(f"✗ ERROR: {name}")
        print(f"Exception: {e}")
        print(f"Duration: {duration}")
        print("-"*80)
        
        return False, duration

def main():
    """Run all data collection scripts."""
    print("\n" + "#"*80)
    print("# DMI & HEATING DATA COLLECTION ORCHESTRATOR")
    print("#"*80)
    print(f"Start time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Scripts to run: {len(SCRIPTS)}")
    print("#"*80 + "\n")
    
    overall_start = datetime.now()
    results = []
    
    # Run each script
    for name, script in SCRIPTS:
        success, duration = run_script(name, script)
        results.append((name, success, duration))
    
    overall_end = datetime.now()
    total_duration = overall_end - overall_start
    
    # Print summary
    print("\n\n" + "#"*80)
    print("# COLLECTION SUMMARY")
    print("#"*80)
    
    successful = sum(1 for _, success, _ in results if success)
    failed = len(results) - successful
    
    print(f"\nTotal scripts: {len(results)}")
    print(f"Successful: {successful}")
    print(f"Failed: {failed}")
    print(f"\nTotal duration: {total_duration}")
    
    print("\nDetailed Results:")
    print("-"*80)
    for name, success, duration in results:
        status = "✓ SUCCESS" if success else "✗ FAILED"
        print(f"{status:12} | {duration!s:>15} | {name}")
    
    print("#"*80 + "\n")
    
    # Exit with appropriate code
    if failed > 0:
        print(f"⚠ Warning: {failed} script(s) failed!")
        sys.exit(1)
    else:
        print("✓ All data collection completed successfully!")
        sys.exit(0)

if __name__ == "__main__":
    main()
