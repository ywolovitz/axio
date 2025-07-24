#!/usr/bin/env python3
"""
Bulk Filtered Import Script
Imports data for all tables for the last 6 months starting from June 2025
"""

import requests
import json
import time
from datetime import datetime
from calendar import monthrange
import sys

# Server configuration
SERVER_URL = "http://localhost:3000"
ENDPOINT = "/import-filtered-data"

# All export IDs and their corresponding data types
EXPORT_MAPPINGS = {
    "5077534948": {"name": "buildings", "emoji": "🏢", "description": "Building information"},
    "5002645397": {"name": "cases", "emoji": "📋", "description": "Support cases and tickets"},
    "5002207692": {"name": "conversations", "emoji": "💬", "description": "Conversation history"},
    "5053863837": {"name": "interactions", "emoji": "🔄", "description": "User interactions"},
    "5157703494": {"name": "nocInteractions", "emoji": "🔧", "description": "NOC interactions"},
    "4693855982": {"name": "userStateInteractions", "emoji": "👤", "description": "User state changes"},
    "5157670999": {"name": "users", "emoji": "👥", "description": "User accounts"},
    "5219392695": {"name": "userSessionHistory", "emoji": "📅", "description": "Session history"},
    "20348692306": {"name": "schedule", "emoji": "📋", "description": "Schedule data"},
    "20357111093": {"name": "slaPolicy", "emoji": "📊", "description": "SLA policies"}
}


def generate_date_ranges():
    start_date = datetime(2025, 6, 29)
    end_date = datetime.now()
    date_ranges = []

    current = datetime(start_date.year, start_date.month, 1)

    while current <= end_date:
        year = current.year
        month = current.month

        # First day of the range
        if current.month == 6 and current.year == 2025:
            first_day = 29
        else:
            first_day = 1

        # Last day of the month or today if it's the current month
        if year == end_date.year and month == end_date.month:
            last_day = end_date.day
        else:
            last_day = monthrange(year, month)[1]

        start_str = f"{year:04d}-{month:02d}-{first_day:02d}"
        end_str = f"{year:04d}-{month:02d}-{last_day:02d}"
        month_name = datetime(year, month, 1).strftime("%B %Y")

        date_ranges.append({
            "start_date": start_str,
            "end_date": end_str,
            "month_name": month_name,
            "year": year,
            "month": month
        })

        # Move to the next month
        if month == 12:
            current = datetime(year + 1, 1, 1)
        else:
            current = datetime(year, month + 1, 1)

    return date_ranges

def make_filtered_import_request(export_id, start_date, end_date, data_info, max_retries=3):
    url = f"{SERVER_URL}{ENDPOINT}"
    payload = {
        "id": export_id,
        "startDate": start_date,
        "endDate": end_date
    }
    headers = {"Content-Type": "application/json"}

    for attempt in range(1, max_retries + 1):
        try:
            print(f"  🔄 Requesting {data_info['name']} data (Attempt {attempt}/{max_retries})...")
            response = requests.post(url, json=payload, headers=headers, timeout=300)

            print(f"    📡 Response status: {response.status_code}")
            print(f"    📄 Response content type: {response.headers.get('content-type', 'unknown')}")

            if response.status_code == 200:
                try:
                    result = response.json()
                    if result and result.get('success'):
                        results_data = result.get('results') or {}
                        database_data = results_data.get('database') or {}

                        records_found = results_data.get('filteredRecordsFound', 0)
                        records_inserted = database_data.get('recordsInserted', 0)
                        duplicates = database_data.get('duplicatesSkipped', 0)
                        duration = result.get('duration', 'Unknown')

                        print(f"  ✅ {data_info['emoji']} {data_info['name']}: {records_found} found, {records_inserted} inserted, {duplicates} duplicates, {duration}")
                        return {
                            'success': True,
                            'export_id': export_id,
                            'data_type': data_info['name'],
                            'records_found': records_found,
                            'records_inserted': records_inserted,
                            'duplicates_skipped': duplicates,
                            'duration': duration,
                            'json_file': results_data.get('jsonFile', {}).get('filename', 'N/A'),
                            'response': result
                        }
                    else:
                        error_msg = result.get('message', 'Unknown error') if result else 'Empty response'
                        raise ValueError(f"Response error: {error_msg}")
                except json.JSONDecodeError as e:
                    raise ValueError(f"Invalid JSON response: {str(e)}\nRaw response: {response.text[:300]}")
            else:
                raise ValueError(f"HTTP {response.status_code} - {response.text[:300]}")
        except Exception as e:
            print(f"  ❌ {data_info['emoji']} {data_info['name']} attempt {attempt} failed: {e}")
            if attempt < max_retries:
                wait = 2 ** attempt
                print(f"    ⏳ Retrying in {wait} seconds...")
                time.sleep(wait)
            else:
                return {
                    'success': False,
                    'export_id': export_id,
                    'data_type': data_info['name'],
                    'error': str(e)
                }


def test_server_connectivity():
    try:
        response = requests.get(f"{SERVER_URL}/health", timeout=10)
        if response.status_code == 200:
            health_data = response.json()
            print(f"✅ Server is reachable: {health_data.get('status', 'OK')}")
            return True
        else:
            print(f"❌ Server returned {response.status_code}")
            return False
    except requests.exceptions.RequestException as e:
        print(f"❌ Cannot connect to server: {e}")
        return False


def save_results_to_file(results, filename):
    try:
        with open(filename, 'w') as f:
            json.dump(results, f, indent=2, default=str)
        print(f"💾 Results saved to: {filename}")
    except Exception as e:
        print(f"❌ Failed to save results: {e}")


def print_summary(all_results):
    print(f"\n{'='*60}")
    print(f"📊 BULK IMPORT SUMMARY")
    print(f"{'='*60}")

    total_operations = len(all_results)
    successful_operations = sum(1 for result in all_results if result['success'])
    failed_operations = total_operations - successful_operations

    total_records_found = sum(result.get('records_found', 0) for result in all_results if result['success'])
    total_records_inserted = sum(result.get('records_inserted', 0) for result in all_results if result['success'])
    total_duplicates = sum(result.get('duplicates_skipped', 0) for result in all_results if result['success'])

    print(f"📈 Operations: {successful_operations}/{total_operations} successful ({failed_operations} failed)")
    print(f"📊 Records Found: {total_records_found:,}")
    print(f"💾 Records Inserted: {total_records_inserted:,}")
    print(f"🔄 Duplicates Skipped: {total_duplicates:,}")

    print(f"\n📅 Results by Month:")
    month_results = {}
    for result in all_results:
        month = result.get('month_name', 'Unknown')
        if month not in month_results:
            month_results[month] = {'success': 0, 'failed': 0, 'records': 0}
        if result['success']:
            month_results[month]['success'] += 1
            month_results[month]['records'] += result.get('records_inserted', 0)
        else:
            month_results[month]['failed'] += 1

    for month, stats in month_results.items():
        print(f"  {month}: {stats['success']} success, {stats['failed']} failed, {stats['records']:,} records")

    print(f"\n📋 Results by Data Type:")
    type_results = {}
    for result in all_results:
        data_type = result.get('data_type', 'Unknown')
        if data_type not in type_results:
            type_results[data_type] = {'success': 0, 'failed': 0, 'records': 0}
        if result['success']:
            type_results[data_type]['success'] += 1
            type_results[data_type]['records'] += result.get('records_inserted', 0)
        else:
            type_results[data_type]['failed'] += 1

    for data_type, stats in sorted(type_results.items()):
        emoji = next((info['emoji'] for info in EXPORT_MAPPINGS.values() if info['name'] == data_type), '📊')
        print(f"  {emoji} {data_type}: {stats['success']} success, {stats['failed']} failed, {stats['records']:,} records")

    if failed_operations > 0:
        print(f"\n❌ Failed Operations:")
        for result in all_results:
            if not result['success']:
                print(f"  • {result.get('data_type', 'Unknown')} ({result.get('month_name', 'Unknown')}): {result.get('error', 'Unknown error')}")


def main():
    print("🚀 Bulk Filtered Import Script")
    print("📅 Importing data for all tables for the last 6 months starting from June 2025")
    print(f"🌐 Server: {SERVER_URL}")

    print(f"\n🔍 Testing server connectivity...")
    if not test_server_connectivity():
        print("❌ Cannot proceed without server connection")
        sys.exit(1)

    date_ranges = generate_date_ranges()

    print(f"\n📅 Date ranges to process:")
    for dr in date_ranges:
        print(f"  • {dr['month_name']}: {dr['start_date']} to {dr['end_date']}")

    print(f"\n📋 Data types to process:")
    for export_id, info in EXPORT_MAPPINGS.items():
        print(f"  • {info['emoji']} {info['name']} (ID: {export_id})")

    total_operations = len(date_ranges) * len(EXPORT_MAPPINGS)
    print(f"\n🎯 Total operations to perform: {total_operations}")

    if len(sys.argv) > 1 and sys.argv[1] == '--auto':
        print("🤖 Auto mode enabled, proceeding without confirmation...")
    else:
        confirm = input(f"\n⚠️  This will make {total_operations} API requests. Continue? (y/N): ")
        if confirm.lower() not in ['y', 'yes']:
            print("❌ Operation cancelled by user")
            sys.exit(0)

    all_results = []
    operation_count = 0

    print(f"\n{'='*60}")
    print(f"🚀 STARTING BULK IMPORT")
    print(f"{'='*60}")

    for date_range in date_ranges:
        print(f"\n📅 Processing {date_range['month_name']} ({date_range['start_date']} to {date_range['end_date']})")

        for export_id, data_info in EXPORT_MAPPINGS.items():
            operation_count += 1
            print(f"  [{operation_count}/{total_operations}] {data_info['emoji']} {data_info['name']}...")

            result = make_filtered_import_request(
                export_id,
                date_range['start_date'],
                date_range['end_date'],
                data_info
            )

            result['month_name'] = date_range['month_name']
            result['start_date'] = date_range['start_date']
            result['end_date'] = date_range['end_date']
            result['operation_number'] = operation_count
            result['timestamp'] = datetime.now().isoformat()

            all_results.append(result)
            time.sleep(1)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    results_filename = f'bulk_import_results_{timestamp}.json'
    save_results_to_file(all_results, results_filename)

    print_summary(all_results)

    print(f"\n🎉 Bulk import completed!")
    print(f"📁 Detailed results saved to: {results_filename}")
    print(f"💾 JSON export files should be in: ./exports/")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print(f"\n\n❌ Script interrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"\n❌ Unexpected error: {e}")
        sys.exit(1)
