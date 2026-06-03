from flask import Flask, jsonify, request
from flask_cors import CORS
from pyhive import hive
import os
import redis
import json
from functools import wraps
import hashlib

app = Flask(__name__)
CORS(app)  # Enable CORS for all routes

# Hive connection configuration
HIVE_HOST = os.getenv('HIVE_HOST', 'localhost')
HIVE_PORT = int(os.getenv('HIVE_PORT', 10000))
HIVE_USERNAME = os.getenv('HIVE_USERNAME', 'root')
HIVE_DATABASE = os.getenv('HIVE_DATABASE', 'default')

# Redis connection configuration
REDIS_HOST = os.getenv('REDIS_HOST', 'k8s')
REDIS_PORT = int(os.getenv('REDIS_PORT', 6379))
redis_client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)

def cache_response(ttl=300):
    """Decorator to cache endpoint responses in Redis"""
    def decorator(f):
        @wraps(f)
        def wrapper(*args, **kwargs):
            # Create cache key from endpoint path and query parameters
            cache_key = f"{request.path}:{request.query_string.decode('utf-8')}"
            cache_key_hash = hashlib.md5(cache_key.encode()).hexdigest()
            
            try:
                # Try to get cached response
                cached_data = redis_client.get(cache_key_hash)
                if cached_data:
                    print(f"Cache HIT for key: {cache_key}")
                    return jsonify(json.loads(cached_data)), 200
            except Exception as e:
                print(f"Redis error (reading): {e}")
            
            # If not cached, execute the function
            print(f"Cache MISS for key: {cache_key}")
            response = f(*args, **kwargs)
            
            # Cache the response
            try:
                if response[1] == 200:  # Only cache successful responses
                    redis_client.setex(cache_key_hash, ttl, json.dumps(response[0].get_json()))
                    print(f"Cached response for {ttl} seconds")
            except Exception as e:
                print(f"Redis error (writing): {e}")
            
            return response
        return wrapper
    return decorator

def get_hive_connection():
    """Create and return a Hive connection"""
    return hive.Connection(host=HIVE_HOST, port=HIVE_PORT, username=HIVE_USERNAME)

def configure_hive_session(cursor):
    """Configure Hive session for better query execution"""
    # Set execution engine and other optimizations
    cursor.execute("SET hive.execution.engine=mr")
    cursor.execute("SET hive.mapred.mode=nonstrict")

@app.route('/health', methods=['GET'])
def health_check():
    """Health check endpoint"""
    return jsonify({'status': 'healthy'}), 200

@app.route('/databases', methods=['GET'])
def get_databases():
    """Get list of all databases"""
    try:
        conn = get_hive_connection()
        cursor = conn.cursor()
        cursor.execute('SHOW DATABASES')
        databases = [row[0] for row in cursor.fetchall()]
        conn.close()
        return jsonify({'databases': databases}), 200
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/tables', methods=['GET'])
def get_tables():
    """Get list of tables from specified database"""
    database = request.args.get('database', HIVE_DATABASE)
    try:
        conn = get_hive_connection()
        cursor = conn.cursor()
        cursor.execute(f'USE {database}')
        cursor.execute('SHOW TABLES')
        tables = [row[0] for row in cursor.fetchall()]
        conn.close()
        return jsonify({'database': database, 'tables': tables}), 200
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/query', methods=['POST'])
def execute_query():
    """Execute a custom Hive query"""
    data = request.get_json()
    
    if not data or 'query' not in data:
        return jsonify({'error': 'Query parameter is required'}), 400
    
    query = data['query']
    database = data.get('database', HIVE_DATABASE)
    
    try:
        conn = get_hive_connection()
        cursor = conn.cursor()
        
        if database:
            cursor.execute(f'USE {database}')
        
        cursor.execute(query)
        
        # Fetch column names
        columns = [desc[0] for desc in cursor.description] if cursor.description else []
        
        # Fetch results
        rows = cursor.fetchall()
        
        # Convert to list of dictionaries
        results = []
        for row in rows:
            results.append(dict(zip(columns, row)))
        
        conn.close()
        
        return jsonify({
            'columns': columns,
            'rows': results,
            'count': len(results)
        }), 200
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/table/<database>/<table>', methods=['GET'])
def get_table_data(database, table):
    """Get data from a specific table"""
    limit = request.args.get('limit', 100, type=int)
    offset = request.args.get('offset', 0, type=int)
    
    try:
        conn = get_hive_connection()
        cursor = conn.cursor()
        cursor.execute(f'USE {database}')
        cursor.execute(f'SELECT * FROM {table} LIMIT {limit} OFFSET {offset}')
        
        # Fetch column names
        columns = [desc[0] for desc in cursor.description]
        
        # Fetch results
        rows = cursor.fetchall()
        
        # Convert to list of dictionaries
        results = []
        for row in rows:
            results.append(dict(zip(columns, row)))
        
        conn.close()
        
        return jsonify({
            'database': database,
            'table': table,
            'columns': columns,
            'rows': results,
            'count': len(results),
            'limit': limit,
            'offset': offset
        }), 200
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/predictions', methods=['GET'])
@cache_response(ttl=600)  # Cache for 10 minutes
def get_predictions():
    """Get predictions data with optional filters, aggregated by minute and dkarea"""
    # Query parameters
    municipality_code = request.args.get('municipalityCode', type=int)
    dkarea = request.args.get('dkarea', type=int)
    start_date = request.args.get('startDate')  # Format: YYYY-MM-DD HH:MM:SS
    end_date = request.args.get('endDate')      # Format: YYYY-MM-DD HH:MM:SS
    sort_desc = request.args.get('sortDesc', 'false').lower() == 'true'
    limit = request.args.get('limit', 100, type=int)
    offset = request.args.get('offset', 0, type=int)
    
    try:
        conn = get_hive_connection()
        cursor = conn.cursor()
        
        # Build WHERE conditions
        conditions = []
        if municipality_code is not None:
            conditions.append(f'municipalityCode = {municipality_code}')
        
        if dkarea is not None and dkarea in [1, 2]:
            conditions.append(f'dkarea = {dkarea}')
        
        if start_date:
            conditions.append(f"`timestamp` >= CAST('{start_date}' AS TIMESTAMP)")
        
        if end_date:
            conditions.append(f"`timestamp` <= CAST('{end_date}' AS TIMESTAMP)")
        
        where_clause = ' WHERE ' + ' AND '.join(conditions) if conditions else ''
        
        # Query predictions table
        query = f'SELECT * FROM predictions{where_clause} LIMIT {limit * 100}'
        print(f"Executing query: {query}")
        cursor.execute(query)
        print("Query executed successfully.")
        
        # Fetch column names
        columns = [desc[0] for desc in cursor.description]
        
        # Fetch results from predictions
        rows = list(cursor.fetchall())
        
        # Query old_predictions table with same conditions
        old_query = f'SELECT * FROM old_predictions{where_clause} LIMIT {limit * 100}'
        print(f"Executing query: {old_query}")
        try:
            cursor.execute(old_query)
            print("Old predictions query executed successfully.")
            old_rows = cursor.fetchall()
            rows.extend(old_rows)
        except Exception as e:
            print(f"Warning: Could not fetch old_predictions: {e}")
        
        conn.close()
        
        # Aggregate by minute (ignoring seconds) in Python
        minute_aggregation = {}
        for row in rows:
            row_dict = dict(zip(columns, row))
            
            # Get timestamp
            timestamp = row_dict.get('predictions.timestamp', row_dict.get('timestamp', ''))
            
            if timestamp:
                # Convert timestamp to string and truncate to minute (remove seconds)
                ts_str = str(timestamp)
                # Format: "2026-01-03 14:42:50" -> "2026-01-03 14:42"
                # Take first 16 characters (YYYY-MM-DD HH:MM)
                minute_key = ts_str[:16]
                
                if minute_key not in minute_aggregation:
                    minute_aggregation[minute_key] = {
                        'timestamp': minute_key + ':00',  # Add :00 for seconds
                        'consumptionkwh': 0,
                        'productionkwh': 0,
                        'price_sum': 0,
                        'price_count': 0
                    }
                
                # Sum consumption
                consumption = row_dict.get('predictions.consumptionkwh', row_dict.get('consumptionkwh', 0))
                if consumption is not None:
                    minute_aggregation[minute_key]['consumptionkwh'] += float(consumption)
                
                # Sum production
                production = row_dict.get('predictions.productionkwh', row_dict.get('productionkwh', 0))
                if production is not None:
                    minute_aggregation[minute_key]['productionkwh'] += float(production)
                
                # Accumulate price for averaging
                price = row_dict.get('predictions.price', row_dict.get('price', 0))
                if price is not None:
                    minute_aggregation[minute_key]['price_sum'] += float(price)
                    minute_aggregation[minute_key]['price_count'] += 1
        
        # Calculate average price and convert to final results
        results = []
        for minute_key, data in minute_aggregation.items():
            avg_price = data['price_sum'] / data['price_count'] if data['price_count'] > 0 else 0
            results.append({
                'predictions.timestamp': data['timestamp'],
                'predictions.consumptionkwh': data['consumptionkwh'],
                'predictions.productionkwh': data['productionkwh'],
                'predictions.price': avg_price
            })
        
        # Sort by timestamp
        if sort_desc:
            results.sort(key=lambda x: x.get('predictions.timestamp', ''), reverse=True)
        else:
            results.sort(key=lambda x: x.get('predictions.timestamp', ''))
        
        # Apply limit and offset after aggregation
        results = results[offset:offset + limit]
        
        return jsonify({
            'database': 'analytics',
            'table': 'predictions',
            'columns': ['predictions.timestamp', 'predictions.consumptionkwh', 'predictions.productionkwh', 'predictions.price'],
            'rows': results,
            'count': len(results),
            'filters': {
                'municipalityCode': municipality_code,
                'dkarea': dkarea,
                'startDate': start_date,
                'endDate': end_date,
                'sortDesc': sort_desc
            }
        }), 200
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/predictions-precision', methods=['GET'])
@cache_response(ttl=600)  # Cache for 10 minutes
def get_predictions_precision():
    """Get predictions precision data aggregated by dkArea"""
    try:
        conn = get_hive_connection()
        cursor = conn.cursor()
        cursor.execute('USE analytics')
        
        # Query all data from predictions_precision table
        query = 'SELECT * FROM predictions_precision'
        print(f"Executing query: {query}")
        cursor.execute(query)
        print("Query executed successfully.")
        
        # Fetch column names
        columns = [desc[0] for desc in cursor.description]
        print(f"Columns: {columns}")
        
        # Fetch results
        rows = cursor.fetchall()
        print(f"Fetched {len(rows)} rows from predictions_precision table")
        
        conn.close()
        
        if len(rows) == 0:
            print("WARNING: No data found in predictions_precision table!")
            return jsonify({
                'table': 'predictions_precision',
                'overallAverage': {
                    'avgConsumptionPrecision': 0,
                    'avgPricePrecision': 0,
                    'totalRecords': 0
                },
                'aggregatedByDkArea': [],
                'rawData': [],
                'totalRecords': 0,
                'warning': 'No data available in predictions_precision table'
            }), 200
        
        # Aggregate by dkArea (1 or 2)
        dk_aggregation = {
            1: {'avgConsumptionPrecision_sum': 0, 'avgPricePrecision_sum': 0, 'count': 0},
            2: {'avgConsumptionPrecision_sum': 0, 'avgPricePrecision_sum': 0, 'count': 0}
        }
        
        all_rows = []
        for row in rows:
            row_dict = dict(zip(columns, row))
            all_rows.append(row_dict)
            
            # Get dkArea
            dk_area = row_dict.get('predictions_precision.dkarea', row_dict.get('dkarea'))
            
            if dk_area in [1, 2]:
                # Get avgConsumptionPrecision and avgPricePrecision
                avg_consumption = row_dict.get('predictions_precision.avgconsumptionprecision', 
                                               row_dict.get('avgconsumptionprecision', 0))
                avg_price = row_dict.get('predictions_precision.avgpriceprecision', 
                                         row_dict.get('avgpriceprecision', 0))
                
                if avg_consumption is not None:
                    dk_aggregation[dk_area]['avgConsumptionPrecision_sum'] += float(avg_consumption)
                if avg_price is not None:
                    dk_aggregation[dk_area]['avgPricePrecision_sum'] += float(avg_price)
                dk_aggregation[dk_area]['count'] += 1
        
        # Calculate overall averages across both DK areas
        total_consumption_sum = dk_aggregation[1]['avgConsumptionPrecision_sum'] + dk_aggregation[2]['avgConsumptionPrecision_sum']
        total_price_sum = dk_aggregation[1]['avgPricePrecision_sum'] + dk_aggregation[2]['avgPricePrecision_sum']
        total_count = dk_aggregation[1]['count'] + dk_aggregation[2]['count']
        
        overall_avg = {
            'avgConsumptionPrecision': total_consumption_sum / total_count if total_count > 0 else 0,
            'avgPricePrecision': total_price_sum / total_count if total_count > 0 else 0,
            'totalRecords': total_count
        }
        
        print(f"Overall average calculated: consumption={overall_avg['avgConsumptionPrecision']}, price={overall_avg['avgPricePrecision']}, records={total_count}")
        
        # Also keep per-area breakdown
        aggregated_results = []
        for dk_area in [1, 2]:
            count = dk_aggregation[dk_area]['count']
            if count > 0:
                aggregated_results.append({
                    'dkarea': dk_area,
                    'avgConsumptionPrecision': dk_aggregation[dk_area]['avgConsumptionPrecision_sum'] / count,
                    'avgPricePrecision': dk_aggregation[dk_area]['avgPricePrecision_sum'] / count,
                    'recordCount': count
                })
        
        return jsonify({
            'table': 'predictions_precision',
            'overallAverage': overall_avg,
            'aggregatedByDkArea': aggregated_results,
            'rawData': all_rows,
            'totalRecords': len(all_rows)
        }), 200
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/historical-production', methods=['GET'])
@cache_response(ttl=600)  # Cache for 10 minutes
def get_historical_production():
    """Get historical production data with optional filters, aggregated by day in Python"""
    # Query parameters
    dkarea = request.args.get('dkarea', type=int)
    municipality_code = request.args.get('municipalityCode', type=int)
    year = request.args.get('year', type=int)
    month = request.args.get('month', type=int)
    start_date = request.args.get('startDate')  # Format: YYYY-MM-DD HH:MM:SS
    end_date = request.args.get('endDate')      # Format: YYYY-MM-DD HH:MM:SS
    sort_desc = request.args.get('sortDesc', 'false').lower() == 'true'
    limit = request.args.get('limit', 100, type=int)
    offset = request.args.get('offset', 0, type=int)
    
    try:
        conn = get_hive_connection()
        cursor = conn.cursor()
        
        # Simple SELECT query - aggregation done in Python to avoid MapReduce
        query = 'SELECT * FROM historical_production'
        
        conditions = []
        if dkarea is not None and dkarea in [1, 2]:
            conditions.append(f'dkArea = {dkarea}')
        
        if municipality_code is not None:
            conditions.append(f'municipalityCode = {municipality_code}')
        
        if year is not None:
            conditions.append(f'year = {year}')
        
        if month is not None:
            conditions.append(f'month = {month}')
        
        if start_date:
            conditions.append(f"timeObserved >= '{start_date}'")
        
        if end_date:
            conditions.append(f"timeObserved <= '{end_date}'")
        
        if conditions:
            query += ' WHERE ' + ' AND '.join(conditions)
        
        # Fetch more data for aggregation, then apply limit after
        query += f' LIMIT {limit * 100}'
        
        print(f"Executing query: {query}")
        
        cursor.execute(query)
        
        print("Query executed successfully.")
        
        # Fetch column names
        columns = [desc[0] for desc in cursor.description]
        
        # Fetch results
        rows = cursor.fetchall()
        
        conn.close()
        
        # Aggregate by day in Python to avoid MapReduce issues
        daily_aggregation = {}
        for row in rows:
            row_dict = dict(zip(columns, row))
            
            # Get timeObserved and extract date
            time_observed = row_dict.get('historical_production.timeobserved', row_dict.get('timeobserved', ''))
            if time_observed:
                # Extract just the date part (YYYY-MM-DD)
                day = str(time_observed)[:10]
                dk_area = row_dict.get('historical_production.dkarea', row_dict.get('dkarea'))
                year_val = row_dict.get('historical_production.year', row_dict.get('year'))
                month_val = row_dict.get('historical_production.month', row_dict.get('month'))
                
                key = (day, dk_area, year_val, month_val)
                
                if key not in daily_aggregation:
                    daily_aggregation[key] = {
                        'day': day,
                        'dkarea': dk_area,
                        'year': year_val,
                        'month': month_val,
                        'totalproductionkwh': 0,
                        'totalsunproductionkwh': 0,
                        'totalwindproductionkwh': 0
                    }
                
                daily_aggregation[key]['totalproductionkwh'] += float(row_dict.get('historical_production.productionkwh', row_dict.get('productionkwh', 0)) or 0)
                daily_aggregation[key]['totalsunproductionkwh'] += float(row_dict.get('historical_production.sunproductionkwh', row_dict.get('sunproductionkwh', 0)) or 0)
                daily_aggregation[key]['totalwindproductionkwh'] += float(row_dict.get('historical_production.windproductionkwh', row_dict.get('windproductionkwh', 0)) or 0)
        
        # Convert to list
        results = list(daily_aggregation.values())
        
        # Sort by day
        if sort_desc:
            results.sort(key=lambda x: x.get('day', ''), reverse=True)
        else:
            results.sort(key=lambda x: x.get('day', ''))
        
        # Apply limit and offset after aggregation
        results = results[offset:offset + limit]
        
        return jsonify({
            'database': 'analytics',
            'table': 'historical_production',
            'columns': ['day', 'dkarea', 'year', 'month', 'totalproductionkwh', 'totalsunproductionkwh', 'totalwindproductionkwh'],
            'rows': results,
            'count': len(results),
            'filters': {
                'dkarea': dkarea,
                'municipalityCode': municipality_code,
                'year': year,
                'month': month,
                'startDate': start_date,
                'endDate': end_date,
                'sortDesc': sort_desc
            }
        }), 200
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/historical-consumption', methods=['GET'])
@cache_response(ttl=600)  # Cache for 10 minutes
def get_historical_consumption():
    """Get historical consumption data with filters, aggregated by day in Python"""
    # Query parameters - only year, month, dkarea filters
    dkarea = request.args.get('dkarea', type=int)
    year = request.args.get('year', type=int)
    month = request.args.get('month', type=int)
    sort_desc = request.args.get('sortDesc', 'false').lower() == 'true'
    limit = request.args.get('limit', 100, type=int)
    offset = request.args.get('offset', 0, type=int)
    
    try:
        conn = get_hive_connection()
        cursor = conn.cursor()
        
        # Simple query without GROUP BY to avoid MapReduce
        # Aggregation done in Python
        query = '''SELECT 
            from_unixtime(CAST(timedk / 1000000 AS BIGINT)) AS timedk,
            consumptionkwh,
            dkarea,
            year,
            month
        FROM historical_consumption'''
        
        conditions = []
        if dkarea is not None and dkarea in [1, 2]:
            conditions.append(f'dkarea = {dkarea}')
        
        # Default to 2025 if no year specified
        if year is not None:
            conditions.append(f'year = {year}')
        else:
            conditions.append('year = 2025')
        
        if month is not None:
            conditions.append(f'month = {month}')
        
        if conditions:
            query += ' WHERE ' + ' AND '.join(conditions)
        
        # Reduced limit to avoid timeout
        query += f' LIMIT {limit * 50}'
        
        print(f"Executing query: {query}")
        
        cursor.execute(query)
        
        print("Query executed successfully.")
        
        # Fetch column names
        columns = [desc[0] for desc in cursor.description]
        idx_timedk = columns.index('timedk')
        idx_consumption = columns.index('consumptionkwh')
        idx_dkarea = columns.index('dkarea')
        idx_year = columns.index('year')
        idx_month = columns.index('month')
        
        # Fetch results
        rows = cursor.fetchall()
        
        conn.close()
        
        print(f"Fetched {len(rows)} rows, aggregating...")
        
        # Aggregate by day in Python
        daily_aggregation = {}
        for row in rows:
            time_dk = row[idx_timedk]
            if not time_dk:
                continue
            
            # Extract date
            if hasattr(time_dk, 'strftime'):
                day = time_dk.strftime('%Y-%m-%d')
            else:
                day = str(time_dk)[:10]
            
            dk_area = row[idx_dkarea]
            key = (day, dk_area)
            
            if key in daily_aggregation:
                daily_aggregation[key]['totalconsumptionkwh'] += float(row[idx_consumption] or 0)
            else:
                daily_aggregation[key] = {
                    'day': day,
                    'dkarea': dk_area,
                    'year': row[idx_year],
                    'month': row[idx_month],
                    'totalconsumptionkwh': float(row[idx_consumption] or 0)
                }
        
        results = list(daily_aggregation.values())
        
        # Sort by day
        if sort_desc:
            results.sort(key=lambda x: x.get('day', ''), reverse=True)
        else:
            results.sort(key=lambda x: x.get('day', ''))
        
        # Apply limit and offset after aggregation
        results = results[offset:offset + limit]
        
        return jsonify({
            'database': 'analytics',
            'table': 'historical_consumption',
            'columns': ['day', 'dkarea', 'year', 'month', 'totalconsumptionkwh'],
            'rows': results,
            'count': len(results),
            'filters': {
                'dkarea': dkarea,
                'year': year,
                'month': month,
                'sortDesc': sort_desc
            }
        }), 200
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/historical-price', methods=['GET'])
@cache_response(ttl=600)  # Cache for 10 minutes
def get_historical_price():
    """Get historical price data with optional filters"""
    # Query parameters
    dkarea = request.args.get('dkarea', type=int)
    year = request.args.get('year', type=str)  # year is string partition
    sort_desc = request.args.get('sortDesc', 'false').lower() == 'true'
    limit = request.args.get('limit', 100, type=int)
    offset = request.args.get('offset', 0, type=int)
    
    try:
        conn = get_hive_connection()
        cursor = conn.cursor()
        
        # Build query - convert timestamp from microseconds to readable date
        query = 'SELECT from_unixtime(CAST(`timestamp` / 1000000 AS BIGINT)) as ts, dkarea, price_eur_mwh, year FROM historical_price'
        
        conditions = []
        if dkarea is not None and dkarea in [1, 2]:
            conditions.append(f'dkarea = {dkarea}')
        
        if year is not None:
            conditions.append(f"year = '{year}'")
        
        if conditions:
            query += ' WHERE ' + ' AND '.join(conditions)
        
        # Apply limit and offset
        query += f' LIMIT {limit}'
        if offset > 0:
            query += f' OFFSET {offset}'
        
        print(f"Executing query: {query}")
        
        cursor.execute(query)
        
        print("Query executed successfully.")
        
        # Fetch column names
        columns = [desc[0] for desc in cursor.description]
        
        # Fetch results
        rows = cursor.fetchall()
        
        # Convert to list of dictionaries
        results = []
        for row in rows:
            row_dict = {}
            for i, col in enumerate(columns):
                value = row[i]
                # Convert datetime objects to strings for JSON serialization
                if hasattr(value, 'isoformat'):
                    value = value.isoformat()
                row_dict[col] = value
            results.append(row_dict)
        
        conn.close()
        
        # Sort in Python to avoid Hive MapReduce issues
        # The ts field has incorrect year (e.g., "53970-12-15 08:00:000.0"), so we sort by:
        # 1. year attribute (correct year)
        # 2. month-day-time extracted from ts (ignoring the wrong year)
        def get_sort_key(row):
            year = row.get('year', '0')
            ts = row.get('ts', '')
            # Extract month-day-time part (everything after first hyphen)
            # Format: "53970-12-15 08:00:000.0" -> "12-15 08:00:000.0"
            month_day_time = ''
            if ts and '-' in ts:
                parts = ts.split('-', 1)
                if len(parts) > 1:
                    month_day_time = parts[1]
            return (year, month_day_time)
        
        if sort_desc:
            results.sort(key=get_sort_key, reverse=True)
        
        return jsonify({
            'database': 'analytics',
            'table': 'historical_price',
            'columns': columns,
            'rows': results,
            'count': len(results),
            'filters': {
                'dkarea': dkarea,
                'year': year,
                'sortDesc': sort_desc
            }
        }), 200
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)

