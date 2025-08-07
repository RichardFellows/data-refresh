from flask import Flask, render_template, request, jsonify, redirect, url_for, flash
from datetime import datetime
import logging
import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.data_refresh import DataRefreshService

app = Flask(__name__)
app.secret_key = 'dev-key-change-in-production'

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

service = None

def get_service():
    global service
    if service is None:
        try:
            service = DataRefreshService()
        except Exception as e:
            logger.error(f"Failed to initialize data refresh service: {e}")
            service = None
    return service


@app.route('/')
def index():
    try:
        refresh_service = get_service()
        if not refresh_service:
            return render_template('error.html', error="Service not available"), 500
        
        table_status = refresh_service.get_table_status()
        connection_status = refresh_service.test_connections()
        
        return render_template('index.html', 
                             table_status=table_status,
                             connection_status=connection_status,
                             current_time=datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
    except Exception as e:
        logger.error(f"Error in index route: {e}")
        return render_template('error.html', error=str(e)), 500


@app.route('/api/status')
def api_status():
    try:
        refresh_service = get_service()
        if not refresh_service:
            return jsonify({"error": "Service not available"}), 500
        
        table_name = request.args.get('table')
        status = refresh_service.get_table_status(table_name)
        return jsonify(status)
    except Exception as e:
        logger.error(f"Error in status API: {e}")
        return jsonify({"error": str(e)}), 500


@app.route('/api/refresh', methods=['POST'])
def api_refresh():
    try:
        refresh_service = get_service()
        if not refresh_service:
            return jsonify({"error": "Service not available"}), 500
        
        data = request.get_json()
        table_name = data.get('table') if data else None
        
        if table_name:
            result = refresh_service.refresh_table(table_name)
        else:
            result = refresh_service.refresh_all_tables()
        
        return jsonify(result)
    except Exception as e:
        logger.error(f"Error in refresh API: {e}")
        return jsonify({"error": str(e)}), 500


@app.route('/api/test-connections')
def api_test_connections():
    try:
        refresh_service = get_service()
        if not refresh_service:
            return jsonify({"error": "Service not available"}), 500
        
        results = refresh_service.test_connections()
        return jsonify(results)
    except Exception as e:
        logger.error(f"Error in test connections API: {e}")
        return jsonify({"error": str(e)}), 500


@app.route('/refresh/<table_name>')
def refresh_table_page(table_name):
    try:
        refresh_service = get_service()
        if not refresh_service:
            flash(f"Service not available", 'error')
            return redirect(url_for('index'))
        
        result = refresh_service.refresh_table(table_name)
        
        if result.get('status') == 'success':
            flash(f"Successfully refreshed table '{table_name}'. Processed {result.get('rows_processed', 0)} rows.", 'success')
        else:
            flash(f"Failed to refresh table '{table_name}': {result.get('error', 'Unknown error')}", 'error')
        
        return redirect(url_for('index'))
    except Exception as e:
        logger.error(f"Error refreshing table {table_name}: {e}")
        flash(f"Error refreshing table '{table_name}': {str(e)}", 'error')
        return redirect(url_for('index'))


@app.route('/refresh-all')
def refresh_all_tables():
    try:
        refresh_service = get_service()
        if not refresh_service:
            flash("Service not available", 'error')
            return redirect(url_for('index'))
        
        results = refresh_service.refresh_all_tables()
        
        success_count = sum(1 for r in results if r.get('status') == 'success')
        error_count = sum(1 for r in results if r.get('status') == 'error')
        
        flash(f"Refresh completed. Success: {success_count}, Errors: {error_count}", 'info')
        
        return redirect(url_for('index'))
    except Exception as e:
        logger.error(f"Error refreshing all tables: {e}")
        flash(f"Error refreshing all tables: {str(e)}", 'error')
        return redirect(url_for('index'))


@app.errorhandler(404)
def not_found(error):
    return render_template('error.html', error="Page not found"), 404


@app.errorhandler(500)
def internal_error(error):
    return render_template('error.html', error="Internal server error"), 500


if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5000)