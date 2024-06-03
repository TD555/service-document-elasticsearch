from waitress import serve
import time
import os
import requests


def is_elasticsearch_available():
    url = os.environ['ELASTICSEARCH_URL']
    max_retries = 60  # Number of retries to check Elasticsearch availability
    retry_interval = 4  # Seconds to wait between retries

    for attempt in range(max_retries):
        try:
            response = requests.get(url)
            if response.status_code == 200:
                return True
            else:
                print(f"Received unexpected status code: {response.status_code}")
        except requests.exceptions.RequestException as e:
            print(f"Error connecting to Elasticsearch: {e}")
        time.sleep(retry_interval)

    return False

if __name__ == "__main__":
    # if is_elasticsearch_available():
        import sys
        sys.path.insert(0, './main')
        from main.service import app
        
        # serve(app, port=5000)
        app.run(host='0.0.0.0', port=8000, debug=True, threaded=True)
