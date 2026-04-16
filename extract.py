import requests
import logging
import time

logging.basicConfig(
    filename="etl.log",
    level=logging.INFO,
    format='%(asctime)s:%(levelname)s:%(message)s'
)

logging.info("Starting ETL process")

def fetch_data(url, retries=3, backoff=2):
    headers = {
        "User-Agent": "Mozilla/5.0",
        "Accept": "application/json"
    }

    for attempt in range(1, retries + 1):
        try:
            response = requests.get(url, headers=headers, timeout=5)
            response.raise_for_status()
            return response.json()

        except requests.Timeout as e:
            logging.warning(f"Timeout error: {e}")

        except requests.HTTPError as e:
            logging.error(f"HTTP error: {e}")

        except Exception as e:
            logging.error(f"Unexpected error (Attempt {attempt}): {e}")

        time.sleep(backoff ** attempt)

    logging.error("All retry attempts failed")
    return None

#def checking_columns(columns):
 #   data = columns
#
 #   if data is None:
  #      return pd.DataFrame()

 #   if isinstance(data, list):
  #      print("Columns and Type")
   #     first = data[0]
    #    for c, t in first.items():
     #       print(c, type(t))

#    elif isinstance(data, dict):
#        print("Columns and Types")
#        for c, t in data.items():
#            print(c, type(t))

#    else:
#        print("No Data types or Columns:", data)

def extract_shows():
    url = "https://api.tvmaze.com/shows"
    data = fetch_data(url)

    if data is None:
        return []

    extracted = []

    for item in data:
        extracted.append({
            "name": item.get("name", "N/A"),
            "type": item.get("type", "N/A"),
            "language": item.get("language", "N/A"),
            "genres": ", ".join(item.get("genres", [])),
            "status": item.get("status", "N/A"),
            "runtime": item.get("runtime", 0)
        })

    return extracted