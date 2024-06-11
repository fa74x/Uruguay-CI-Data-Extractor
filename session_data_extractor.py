import re
import os
import json
import argparse
import threading
import pytesseract
from PIL import Image
from io import BytesIO
from datetime import datetime
from browsermobproxy import Server
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options as ChromeOptions
from selenium.webdriver.common.desired_capabilities import DesiredCapabilities
from concurrent.futures import ThreadPoolExecutor, as_completed

# Constants
BROWSERMOB_PROXY_PATH = os.path.join(os.getcwd(), 'browsermob-proxy-2.1.4', 'bin', 'browsermob-proxy')
JSON_FILE_PATH = 'sessions.json'
TESSERACT_CMD_PATH = os.path.join(os.getcwd(), 'Tesseract-OCR', 'tesseract.exe')

# Set Tesseract OCR executable path
pytesseract.pytesseract.tesseract_cmd = TESSERACT_CMD_PATH

# Lock for thread-safe file operations
json_lock = threading.Lock()

def initialize_proxy():
    """Start the BrowserMob Proxy server and create a proxy."""
    server = Server(BROWSERMOB_PROXY_PATH)
    server.start()
    return server, server.create_proxy()

def setup_chrome_options(proxy):
    """Set up Chrome options to use the proxy."""
    chrome_options = ChromeOptions()
    chrome_options.add_argument('--log-level=3')
    chrome_options.add_argument(f'--proxy-server={proxy.proxy}')
    chrome_options.add_argument('--ignore-certificate-errors')  # Ignore SSL certificate errors
    chrome_options.add_experimental_option("excludeSwitches", ["enable-logging"])
    caps = DesiredCapabilities.CHROME.copy()
    caps['goog:loggingPrefs'] = {'performance': 'ALL'}
    chrome_options.set_capability('goog:loggingPrefs', {'performance': 'ALL'})
    return chrome_options

def extract_token_data(har, tries):
    """Extract token data from the HAR (HTTP Archive) data."""
    filtered_entries = []
    tab_id, token_id, timestamp1, timestamp2, cookie = None, None, None, None, None
    i = 0
    
    for entry in har['log']['entries']:
        url = entry['request']['url']
        if 'tokenId=' in url:
            i += 1
            if i == (200 + (tries * 6)):
                filtered_entries.append(entry)
                
                tab_id = re.search(r'tabId=([^&]+)', url).group(1)
                token_id = re.search(r'tokenId=([^&]+)', url).group(1)
                
                occurrences = [m.start() for m in re.finditer('timestamp=', url)]
                timestamp1 = url[occurrences[0] + len('timestamp='):occurrences[0] + len('timestamp=') + 13]
                timestamp2 = url[occurrences[1] + len('timestamp='):occurrences[1] + len('timestamp=') + 13]
                
                for header in entry['request']['headers']:
                    if header['name'] == 'Cookie':
                        cookie = re.search(r'JSESSIONID=([^;]+)', header['value']).group(1)
                        cookie = f"JSESSIONID={cookie}; ROUTEID=.mef01"
                        break
    return filtered_entries, tab_id, token_id, timestamp1, timestamp2, cookie

def append_to_json(data_to_append, json_file_path):
    """Append extracted data to the JSON file."""
    with json_lock:
        if os.path.exists(json_file_path):
            with open(json_file_path, 'r', encoding='utf-8') as file:
                existing_data = json.load(file)
        else:
            existing_data = []
        
        existing_data.append(data_to_append)
        
        with open(json_file_path, 'w', encoding='utf-8') as file:
            json.dump(existing_data, file, ensure_ascii=False, indent=4)

def run_instance():
    """Run a single instance of the main program."""
    try:
        # Start the BrowserMob Proxy server and create a proxy
        server, proxy = initialize_proxy()
        
        # Set up Chrome options
        chrome_options = setup_chrome_options(proxy)
        
        # Initialize WebDriver with proxy and capabilities
        driver = webdriver.Chrome(options=chrome_options)
        
        # Start capturing network traffic
        proxy.new_har('mef', options={'captureHeaders': True, 'captureContent': True})
        
        # Open the login page
        driver.get('https://www.tramitesenlinea.mef.gub.uy/Apia/portal/tramite.jsp?id=2629')
        
        # Locate the email input element and enter the email address
        email_input = WebDriverWait(driver, 5).until(
            EC.presence_of_element_located((By.ID, 'E_1361_1'))
        )
        email_input.send_keys('xxxxxx@gmail.com')
        email_input.send_keys(Keys.TAB)

        tries = 0  # Variable to keep track of CAPTCHA attempts

        while True:        
            if tries == 20:
                break
            try:
                # Locate the CAPTCHA image element
                captcha_image_element = WebDriverWait(driver, 5).until(
                    EC.presence_of_element_located((By.XPATH, '/html/body/div[1]/div/div[1]/main/div/div[2]/div/div/div/div/section[2]/div/div/div/div/div[3]/div[2]/div[1]/img'))
                )

                # Capture the CAPTCHA image in memory
                captcha_image_bytes = captcha_image_element.screenshot_as_png
                captcha_image = Image.open(BytesIO(captcha_image_bytes))

                # Apply a binary threshold to the image
                threshold = 128
                captcha_image = captcha_image.point(lambda p: p > threshold and 255)

                # Use pytesseract to extract text from the image
                captcha_text = pytesseract.image_to_string(captcha_image, config='--psm 13 --oem 3 -c tessedit_char_whitelist=0123456789abcdefghijklmnopqrstuvwxyz')[:5]

                # Enter the CAPTCHA text
                captcha_input = WebDriverWait(driver, 5).until(
                    EC.presence_of_element_located((By.ID, 'E_1361_3'))
                )
                captcha_input.clear()
                captcha_input.send_keys(captcha_text)
                
                # Click the 'Siguiente' button
                siguiente_button = WebDriverWait(driver, 5).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, '.actionButton.css-imh0o5[data-action="execution/task/confirm"]'))
                )
                siguiente_button.click()
            
                # Accept terms and conditions
                accept_terms = WebDriverWait(driver, 5).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, 'label[for="E_6687_2_0"].executionForm__radio.css-15dx1jm'))
                )
                accept_terms.click()

                break
            except:
                print(f"Wrong CAPTCHA. Trying again...")
                tries += 1  # Increment tries on CAPTCHA failure
        
        # Click the 'Siguiente' button
        siguiente_button = WebDriverWait(driver, 5).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, 'button.actionButton.css-imh0o5[data-action="execution/task/next"]'))
        )
        siguiente_button.click()
        
        # Select 'Tipo de persona'
        tipo_de_persona = WebDriverWait(driver, 5).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, 'input[name="CRMRCSPS_TIPO_DE_PERSONA_STR"][id="E_6647_1"]'))
        )
        tipo_de_persona.click()
        
        # Choose 'Persona Física'
        persona_fisica = WebDriverWait(driver, 5).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, 'div.autocomplete__option.css-9651vz[id="list__item__E_6647_1__1"]'))
        )
        persona_fisica.click()
        
        # Select 'País emisor'
        pais_emisor = WebDriverWait(driver, 5).until(
            EC.element_to_be_clickable((By.CSS_SELECTOR, 'input[name="CRMRCSPS_PAIS_EMISOR_STR"][id="E_6648_3"]'))
        )
        pais_emisor.click()
        
        # Choose 'Uruguay'
        uruguay = WebDriverWait(driver, 5).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, 'div.autocomplete__option.css-9651vz[id="list__item__E_6648_3__1"]'))
        )
        uruguay.click()
        
        # Enter CI number
        ci_input = WebDriverWait(driver, 5).until(
            EC.element_to_be_clickable((By.CSS_SELECTOR, 'input[name="CRMRCSPS_NUMERO_DE_DOCUMENTO_STR"][id="E_6648_2"]'))
        )
        ci_input.send_keys('50885301')
        ci_input.send_keys(Keys.TAB)
        
        # Extract token data from HAR
        har = proxy.har
        filtered_entries, tab_id, token_id, timestamp1, timestamp2, cookie = extract_token_data(har, tries)
        
        # Append extracted data to JSON file with current datetime
        data_to_append = {
            'tabId': tab_id,
            'tokenId': token_id,
            'timestamp1': timestamp1,
            'timestamp2': timestamp2,
            'cookie': cookie,
            'datetime': datetime.now().isoformat()
        }
        append_to_json(data_to_append, JSON_FILE_PATH)
        
        # Print extracted values
        print(f"Extracted tabId: {tab_id}")
        print(f"Extracted tokenId: {token_id}")
        print(f"Extracted timestamp1: {timestamp1}")
        print(f"Extracted timestamp2: {timestamp2}")
        print(f"Extracted JSESSIONID: {cookie}")
    
    except:
        print(f"Something went wrong. Closing the script\n")
    
    finally:
        # Close the WebDriver
        driver.quit()
        
        # Stop the proxy server
        server.stop()

def main(num_instances):
    """Main function to run multiple instances in parallel."""
    with ThreadPoolExecutor(max_workers=num_instances) as executor:
        futures = [executor.submit(run_instance) for _ in range(num_instances)]
        for future in as_completed(futures):
            try:
                future.result()
            except:
                print(f"Exception occurred.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Run multiple instances of the script in parallel.')
    parser.add_argument('--instances', type=int, default=1, help='Number of instances to run in parallel')
    args = parser.parse_args()
    main(args.instances)
