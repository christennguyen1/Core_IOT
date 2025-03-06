import requests
import time
from datetime import datetime
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class ThingsboardClient:
    def __init__(self, host="https://app.coreiot.io"):
        self.host = host
        self.token = None
        self.refresh_token = None
        self.session = requests.Session()
    
    def login(self, username, password):
        url = f"{self.host}/api/auth/login"
        headers = {
            "Content-Type": "application/json"
        }
        payload = {
            "username": username,
            "password": password
        }
        
        try:
            response = self.session.post(url, json=payload, headers=headers)
            response.raise_for_status()
            
            data = response.json()
            self.token = data.get("token")
            self.refresh_token = data.get("refreshToken")
            
            logger.info("Login successful")
            return True
            
        except requests.exceptions.HTTPError as e:
            logger.error(f"Login failed: {e}")
            logger.error(f"Response: {response.text}")
            return False
        except Exception as e:
            logger.error(f"Unexpected error during login: {e}")
            return False
    
    def get_telemetry_data(self, device_id, keys, start_time, end_time, limit=1000, interval=0):
        # Convert time strings to timestamps to get
        if isinstance(start_time, str):
            start_time = self._convert_time_to_ms(start_time)
        if isinstance(end_time, str):
            end_time = self._convert_time_to_ms(end_time)
        
        url = f"{self.host}/api/plugins/telemetry/DEVICE/{device_id}/values/timeseries"
        params = {
            "keys": keys,
            "startTs": start_time,
            "endTs": end_time,
            "interval": interval,
            "limit": limit,
            "useStrictDataTypes": "false"
        }
        headers = {
            "Authorization": f"Bearer {self.token}",
            "Content-Type": "application/json"
        }
        
        try:
            response = self.session.get(url, params=params, headers=headers)
            response.raise_for_status()
            
            data = response.json()
            logger.info(f"Successfully retrieved telemetry for keys: {keys}")
            return {"success": True, "data": data}
            
        except requests.exceptions.HTTPError as e:
            error_msg = f"Error retrieving telemetry data: {e}"
            logger.error(error_msg)
            
            # Process token expiration
            if response.status_code == 401:
                return {"success": False, "error": "Token expired", "status_code": 401}
            
            return {"success": False, "error": error_msg, "response": response.text}
        except Exception as e:
            error_msg = f"Unexpected error retrieving telemetry: {e}"
            logger.error(error_msg)
            return {"success": False, "error": error_msg}
    
    def _convert_time_to_ms(self, time_str):
        try:
            dt = datetime.strptime(time_str, "%d-%m-%Y %H:%M:%S")
            return int(dt.timestamp() * 1000)
        except ValueError as e:
            logger.error(f"Error converting time: {e}")
            raise ValueError(f"Invalid time format. Expected DD-MM-YYYY HH:MM:SS, got {time_str}")

def main():
    username = "vinh.nguyen123@hcmut.edu.vn"
    password = "Vinhnguyen1"
    device_id = "ebaeb540-e37c-11ef-ad09-515f790ed9df"
    attribute = "temperature"
    start_time = "2-2-2025 0:0:0"
    end_time = "3-5-2025 0:0:0"
    
    # Initialize client
    client = ThingsboardClient()
    
    # Login
    if not client.login(username, password):
        logger.error("Failed to login. Exiting.")
        return
    
    while True:
        try:
            # Get telemetry data
            result = client.get_telemetry_data(device_id, attribute, start_time, end_time)
            
            if result["success"]:
                # Process the data
                logger.info("Data retrieved successfully")
                logger.info(f"Data: {result['data']}")

                #ToDO
            else:
                # Handle errors
                if result.get("status_code") == 401:
                    logger.warning("Token expired. Attempting to login again.")
                    client.login(username, password)
                else:
                    logger.error(f"Error: {result.get('error')}")
            
            # Wait before next request
            time.sleep(5)
            
        except KeyboardInterrupt:
            logger.info("Program terminated by user")
            break
        except Exception as e:
            logger.error(f"Unexpected error in main loop: {e}")
            time.sleep(5)

if __name__ == "__main__":
    main()