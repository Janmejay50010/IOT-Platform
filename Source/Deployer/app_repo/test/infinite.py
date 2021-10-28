import requests
import time
while(True):
    try:
        response = requests.get("https://api.mocki.io/v1/4fdc77da")
        if(response.status_code == 200):
            print(response.text)
        else:
            print(response)
    except Exception as e:
        print(e)
    finally:
        time.sleep(2)
