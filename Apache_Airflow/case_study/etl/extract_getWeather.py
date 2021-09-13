## import libraries
import requests
#import config as C
import json
import os
from datetime import datetime

def fetch_weather_info():
    """This function will fetch openweathermap.com'S api AND  GET WEATHER FOR bROOKLYN,Ny AND then dump the json to the src/data directory
    with the file name "todays date".json" """
    API_KEY='e4e9cd51a9ecd44bb1929b8c4784df55'
    print(API_KEY)
    parameters={'q': 'Brooklyn, USA','appid':API_KEY}
    print(parameters)
    result = requests.get("http://api.openweathermap.org/data/2.5/weather?q=London&appid=e4e9cd51a9ecd44bb1929b8c4784df55",)
    print("result is ",result)

    # if the API call was successful ,get the json and dump it to a file with todays date as title.
    if result.status_code == 200:
        print("con")
        # Get json data
        json_data = result.json()
        file_name = str(datetime.now().date())+ '.json'
        tot_name =os.path.join(os.path.dirname(), 'data',file_name)
        with open(tot_name,'w') as outputfile:
            json.dump(json_data,outputfile)

    else :
        print("Error in API call")

if __name__ == '__main__':
    fetch_weather_info()