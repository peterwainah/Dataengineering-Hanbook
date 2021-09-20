## import libraries
import requests
#import config as C
import json
import os
from datetime import datetime
from Apache_Airflow.case_study.setting.project_config import *

def fetch_weather_info():
    """This function will fetch openweathermap.com'S api AND  GET WEATHER FOR bROOKLYN,Ny AND then dump the json to the src/data directory
    with the file name "todays date".json" """
    # API_KEY=os.environ.get('API_KEY')
    # print(API_KEY)
    parameters={'q': 'Brooklyn, USA','appid':os.environ.get('API_KEY')}
    result = requests.get("http://api.openweathermap.org/data/2.5/weather?",parameters)
    print("result is ",result)

    # if the API call was successful ,get the json and dump it to a file with todays date as title.
    if result.status_code == 200:
        print("con")
        # Get json data
        json_data = result.json()
        file_name = str(datetime.now().date())+ '.json'
        tot_name =os.path.join(os.path.dirname("/home/peterwainaina/Dataengineering-Handbook/outputs/"), file_name)
        with open(tot_name,'w') as outputfile:
            json.dump(json_data,outputfile)

    else :
        print("Error in API call")

if __name__ == '__main__':
    fetch_weather_info()