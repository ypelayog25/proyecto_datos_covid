import pandas as pd
import requests
import os
from datetime import datetime

class CovidDataExtractor:
    def __init__(self):
        self.base_url = "https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/"
        self.data_files = {
            'confirmed': 'time_series_covid19_confirmed_global.csv',
            'deaths': 'time_series_covid19_deaths_global.csv',
            'recovered': 'time_series_covid19_recovered_global.csv'
        }
        self.raw_data_path = 'data/raw/'
        
    def create_directories(self):
        os.makedirs(self.raw_data_path, exist_ok=True)
        os.makedirs('data/processed', exist_ok=True)
        
    def download_data(self):
        print(f"Descargando datos COVID-19 - {datetime.now()}")
        
        for data_type, filename in self.data_files.items():
            url = self.base_url + filename
            try:
                response = requests.get(url)
                response.raise_for_status()
                
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                local_filename = f"{data_type}_{timestamp}.csv"
                filepath = os.path.join(self.raw_data_path, local_filename)
                
                with open(filepath, 'wb') as f:
                    f.write(response.content)
                
                # Guardar versión actual
                current_filepath = os.path.join(self.raw_data_path, f"{data_type}_current.csv")
                with open(current_filepath, 'wb') as f:
                    f.write(response.content)
                    
                print(f"✅ Descargado: {data_type}")
                
            except requests.RequestException as e:
                print(f"❌ Error descargando {data_type}: {e}")

if __name__ == "__main__":
    extractor = CovidDataExtractor()
    extractor.create_directories()
    extractor.download_data()
