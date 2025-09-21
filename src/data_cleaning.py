import pandas as pd
import os

class CovidDataCleaner:
    def __init__(self):
        self.raw_data_path = "data/raw/"
        self.processed_path = "data/processed/"
        os.makedirs(self.processed_path, exist_ok=True)

    def clean(self):
        datasets = {}
        for data_type in ["confirmed", "deaths", "recovered"]:
            filepath = os.path.join(self.raw_data_path, f"{data_type}_current.csv")
            if os.path.exists(filepath):
                df = pd.read_csv(filepath)

                # Quitar columnas innecesarias
                if "Province/State" in df.columns:
                    df = df.drop(columns=["Province/State"])
                
                # Normalizar nombres de columnas
                df = df.rename(columns={"Country/Region": "Country"})
                
                # Guardar limpio
                clean_path = os.path.join(self.processed_path, f"{data_type}_clean.csv")
                df.to_csv(clean_path, index=False)
                datasets[data_type] = df
                print(f"✅ Limpieza completa: {data_type}")
            else:
                print(f"⚠️ No se encontró {filepath}")
        return datasets

if __name__ == "__main__":
    cleaner = CovidDataCleaner()
    cleaner.clean()
