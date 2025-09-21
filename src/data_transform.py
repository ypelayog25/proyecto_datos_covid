import pandas as pd
import os

class CovidDataTransformer:
    def __init__(self):
        self.processed_path = "data/processed/"
        os.makedirs(self.processed_path, exist_ok=True)

    def transform(self):
        dfs = {}
        for data_type in ["confirmed", "deaths", "recovered"]:
            filepath = os.path.join(self.processed_path, f"{data_type}_clean.csv")
            if os.path.exists(filepath):
                df = pd.read_csv(filepath)
                df_long = df.melt(
                    id_vars=["Country", "Lat", "Long"],
                    var_name="date",
                    value_name=data_type
                )
                # Convertir fecha a formato consistente
                df_long["date"] = pd.to_datetime(df_long["date"], format="%m/%d/%y", errors="coerce")
                dfs[data_type] = df_long
            else:
                print(f"⚠️ No se encontró {filepath}")

        # Combinar en un solo dataframe
        merged = dfs["confirmed"].merge(
            dfs["deaths"], on=["Country", "Lat", "Long", "date"], how="left"
        ).merge(
            dfs["recovered"], on=["Country", "Lat", "Long", "date"], how="left"
        )

        # Guardar dataset final
        final_path = os.path.join(self.processed_path, "covid_clean.csv")
        merged.to_csv(final_path, index=False)
        print(f"✅ Datos transformados guardados en {final_path}")

if __name__ == "__main__":
    transformer = CovidDataTransformer()
    transformer.transform()
