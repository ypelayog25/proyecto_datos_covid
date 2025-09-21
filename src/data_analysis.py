import pandas as pd
import os

class CovidDataAnalysis:
    def __init__(self):
        self.processed_path = "data/processed/"
        self.clean_file = os.path.join(self.processed_path, "covid_clean.csv")

    def load_data(self):
        if os.path.exists(self.clean_file):
            df = pd.read_csv(self.clean_file, parse_dates=["date"])
            print(f"✅ Datos cargados: {df.shape[0]} filas, {df.shape[1]} columnas")
            return df
        else:
            print("❌ No se encontró covid_clean.csv. Ejecuta primero data_transform.py")
            return None

    def validate_data(self, df):
        print("\n🔎 Validación de datos")
        print(f"Fechas: {df['date'].min().date()} → {df['date'].max().date()}")
        print(f"Países únicos: {df['Country'].nunique()}")
        print(f"Valores nulos: \n{df.isna().sum()}")

        # Reglas simples de validación
        if (df[["confirmed", "deaths", "recovered"]] < 0).any().any():
            print("⚠️ Se encontraron valores negativos en las métricas")
        else:
            print("✅ No hay valores negativos")

    def global_summary(self, df):
        """Métricas globales por fecha"""
        summary = df.groupby("date")[["confirmed", "deaths", "recovered"]].sum().reset_index()
        output_path = os.path.join(self.processed_path, "global_summary.csv")
        summary.to_csv(output_path, index=False)
        print(f"📊 Resumen global guardado en {output_path}")

    def top_countries(self, df, metric="confirmed", top_n=10):
        latest_date = df["date"].max()
        latest = df[df["date"] == latest_date]

        ranking = (
            latest.groupby("Country")[metric]
            .sum()
            .sort_values(ascending=False)
            .head(top_n)
            .reset_index()
        )
        output_path = os.path.join(self.processed_path, f"top_countries_{metric}.csv")
        ranking.to_csv(output_path, index=False)
        print(f"🌍 Ranking {metric} guardado en {output_path}")

    def run_pipeline(self):
        df = self.load_data()
        if df is not None:
            self.validate_data(df)
            self.global_summary(df)
            self.top_countries(df, "confirmed")
            self.top_countries(df, "deaths")
            print("✅ Análisis completado")

if __name__ == "__main__":
    analysis = CovidDataAnalysis()
    analysis.run_pipeline()
