import pandas as pd
import numpy as np
from supabase import create_client, Client

# Credenciales de Supabase
SUPABASE_URL = "https://wihcccvrwsiemsiddavs.supabase.co"
SUPABASE_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6IndpaGNjY3Zyd3NpZW1zaWRkYXZzIiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc1MDcwMDQ5NiwiZXhwIjoyMDY2Mjc2NDk2fQ.krFz_vYCVdABBmDgXw_kXc886w-KbVOemisdoHGI2zw"
NOMBRE_TABLA = "facturas"
NOMBRE_CSV = "facturas_completo.csv"

def main():
    # Leer el archivo CSV
    try:
        df = pd.read_csv(NOMBRE_CSV)
        # Reemplaza NaN, inf y -inf por None
        df = df.replace({np.nan: None, np.inf: None, -np.inf: None})
    except Exception as e:
        print(f"Error al leer el archivo CSV: {e}")
        return

    datos = df.to_dict(orient="records")

    # Conectar con Supabase
    supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

    # Insertar los datos en la tabla
    for fila in datos:
        try:
            respuesta = supabase.table(NOMBRE_TABLA).insert(fila).execute()
            print(f"Fila insertada: {respuesta}")
        except Exception as e:
            print(f"Error al insertar fila: {e}")

if __name__ == "__main__":
    main()