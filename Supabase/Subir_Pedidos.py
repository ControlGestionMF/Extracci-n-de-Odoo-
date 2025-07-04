import pandas as pd
import numpy as np
from supabase import create_client, Client

# Credenciales de Supabase
SUPABASE_URL = "https://wihcccvrwsiemsiddavs.supabase.co"
SUPABASE_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6IndpaGNjY3Zyd3NpZW1zaWRkYXZzIiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc1MDcwMDQ5NiwiZXhwIjoyMDY2Mjc2NDk2fQ.krFz_vYCVdABBmDgXw_kXc886w-KbVOemisdoHGI2zw"
NOMBRE_TABLA = "pedidos"
NOMBRE_CSV = "pedidos_venta_odoo.csv"

# Corrige aquí los nombres de columnas tipo bigint (agrega todas las que sean bigint en tu tabla)
COLUMNAS_BIGINT = ["id_pedido", "id_cliente", "id_vendedor", "id_lista_precios", "id_plazo_pago"]

def safe_int(x):
    if x is None or pd.isna(x) or str(x).strip() == "":
        return None
    try:
        return int(float(str(x).strip()))
    except Exception:
        return None

def main():
    # Leer el archivo CSV
    try:
        df = pd.read_csv(NOMBRE_CSV)
        print("Columnas detectadas en el CSV:", list(df.columns))
        # Reemplaza NaN, inf y -inf por None
        df = df.replace({np.nan: None, np.inf: None, -np.inf: None})
        # Convierte columnas a enteros si no son None
        for col in COLUMNAS_BIGINT:
            if col in df.columns:
                print(f"Convirtiendo columna '{col}' a int...")
                df[col] = df[col].apply(safe_int)
        print("Tipos de datos después de conversión:\n", df.dtypes)
        print("Primeros valores de las columnas bigint:")
        for col in COLUMNAS_BIGINT:
            if col in df.columns:
                print(df[col].head())
    except Exception as e:
        print(f"Error al leer el archivo CSV: {e}")
        return

    # Conversión extra: fuerza a int cualquier columna que tenga solo valores tipo ".0" y que sea numérica
    for col in df.columns:
        if df[col].dtype in [np.float64, np.float32] or (
            df[col].dtype == object and df[col].astype(str).str.match(r"^\d+\.0$").any()
        ):
            try:
                df[col] = df[col].apply(lambda x: int(float(x)) if x is not None and str(x).strip() != "" and not pd.isna(x) else None)
            except Exception:
                pass

    datos = df.to_dict(orient="records")

    # Conectar con Supabase
    supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

    # Insertar los datos en la tabla
    for fila in datos:
        for col in COLUMNAS_BIGINT:
            if col in fila:
                print(f"Valor a insertar en '{col}': {fila[col]} ({type(fila[col])})")
        try:
            respuesta = supabase.table(NOMBRE_TABLA).insert(fila).execute()
            print(f"Fila insertada: {respuesta}")
        except Exception as e:
            print(f"Error al insertar fila: {e}")

if __name__ == "__main__":
    main()
