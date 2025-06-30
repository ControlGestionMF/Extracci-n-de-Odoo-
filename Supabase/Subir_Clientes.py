import xmlrpc.client
import pandas as pd
import requests
import json
import math
import numpy as np
from datetime import datetime

# --- Conexión a Odoo ---
url = 'https://movingfood.konos.cl'
db = 'movingfood-mfood-erp-main-7481157'
username = 'logistica@movingfood.cl'
api_key = '7a1e4e24b1f34abbe7c6fd93fd5fd75dccda90a6'

# --- Supabase ---
SUPABASE_URL = 'https://wihcccvrwsiemsiddavs.supabase.co'
SUPABASE_API_KEY = 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6IndpaGNjY3Zyd3NpZW1zaWRkYXZzIiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc1MDcwMDQ5NiwiZXhwIjoyMDY2Mjc2NDk2fQ.krFz_vYCVdABBmDgXw_kXc886w-KbVOemisdoHGI2zw'
SUPABASE_TABLE = 'clientes'

def conectar_odoo():
    try:
        common = xmlrpc.client.ServerProxy(f'{url}/xmlrpc/2/common')
        uid = common.authenticate(db, username, api_key, {})
        if not uid:
            raise Exception('Error de autenticación en Odoo')
        models = xmlrpc.client.ServerProxy(f'{url}/xmlrpc/2/object')
        return uid, models
    except Exception as e:
        raise Exception(f'Error al conectar con Odoo: {str(e)}')

def extraer_clientes(uid, models):
    try:
        fields = [
            'id', 'company_type', 'type', 'name', 'vat', 'visit_day',
            'street', 'street2', 'city', 'state_id', 'email', 'phone',
            'create_date', 'property_payment_term_id', 'credit_limit',
            'partner_latitude', 'partner_longitude', 'category_id', 'user_id',
            'parent_id', 'customer_rank', 'property_product_pricelist'
        ]

        clientes = models.execute_kw(
            db, uid, api_key,
            'res.partner', 'search_read',
            [[('customer_rank', '>', 0)]],
            {'fields': fields}
        )

        print(f"Datos recibidos de Odoo: {len(clientes)} registros")

        if not clientes:
            print("No se encontraron clientes en Odoo")
            return pd.DataFrame()

        df = pd.DataFrame(clientes)

        # Filtrar contactos personales
        df = df[~((df['company_type'] == 'person') & (df['type'] == 'contact'))].copy()

        # Procesar campos many2one
        campos_many2one = ['state_id', 'property_payment_term_id', 'user_id', 'parent_id', 'property_product_pricelist']
        for campo in campos_many2one:
            df[campo + '_id'] = df[campo].apply(lambda v: v[0] if isinstance(v, (list, tuple)) and v else None)
            df[campo + '_name'] = df[campo].apply(lambda v: v[1] if isinstance(v, (list, tuple)) and v else None)

        # Procesar etiquetas
        def procesar_etiquetas(cats):
            if not cats or not isinstance(cats, list):
                return None
            try:
                return ','.join([str(c[1]) for c in cats if isinstance(c, (list, tuple)) and len(c) > 1])
            except:
                return None

        df['etiqueta'] = df['category_id'].apply(procesar_etiquetas)
        df['comercial_company_name'] = df.get('parent_id_name', pd.Series([None] * len(df))).fillna(df['name'])
        
        # Manejo especial para fechas - MEJORADO
        def limpiar_fecha(fecha):
            if pd.isna(fecha) or fecha is None or str(fecha).lower() == 'nat':
                return None
            try:
                if isinstance(fecha, str):
                    # Si ya es un string, intentar parsearlo
                    fecha_dt = pd.to_datetime(fecha, errors='coerce')
                else:
                    fecha_dt = pd.to_datetime(fecha, errors='coerce')
                
                if pd.isna(fecha_dt):
                    return None
                
                return fecha_dt.isoformat()
            except:
                return None

        df['create_date'] = df['create_date'].apply(limpiar_fecha)

        # Renombrar columnas
        df_dw = df.rename(columns={
            'id': 'id_cliente',
            'company_type': 'tipo_compania',
            'type': 'tipo_direccion',
            'vat': 'rut',
            'visit_day': 'dia_visita',
            'street': 'calle1',
            'street2': 'calle2',
            'city': 'ciudad',
            'email': 'mail',
            'phone': 'telefono',
            'create_date': 'fecha_creacion',
            'credit_limit': 'credito_limite',
            'property_payment_term_id_name': 'plazo_pago',
            'state_id_name': 'comuna',
            'partner_latitude': 'geo_latitud',
            'partner_longitude': 'geo_longitud',
            'user_id_id': 'id_vendedor',
            'property_product_pricelist_name': 'tarifa'
        })

        # Seleccionar columnas finales
        columnas_finales = [
            'id_cliente', 'tipo_compania', 'tipo_direccion', 'comercial_company_name',
            'rut', 'dia_visita', 'calle1', 'calle2', 'comuna', 'ciudad',
            'mail', 'telefono', 'fecha_creacion', 'plazo_pago',
            'credito_limite', 'geo_latitud', 'geo_longitud',
            'etiqueta', 'id_vendedor', 'tarifa'
        ]

        return df_dw[columnas_finales]

    except Exception as e:
        print(f"Error al procesar clientes: {str(e)}")
        return pd.DataFrame()

def clean_data_for_json(data):
    """Convierte los valores especiales a formatos compatibles con JSON - MEJORADO"""
    if isinstance(data, dict):
        return {k: clean_data_for_json(v) for k, v in data.items()}
    elif isinstance(data, list):
        return [clean_data_for_json(v) for v in data]
    elif isinstance(data, (pd.Timestamp, datetime)):
        return data.isoformat() if not pd.isna(data) else None
    elif isinstance(data, str):
        # Verificar strings problemáticos
        if data.lower() in ['nat', 'nan', 'null', 'none', '']:
            return None
        return data
    elif isinstance(data, float):
        if math.isnan(data) or not math.isfinite(data):
            return None
        return data
    elif isinstance(data, (int, np.integer)):
        return int(data)
    elif isinstance(data, (np.floating)):
        if math.isnan(float(data)) or not math.isfinite(float(data)):
            return None
        return float(data)
    elif pd.isna(data):
        return None
    else:
        return data

def limpiar_dataframe_para_supabase(df):
    """Limpia exhaustivamente el DataFrame antes de enviarlo a Supabase"""
    df_clean = df.copy()
    
    # Reemplazar todos los valores problemáticos
    valores_problematicos = [pd.NaT, np.nan, 'NaT', 'nan', 'NaN', 'NULL', 'null', '']
    for valor in valores_problematicos:
        df_clean = df_clean.replace(valor, None)
    
    # Limpiar específicamente las columnas de fecha
    columnas_fecha = ['fecha_creacion']
    for col in columnas_fecha:
        if col in df_clean.columns:
            df_clean[col] = df_clean[col].apply(lambda x: None if pd.isna(x) or str(x).lower() in ['nat', 'nan', 'null', 'none', ''] else x)
    
    # Limpiar columnas numéricas
    columnas_numericas = ['geo_latitud', 'geo_longitud', 'credito_limite', 'id_cliente', 'id_vendedor']
    for col in columnas_numericas:
        if col in df_clean.columns:
            df_clean[col] = df_clean[col].apply(lambda x: None if pd.isna(x) or not isinstance(x, (int, float)) or (isinstance(x, float) and not math.isfinite(x)) else x)
    
    return df_clean

def subir_a_supabase(csv_path):
    try:
        # Leer el archivo CSV con configuraciones específicas
        df = pd.read_csv(csv_path, keep_default_na=False, na_values=['NaT', 'nan', 'NaN', 'NULL', 'null', ''])
        
        print(f"Datos leídos del CSV: {len(df)} registros")
        
        # Limpiar el DataFrame exhaustivamente
        df_clean = limpiar_dataframe_para_supabase(df)
        
        # Convertir a diccionario y limpiar datos para JSON
        data = df_clean.to_dict(orient='records')
        clean_data = clean_data_for_json(data)
        
        # Validar que no queden valores problemáticos
        def validar_registro(registro):
            for key, value in registro.items():
                if isinstance(value, str) and value.lower() in ['nat', 'nan', 'null']:
                    registro[key] = None
            return registro
        
        clean_data = [validar_registro(registro) for registro in clean_data]
        
        # Configurar headers
        headers = {
            "apikey": SUPABASE_API_KEY,
            "Authorization": f"Bearer {SUPABASE_API_KEY}",
            "Content-Type": "application/json",
            "Prefer": "resolution=merge-duplicates"
        }

        print(f"Subiendo {len(clean_data)} registros a Supabase...")
        
        # Mostrar una muestra de los datos para debug
        if clean_data:
            print("Muestra del primer registro:")
            for key, value in list(clean_data[0].items())[:5]:
                print(f"  {key}: {value} ({type(value)})")
        
        # Enviar datos en lotes más pequeños para mejor control
        batch_size = 100
        total_lotes = math.ceil(len(clean_data) / batch_size)
        
        for i in range(0, len(clean_data), batch_size):
            batch = clean_data[i:i + batch_size]
            lote_num = i // batch_size + 1
            
            # Verificar que el batch no esté vacío
            if not batch:
                continue
            
            print(f"Procesando lote {lote_num}/{total_lotes} ({len(batch)} registros)...")
            
            try:
                response = requests.post(
                    f"{SUPABASE_URL}/rest/v1/{SUPABASE_TABLE}",
                    json=batch,
                    headers=headers,
                    timeout=60
                )
                
                if response.status_code not in [200, 201, 204]:
                    error_details = response.json() if response.headers.get('content-type', '').startswith('application/json') else response.text
                    print(f"Detalles del error en lote {lote_num}:")
                    print(f"   Status: {response.status_code}")
                    print(f"   Response: {error_details}")
                    
                    # Si hay un error, mostrar algunos registros del lote para debug
                    print(f"Muestra de registros del lote fallido:")
                    for j, record in enumerate(batch[:3]):
                        print(f"  Registro {j+1}: {dict(list(record.items())[:3])}")
                    
                    raise Exception(f"Error al subir lote {lote_num}: {response.status_code} - {error_details}")
                
                print(f"Lote {lote_num}/{total_lotes} subido correctamente")
                
            except requests.exceptions.RequestException as e:
                raise Exception(f"Error de conexión al subir lote {lote_num}: {str(e)}")

        print("Todos los datos se subieron correctamente a Supabase")
        
    except Exception as e:
        raise Exception(f"Error al subir a Supabase: {str(e)}")

def main():
    try:
        print("Conectando a Odoo...")
        uid, models = conectar_odoo()

        print("Extrayendo clientes...")
        df_clientes = extraer_clientes(uid, models)

        if df_clientes.empty:
            print("No se encontraron clientes para procesar")
            return

        print(f"Guardando {len(df_clientes)} registros en CSV...")
        csv_path = 'clientes_odoo.csv'
        df_clientes.to_csv(csv_path, index=False, na_rep='')  # Cambié na_rep a string vacío
        print(f"Archivo guardado como {csv_path}")

        print("Iniciando subida a Supabase...")
        subir_a_supabase(csv_path)

    except Exception as e:
        print(f"\nError durante la ejecución: {str(e)}")

if __name__ == '__main__':
    main()