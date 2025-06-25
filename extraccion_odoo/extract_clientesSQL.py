# EXTRAER DE ODOO Y CARGAR A MYSQL
import xmlrpc.client
import pandas as pd
import pymysql
import numpy as np
from pymysql.constants import CLIENT

# --- CONEXIÓN A ODOO ---
url = 'https://movingfood.konos.cl'
db = 'movingfood-mfood-erp-main-7481157'
username = 'logistica@movingfood.cl'
api_key = '7a1e4e24b1f34abbe7c6fd93fd5fd75dccda90a6'

def conectar_odoo():
    common = xmlrpc.client.ServerProxy(f'{url}/xmlrpc/2/common')
    uid = common.authenticate(db, username, api_key, {})
    if not uid:
        raise Exception('Error de autenticación en Odoo')
    models = xmlrpc.client.ServerProxy(f'{url}/xmlrpc/2/object')
    return uid, models

def extraer_clientes(uid, models):
    fields = [
        'id', 'company_type', 'type', 'name', 'vat', 'visit_day',
        'street', 'street2', 'city', 'state_id', 'email', 'phone',
        'create_date', 'property_payment_term_id', 'credit_limit',
        'partner_latitude', 'partner_longitude', 'category_id', 'user_id',
        'parent_id', 'customer_rank'
    ]
    clientes = models.execute_kw(
        db, uid, api_key,
        'res.partner', 'search_read',
        [[('customer_rank', '>', 0)]],
        {'fields': fields}
    )
    df = pd.DataFrame(clientes)
    df = df[~((df['company_type'] == 'person') & (df['type'] == 'contact'))].copy()

    for f in ['state_id', 'property_payment_term_id', 'user_id', 'parent_id']:
        df[f + '_id'] = df[f].apply(lambda v: v[0] if isinstance(v, (list, tuple)) and v else None)
        df[f + '_name'] = df[f].apply(lambda v: v[1] if isinstance(v, (list, tuple)) and v else None)

    def procesar_etiquetas(cats):
        if not cats or not isinstance(cats, list):
            return ''
        try:
            return ','.join([str(c[1]) for c in cats if isinstance(c, (list, tuple)) and len(c) > 1])
        except (TypeError, IndexError):
            return ''
    df['etiqueta'] = df['category_id'].apply(procesar_etiquetas)

    df_dw = df.rename(columns={
        'id': 'id_cliente',
        'company_type': 'tipo_compania',
        'type': 'tipo_direccion',
        'name': 'nombre_cliente',
        'vat': 'rut',
        'visit_day': 'dia_visita',
        'street': 'calle1',
        'street2': 'calle2',
        'city': 'ciudad',
        'email': 'mail',
        'phone': 'telefono',
        'create_date': 'fecha_creacion',
        'credit_limit': 'credito_limite',
        'parent_id_name': 'nombre_compania'
    })

    df_final = df_dw[[
        'id_cliente', 'tipo_compania', 'tipo_direccion', 'nombre_cliente',
        'nombre_compania', 'rut', 'dia_visita', 'calle1', 'calle2', 'state_id_name', 'ciudad',
        'mail', 'telefono', 'fecha_creacion', 'property_payment_term_id_name',
        'credito_limite', 'partner_latitude', 'partner_longitude',
        'etiqueta', 'user_id_id'
    ]]

    df_final.columns = [
        'id_cliente', 'tipo_compania', 'tipo_direccion', 'nombre_cliente',
        'nombre_compania', 'rut', 'dia_visita', 'calle1', 'calle2', 'comuna', 'ciudad',
        'mail', 'telefono', 'fecha_creacion', 'plazo_pago',
        'credito_limite', 'geo_latitud', 'geo_longitud',
        'etiqueta', 'id_vendedor'
    ]
    return df_final

# --- CARGAR A MYSQL ---
def cargar_a_mysql(df):
    config = {
        'host': 'localhost',
        'port': 3306,
        'user': 'root',
        'password': '2025',
        'database': 'clientes_mf',
        'charset': 'utf8mb4'
    }

    connection = pymysql.connect(**config)
    cursor = connection.cursor()

    cursor.execute("DROP TABLE IF EXISTS clientes")

    create_table = "CREATE TABLE clientes ("
    for col in df.columns:
        dtype = 'TEXT' if df[col].dtype == 'object' else 'FLOAT' if 'float' in str(df[col].dtype) else 'INT'
        create_table += f"`{col}` {dtype}, "
    create_table = create_table[:-2] + ")"
    cursor.execute(create_table)

    df = df.replace({np.nan: None})
    cols = ", ".join([f"`{c}`" for c in df.columns])
    vals = ", ".join(["%s"]*len(df.columns))

    for i, row in enumerate(df.itertuples(index=False), 1):
        try:
            cursor.execute(f"INSERT INTO clientes ({cols}) VALUES ({vals})", row)
            if i % 100 == 0:
                connection.commit()
        except Exception as e:
            print(f"Error en fila {i}: {e}")
    connection.commit()
    cursor.close()
    connection.close()
    print("Carga completa a MySQL.")

# --- EJECUCIÓN PRINCIPAL ---
def main():
    print("Conectando a Odoo...")
    uid, models = conectar_odoo()

    print("Extrayendo datos de clientes...")
    df_clientes = extraer_clientes(uid, models)

    print(f"Total de clientes extraídos: {len(df_clientes)}")
    cargar_a_mysql(df_clientes)

if __name__ == '__main__':
    main()
