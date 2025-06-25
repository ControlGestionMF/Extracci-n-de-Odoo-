import xmlrpc.client
import pandas as pd

# Parámetros de conexión a Odoo
url = 'https://movingfood.konos.cl'
db = 'movingfood-mfood-erp-main-7481157'
username = 'logistica@movingfood.cl'
api_key = '7a1e4e24b1f34abbe7c6fd93fd5fd75dccda90a6'

# Establecer conexión XML-RPC con Odoo
def conectar_odoo():
    common = xmlrpc.client.ServerProxy(f'{url}/xmlrpc/2/common')
    uid = common.authenticate(db, username, api_key, {})
    if not uid:
        raise Exception('Error de autenticación en Odoo')
    models = xmlrpc.client.ServerProxy(f'{url}/xmlrpc/2/object')
    return uid, models

# Función para extraer clientes (modelo res.partner)
def extraer_clientes(uid, models):
    fields = [
        'id', 'company_type', 'type', 'name', 'vat', 'visit_day',
        'street', 'street2', 'city', 'state_id', 'email', 'phone',
        'create_date', 'property_payment_term_id', 'credit_limit',
        'partner_latitude', 'partner_longitude', 'category_id', 'user_id',
        'parent_id', 'customer_rank', 'property_product_pricelist'
    ]

    # Leer datos desde Odoo: solo clientes reales
    clientes = models.execute_kw(
        db, uid, api_key,
        'res.partner', 'search_read',
        [[('customer_rank', '>', 0)]],
        {'fields': fields}
    )
    df = pd.DataFrame(clientes)

    # Filtrar para eliminar registros tipo 'person' y 'contact'
    df = df[~((df['company_type'] == 'person') & (df['type'] == 'contact'))].copy()

    # Procesar campos Many2one
    campos_many2one = [
        'state_id', 'property_payment_term_id', 'user_id',
        'parent_id', 'property_product_pricelist'
    ]
    for campo in campos_many2one:
        df[campo + '_id'] = df[campo].apply(lambda v: v[0] if isinstance(v, (list, tuple)) and v else None)
        df[campo + '_name'] = df[campo].apply(lambda v: v[1] if isinstance(v, (list, tuple)) and v else None)

    # Procesar etiquetas
    def procesar_etiquetas(cats):
        if not cats or not isinstance(cats, list):
            return ''
        try:
            return ','.join([str(c[1]) for c in cats if isinstance(c, (list, tuple)) and len(c) > 1])
        except (TypeError, IndexError):
            return ''
    df['etiqueta'] = df['category_id'].apply(procesar_etiquetas)

    # Crear columna comercial_company_name (usa parent si existe, sino name)
    df['comercial_company_name'] = df['parent_id_name'].fillna(df['name'])

    # Mapear columnas al esquema final
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

    # Selección final de columnas
    columnas_finales = [
        'id_cliente', 'tipo_compania', 'tipo_direccion', 'comercial_company_name',
        'rut', 'dia_visita', 'calle1', 'calle2', 'comuna', 'ciudad',
        'mail', 'telefono', 'fecha_creacion', 'plazo_pago',
        'credito_limite', 'geo_latitud', 'geo_longitud',
        'etiqueta', 'id_vendedor', 'tarifa'
    ]

    df_final = df_dw[columnas_finales]

    return df_final

# Ejecución principal
def main():
    try:
        print("Conectando a Odoo...")
        uid, models = conectar_odoo()

        print("Extrayendo datos de clientes...")
        df_clientes = extraer_clientes(uid, models)

        print("\nMuestra de los datos extraídos:")
        print(df_clientes.head())

        print("\nResumen de datos:")
        print(f"Total de clientes filtrados: {len(df_clientes)}")
        print(f"Columnas: {list(df_clientes.columns)}")

        # Mostrar todos los valores únicos de tarifas
        print("\nTarifas distintas encontradas:")
        print(df_clientes['tarifa'].dropna().unique())

        # Clientes con tarifa asignada
        clientes_con_tarifa = df_clientes[df_clientes['tarifa'].notnull()]
        print(f"\nTotal clientes con tarifa asignada: {len(clientes_con_tarifa)}")
        print(clientes_con_tarifa[['comercial_company_name', 'tarifa']].head(10))

        # Clientes sin tarifa
        clientes_sin_tarifa = df_clientes[df_clientes['tarifa'].isnull()]
        print(f"\nTotal clientes sin tarifa asignada: {len(clientes_sin_tarifa)}")
        print(clientes_sin_tarifa[['comercial_company_name', 'tarifa']].head(10))

        # Guardar en CSV
        df_clientes.to_csv('clientes_odoo.csv', index=False)
        print("Datos guardados en clientes_odoo.csv")

    except Exception as e:
        print(f"\nError durante la ejecución: {str(e)}")

if __name__ == '__main__':
    main()
