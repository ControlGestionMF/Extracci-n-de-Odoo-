import xmlrpc.client
import pandas as pd

# Parámetros de conexión a Odoo
url = 'https://movingfood.konos.cl'
db = 'movingfood-mfood-erp-main-7481157'
username = 'logistica@movingfood.cl'
api_key = '7a1e4e24b1f34abbe7c6fd93fd5fd75dccda90a6'

# Conectar a Odoo
def conectar_odoo():
    common = xmlrpc.client.ServerProxy(f'{url}/xmlrpc/2/common')
    uid = common.authenticate(db, username, api_key, {})
    if not uid:
        raise Exception('Error de autenticación en Odoo')
    models = xmlrpc.client.ServerProxy(f'{url}/xmlrpc/2/object')
    return uid, models

# Extraer productos con información de impuestos
def extraer_productos_con_impuestos(uid, models):
    fields_productos = [
        'id', 'default_code', 'name', 'list_price',
        'standard_price', 'sale_ok', 'create_date', 'taxes_id'
    ]
    
    # Leer productos
    productos = models.execute_kw(
        db, uid, api_key,
        'product.product', 'search_read',
        [[],], {'fields': fields_productos}
    )
    
    # Leer todos los impuestos
    impuestos = models.execute_kw(
        db, uid, api_key,
        'account.tax', 'search_read',
        [[],], {'fields': ['id', 'name', 'amount']}
    )
    df_impuestos = pd.DataFrame(impuestos).set_index('id')

    # Construir lista de registros finales
    lista_final = []
    for prod in productos:
        impuestos_ids = prod.get('taxes_id', [])
        if impuestos_ids:
            for tax_id in impuestos_ids:
                if tax_id in df_impuestos.index:
                    tax = df_impuestos.loc[tax_id]
                    lista_final.append({
                        'id_producto': prod['id'],
                        'referencia_interna': prod.get('default_code'),
                        'nombre_producto': prod.get('name'),
                        'precio_unitario': prod.get('list_price'),
                        'coste_unitario': prod.get('standard_price'),
                        'puede_ser_vendido': prod.get('sale_ok'),
                        'fecha_creacion': prod.get('create_date'),
                        'id_impuesto': tax_id,
                        'nombre_impuesto': tax['name'],
                        'valor_impuesto': tax['amount']
                    })
        else:
            lista_final.append({
                'id_producto': prod['id'],
                'referencia_interna': prod.get('default_code'),
                'nombre_producto': prod.get('name'),
                'precio_unitario': prod.get('list_price'),
                'coste_unitario': prod.get('standard_price'),
                'puede_ser_vendido': prod.get('sale_ok'),
                'fecha_creacion': prod.get('create_date'),
                'id_impuesto': None,
                'nombre_impuesto': None,
                'valor_impuesto': None
            })

    df_final = pd.DataFrame(lista_final)
    return df_final

# Ejecución principal
def main():
    try:
        print("Conectando a Odoo...")
        uid, models = conectar_odoo()
        
        print("Extrayendo productos con impuestos...")
        df = extraer_productos_con_impuestos(uid, models)
        
        print("\nMuestra de datos extraídos:")
        print(df.head())
        print(f"\nTotal de registros: {len(df)}")
        
        df.to_csv('productos_con_impuestos.csv', index=False)
        print("\nArchivo guardado: productos_con_impuestos.csv")
        
    except Exception as e:
        print(f"\nError durante la ejecución: {str(e)}")

if __name__ == '__main__':
    main()
