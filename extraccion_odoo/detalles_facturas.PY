import xmlrpc.client
import pandas as pd

# Configuración de conexión
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

def extraer_detalle_facturas(uid, models, batch_size=100, max_records=5000):
    factura_ids = models.execute_kw(
        db, uid, api_key,
        'account.move', 'search',
        [[('state', '=', 'posted'), ('move_type', 'in', ['out_invoice', 'out_refund'])]],
        {'limit': max_records}
    )

    if not factura_ids:
        return pd.DataFrame()

    print(f"Facturas encontradas: {len(factura_ids)}")

    lineas = models.execute_kw(
        db, uid, api_key,
        'account.move.line', 'search_read',
        [[('move_id', 'in', factura_ids), ('exclude_from_invoice_tab', '=', False)]],
        {
            'fields': [
                'id', 'move_id', 'product_id', 'quantity',
                'discount', 'price_unit', 'price_subtotal',
                'price_total'
            ],
            'limit': 100000
        }
    )

    print(f"Líneas de factura descargadas: {len(lineas)}")

    df = pd.DataFrame(lineas)
    df['id_documento'] = df['move_id'].apply(lambda v: v[0] if isinstance(v, (list, tuple)) and v else None)
    df['id_producto'] = df['product_id'].apply(lambda v: v[0] if isinstance(v, (list, tuple)) and v else None)

    # Obtener costo estándar desde product.product
    ids_productos = df['id_producto'].dropna().unique().tolist()
    productos_data = models.execute_kw(
        db, uid, api_key,
        'product.product', 'read',
        [ids_productos],
        {'fields': ['id', 'standard_price']}
    )
    costos = {p['id']: p['standard_price'] for p in productos_data}
    df['costo_unit'] = df['id_producto'].apply(lambda pid: costos.get(pid, 0.0))

    df = df.rename(columns={
        'id': 'id_linea',
        'quantity': 'cantidad',
        'discount': 'descuento',
        'price_unit': 'precio_unitario',
        'price_subtotal': 'subtotal',
        'price_total': 'total'
    })

    columnas = [
        'id_linea', 'id_documento', 'id_producto', 'cantidad',
        'descuento', 'precio_unitario', 'subtotal', 'total', 'costo_unit'
    ]

    return df[columnas]

def main():
    try:
        print("Conectando a Odoo...")
        uid, models = conectar_odoo()

        print("Extrayendo detalles de líneas de factura...")
        df_detalle = extraer_detalle_facturas(uid, models)

        print("\nMuestra de líneas extraídas:")
        print(df_detalle.head())

        print(f"\nTotal líneas extraídas: {len(df_detalle)}")

        df_detalle.to_csv('detalle_facturas.csv', index=False)
        print("Datos guardados en detalle_facturas.csv")

    except Exception as e:
        print(f"\nError durante la ejecución: {str(e)}")

if __name__ == '__main__':
    main()
