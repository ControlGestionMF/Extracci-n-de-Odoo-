import xmlrpc.client
import pandas as pd
from datetime import datetime
from dateutil.relativedelta import relativedelta

# Parámetros de conexión a Odoo
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

def extraer_pedidos_venta(uid, models, batch_size=100, max_records=5000):
    fields = [
        'id', 'name', 'date_order', 'partner_id', 'user_id', 'amount_total',
        'amount_untaxed', 'amount_tax', 'state', 'invoice_status', 'pricelist_id',
        'payment_term_id', 'create_date', 'note'
    ]

    fecha_desde_dt = datetime.now() - relativedelta(months=2)
    fecha_desde = fecha_desde_dt.strftime('%Y-%m-%d %H:%M:%S')

    offset = 0
    all_pedidos = []

    while True:
        pedidos = models.execute_kw(
            db, uid, api_key,
            'sale.order', 'search_read',
            [[('date_order', '>=', fecha_desde)]],
            {
                'fields': fields,
                'limit': batch_size,
                'offset': offset,
                'order': 'date_order desc'
            }
        )
        if not pedidos:
            break

        all_pedidos.extend(pedidos)
        offset += batch_size

        print(f"Descargados {len(all_pedidos)} pedidos...")

        if len(all_pedidos) >= max_records:
            all_pedidos = all_pedidos[:max_records]
            break

    df = pd.DataFrame(all_pedidos)

    # Procesar campos Many2one
    many2one_campos = ['partner_id', 'user_id', 'pricelist_id', 'payment_term_id']
    for campo in many2one_campos:
        df[campo + '_id'] = df[campo].apply(lambda v: v[0] if isinstance(v, (list, tuple)) and v else None)

    # Obtener líneas de pedido con impuestos
    pedido_ids = df['id'].tolist()
    lineas = models.execute_kw(
        db, uid, api_key,
        'sale.order.line', 'search_read',
        [[('order_id', 'in', pedido_ids)]],
        {'fields': ['order_id', 'product_id', 'product_uom_qty', 'price_unit', 'tax_id']}
    )

    product_ids = list({linea['product_id'][0] for linea in lineas if 'product_id' in linea and linea['product_id']})

    productos = models.execute_kw(
        db, uid, api_key,
        'product.product', 'search_read',
        [[('id', 'in', product_ids)]],
        {'fields': ['id', 'name', 'product_tmpl_id']}
    )

    productos_map = {p['id']: (p['name'], p['product_tmpl_id'][0] if p['product_tmpl_id'] else None) for p in productos}
    template_ids = list({p['product_tmpl_id'][0] for p in productos if p['product_tmpl_id']})

    templates = models.execute_kw(
        db, uid, api_key,
        'product.template', 'search_read',
        [[('id', 'in', template_ids)]],
        {'fields': ['id', 'default_code']}
    )

    template_map = {t['id']: t['default_code'] for t in templates}

    tax_ids = set()
    for linea in lineas:
        if 'tax_id' in linea and linea['tax_id']:
            tax_ids.update(linea['tax_id'])

    impuestos = []
    if tax_ids:
        impuestos = models.execute_kw(
            db, uid, api_key,
            'account.tax', 'search_read',
            [[('id', 'in', list(tax_ids))]],
            {'fields': ['id', 'name']}
        )
    impuestos_map = {imp['id']: imp['name'] for imp in impuestos}

    productos_por_pedido = {}
    impuestos_por_pedido = {}
    for linea in lineas:
        order_id = linea['order_id'][0] if isinstance(linea['order_id'], (list, tuple)) else linea['order_id']
        product_id = linea['product_id'][0] if isinstance(linea['product_id'], (list, tuple)) else linea['product_id']
        producto_nombre = productos_map.get(product_id, ('', None))[0]
        template_id = productos_map.get(product_id, ('', None))[1]
        sku = template_map.get(template_id, '') if template_id else ''
        cantidad = linea.get('product_uom_qty', 0)
        precio = linea.get('price_unit', 0)
        texto = f"{producto_nombre} (SKU: {sku}) x{cantidad}, ${precio:.2f}"
        if order_id not in productos_por_pedido:
            productos_por_pedido[order_id] = []
        productos_por_pedido[order_id].append(texto)

        if 'tax_id' in linea and linea['tax_id']:
            nombres_taxes = [impuestos_map.get(tid, '') for tid in linea['tax_id']]
            if order_id not in impuestos_por_pedido:
                impuestos_por_pedido[order_id] = set()
            impuestos_por_pedido[order_id].update(nombres_taxes)

    df['productos'] = df['id'].apply(lambda id_: " | ".join(productos_por_pedido.get(id_, [])))
    df['impuestos'] = df['id'].apply(lambda id_: ", ".join(sorted(impuestos_por_pedido.get(id_, []))) if id_ in impuestos_por_pedido else "")

    df_final = df.rename(columns={
        'id': 'id_pedido',
        'name': 'nombre_pedido',
        'date_order': 'fecha_pedido',
        'amount_total': 'monto_total',
        'amount_untaxed': 'base_imponible',
        'amount_tax': 'monto_impuestos',
        'user_id_id': 'id_vendedor',
        'partner_id_id': 'id_cliente',
        'state': 'estado_pedido',
        'invoice_status': 'estado_factura',
        'pricelist_id': 'id_lista_precios',
        'payment_term_id': 'id_plazo_pago',
        'create_date': 'fecha_creacion',
        'note': 'note_new'
    })

    columnas = [
        'id_pedido', 'nombre_pedido', 'fecha_pedido', 'id_cliente',
        'id_vendedor', 'monto_total', 'base_imponible', 'monto_impuestos',
        'estado_pedido', 'estado_factura', 'id_lista_precios', 'id_plazo_pago',
        'fecha_creacion', 'note_new', 'productos', 'impuestos'
    ]

    return df_final[columnas]

def main():
    try:
        print("Conectando a Odoo...")
        uid, models = conectar_odoo()

        print("Extrayendo pedidos de venta últimos 2 meses (máx 5,000)...")
        df_pedidos = extraer_pedidos_venta(uid, models)

        print("\nMuestra de los pedidos extraídos:")
        print(df_pedidos.head())

        print(f"\nTotal de pedidos extraídos: {len(df_pedidos)}")
        print(f"Columnas: {list(df_pedidos.columns)}")

        df_pedidos.to_csv('pedidos_venta_odoo.csv', index=False)
        print("Datos guardados en pedidos_venta_odoo.csv")

    except Exception as main_exception:
        print(f"\nError durante la ejecución: {str(main_exception)}")

if __name__ == '__main__':
    main()
