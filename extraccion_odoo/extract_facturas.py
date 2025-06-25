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

def extraer_facturas(uid, models, batch_size=100, max_records=5000):
    fields = [
        'id', 'name', 'move_type', 'invoice_date', 'invoice_date_due', 'partner_id',
        'amount_total', 'amount_residual', 'amount_untaxed', 'amount_untaxed_signed',
        'invoice_origin', 'invoice_payment_term_id', 'currency_id',
        'create_date', 'journal_id', 'l10n_latam_document_type_id', 'partner_shipping_id'
    ]

    offset = 0
    all_facturas = []

    while True:
        facturas = models.execute_kw(
            db, uid, api_key,
            'account.move', 'search_read',
            [[('state', '=', 'posted'), ('move_type', 'in', ['out_invoice', 'out_refund'])]],
            {
                'fields': fields,
                'limit': batch_size,
                'offset': offset,
                'order': 'invoice_date desc'
            }
        )
        if not facturas:
            break
        all_facturas.extend(facturas)
        offset += batch_size

        print(f"Descargadas {len(all_facturas)} facturas...")

        if len(all_facturas) >= max_records:
            all_facturas = all_facturas[:max_records]
            break

    df = pd.DataFrame(all_facturas)

    # Procesar campos Many2one
    df['id_cliente'] = df['partner_id'].apply(lambda v: v[0] if isinstance(v, (list, tuple)) and v else None)
    df['plazo_pago'] = df['invoice_payment_term_id'].apply(lambda v: v[1] if isinstance(v, (list, tuple)) and v else None)
    df['moneda'] = df['currency_id'].apply(lambda v: v[1] if isinstance(v, (list, tuple)) and v else None)
    df['diario'] = df['journal_id'].apply(lambda v: v[1] if isinstance(v, (list, tuple)) and v else None)
    df['tipo_documento'] = df['l10n_latam_document_type_id'].apply(lambda v: v[1] if isinstance(v, (list, tuple)) and v else None)
    df['id_direccion_entrega'] = df['partner_shipping_id'].apply(lambda v: v[0] if isinstance(v, (list, tuple)) and v else None)
    df['direccion_entrega'] = df['partner_shipping_id'].apply(lambda v: v[1] if isinstance(v, (list, tuple)) and v else None)

    # Obtener impuestos
    factura_ids = df['id'].tolist()
    impuestos_por_factura = {}
    tax_ids = set()

    lines = models.execute_kw(
        db, uid, api_key,
        'account.move.line', 'search_read',
        [[('move_id', 'in', factura_ids)]],
        {'fields': ['move_id', 'tax_ids']}
    )

    for line in lines:
        move_id_raw = line.get('move_id')
        move_id = move_id_raw[0] if isinstance(move_id_raw, (list, tuple)) and move_id_raw else move_id_raw

        for tid in line.get('tax_ids', []):
            if move_id not in impuestos_por_factura:
                impuestos_por_factura[move_id] = set()
            impuestos_por_factura[move_id].add(tid)
            tax_ids.add(tid)

    impuestos_data = []
    if tax_ids:
        impuestos_data = models.execute_kw(
            db, uid, api_key,
            'account.tax', 'read',
            [list(tax_ids)],
            {'fields': ['id', 'name']}
        )

    tax_map = {t['id']: t['name'] for t in impuestos_data}

    df['impuestos'] = df['id'].apply(
        lambda fid: " | ".join([tax_map.get(tid, str(tid)) for tid in impuestos_por_factura.get(fid, [])])
    )

    # Detalle de líneas de factura
    detalles = models.execute_kw(
        db, uid, api_key,
        'account.move.line', 'search_read',
        [[('move_id', 'in', factura_ids), ('exclude_from_invoice_tab', '=', False)]],
        {
            'fields': ['move_id', 'product_id', 'name', 'quantity', 'price_unit'],
            'limit': 100000
        }
    )

    detalle_factura = {}
    for d in detalles:
        fid = d['move_id'][0] if isinstance(d['move_id'], (list, tuple)) else d['move_id']
        texto = f"{d.get('name', '')} - {d.get('quantity', 0)} x {d.get('price_unit', 0):,.2f}"
        if fid not in detalle_factura:
            detalle_factura[fid] = []
        detalle_factura[fid].append(texto)

    df['detalle'] = df['id'].apply(lambda x: " | ".join(detalle_factura.get(x, [])))

    # Renombrar columnas
    df = df.rename(columns={
        'id': 'id_documento',
        'name': 'folio',
        'invoice_date': 'fecha_emision',
        'invoice_date_due': 'fecha_vencimiento',
        'amount_total': 'monto_total',
        'amount_residual': 'monto_pendiente',
        'amount_untaxed': 'base_imponible',
        'amount_untaxed_signed': 'base_imponible_firmada',
        'invoice_origin': 'referencia_origen',
        'create_date': 'fecha_creacion'
    })

    columnas = [
        'id_documento', 'folio', 'tipo_documento', 'fecha_emision', 'fecha_vencimiento',
        'id_cliente', 'monto_total', 'base_imponible', 'base_imponible_firmada',
        'monto_pendiente', 'referencia_origen',
        'plazo_pago', 'moneda', 'fecha_creacion',
        'id_direccion_entrega', 'direccion_entrega', 'diario', 'impuestos', 'detalle'
    ]

    return df[columnas]

def main():
    try:
        print("Conectando a Odoo...")
        uid, models = conectar_odoo()

        print("Extrayendo facturas de venta y notas de crédito...")
        df_facturas = extraer_facturas(uid, models)

        print("\nMuestra de facturas extraídas:")
        print(df_facturas.head())

        print(f"\nTotal facturas extraídas: {len(df_facturas)}")

        df_facturas.to_csv('facturas_odoo.csv', index=False)
        print("Datos guardados en facturas_odoo.csv")

    except Exception as e:
        print(f"\nError durante la ejecución: {str(e)}")

if __name__ == '__main__':
    main()
