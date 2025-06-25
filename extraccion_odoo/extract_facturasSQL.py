import xmlrpc.client
import pandas as pd
import pymysql
from pymysql.constants import CLIENT
import numpy as np

# Configuración Odoo
url = 'https://movingfood.konos.cl'
db = 'movingfood-mfood-erp-main-7481157'
username = 'logistica@movingfood.cl'
api_key = '7a1e4e24b1f34abbe7c6fd93fd5fd75dccda90a6'

# Configuración MySQL
mysql_config = {
    'host': 'localhost',
    'port': 3306,
    'user': 'root',
    'password': '2025',
    'database': 'clientes_mf',
    'charset': 'utf8mb4'
}

def conectar_odoo():
    common = xmlrpc.client.ServerProxy(f'{url}/xmlrpc/2/common')
    uid = common.authenticate(db, username, api_key, {})
    if not uid:
        raise Exception('Error de autenticación en Odoo')
    models = xmlrpc.client.ServerProxy(f'{url}/xmlrpc/2/object')
    return uid, models

def extraer_facturas(uid, models, batch_size=100, max_records=5000):
    fields = [
        'id', 'name', 'move_type', 'invoice_date', 'partner_id', 'amount_total',
        'amount_residual', 'amount_untaxed', 'invoice_origin',
        'invoice_payment_term_id', 'currency_id', 'state', 'create_date',
        'journal_id', 'l10n_latam_document_type_id', 'partner_shipping_id'
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

        if len(all_facturas) >= max_records:
            all_facturas = all_facturas[:max_records]
            break

    df = pd.DataFrame(all_facturas)

    df['id_cliente'] = df['partner_id'].apply(lambda v: v[0] if isinstance(v, (list, tuple)) and v else None)
    df['cliente'] = df['partner_id'].apply(lambda v: v[1] if isinstance(v, (list, tuple)) and v else None)
    df['plazo_pago'] = df['invoice_payment_term_id'].apply(lambda v: v[1] if isinstance(v, (list, tuple)) and v else None)
    df['moneda'] = df['currency_id'].apply(lambda v: v[1] if isinstance(v, (list, tuple)) and v else None)
    df['diario'] = df['journal_id'].apply(lambda v: v[1] if isinstance(v, (list, tuple)) and v else None)
    df['tipo_documento'] = df['l10n_latam_document_type_id'].apply(lambda v: v[1] if isinstance(v, (list, tuple)) and v else None)
    df['id_direccion_entrega'] = df['partner_shipping_id'].apply(lambda v: v[0] if isinstance(v, (list, tuple)) and v else None)
    df['direccion_entrega'] = df['partner_shipping_id'].apply(lambda v: v[1] if isinstance(v, (list, tuple)) and v else None)

    partner_ids = df['id_cliente'].dropna().unique().tolist()
    partner_ruts = {}
    if partner_ids:
        partners_data = models.execute_kw(
            db, uid, api_key,
            'res.partner', 'read',
            [partner_ids],
            {'fields': ['id', 'vat']}
        )
        partner_ruts = {p['id']: p.get('vat', '') for p in partners_data}
    df['rut'] = df['id_cliente'].apply(lambda x: partner_ruts.get(x, ''))

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
    df['impuestos'] = df['id'].apply(lambda fid: " | ".join([tax_map.get(tid, str(tid)) for tid in impuestos_por_factura.get(fid, [])]))

    df = df.rename(columns={
        'id': 'id_documento',
        'name': 'folio',
        'invoice_date': 'fecha_emision',
        'amount_total': 'monto_total',
        'amount_residual': 'monto_pendiente',
        'amount_untaxed': 'base_imponible',
        'invoice_origin': 'referencia_origen',
        'create_date': 'fecha_creacion',
        'state': 'estado'
    })

    estado_map = {
        'draft': 'Borrador',
        'posted': 'Publicado',
        'paid': 'Pagado',
        'cancel': 'Cancelado',
        'sent': 'Enviado'
    }
    df['estado'] = df['estado'].map(estado_map).fillna(df['estado'])

    columnas = [
        'id_documento', 'folio', 'tipo_documento', 'fecha_emision', 'cliente', 'id_cliente', 'rut',
        'monto_total', 'base_imponible', 'monto_pendiente', 'referencia_origen',
        'plazo_pago', 'moneda', 'estado', 'fecha_creacion',
        'id_direccion_entrega', 'direccion_entrega', 'diario', 'impuestos'
    ]

    return df[columnas]

def guardar_en_mysql(df):
    try:
        connection = pymysql.connect(**mysql_config)
        cursor = connection.cursor()

        cursor.execute("DROP TABLE IF EXISTS facturas")

        create_table = "CREATE TABLE facturas ("
        for col in df.columns:
            dtype = 'TEXT' if df[col].dtype == 'object' else 'FLOAT' if 'float' in str(df[col].dtype) else 'INT'
            create_table += f"`{col}` {dtype}, "
        create_table = create_table.rstrip(', ') + ")"

        cursor.execute(create_table)

        df = df.replace({np.nan: None})
        cols = ", ".join([f"`{c}`" for c in df.columns])
        vals = ", ".join(["%s"]*len(df.columns))

        for i, row in enumerate(df.itertuples(index=False), 1):
            try:
                cursor.execute(f"INSERT INTO facturas ({cols}) VALUES ({vals})", row)
                if i % 100 == 0:
                    connection.commit()
                    print(f"Insertadas {i} filas")
            except Exception as e:
                print(f"Error fila {i}: {e}")

        connection.commit()
        print(f"Total insertadas: {len(df)}")

    except Exception as e:
        print(f"Error MySQL: {e}")
    finally:
        cursor.close()
        connection.close()

def main():
    try:
        print("Conectando a Odoo...")
        uid, models = conectar_odoo()

        print("Extrayendo facturas...")
        df_facturas = extraer_facturas(uid, models)

        print("Guardando en base de datos MySQL...")
        guardar_en_mysql(df_facturas)

        # Opcional: guardar backup CSV
        df_facturas.to_csv('facturas_odoo.csv', index=False)
        print("Backup CSV generado: facturas_odoo.csv")

    except Exception as e:
        print(f"Error general: {str(e)}")

if __name__ == '__main__':
    main()
