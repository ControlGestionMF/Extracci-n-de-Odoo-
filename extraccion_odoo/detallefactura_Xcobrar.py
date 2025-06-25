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

def extraer_facturas_por_cobranza(uid, models, max_records=5000):
    # Buscar facturas de cliente posteadas con saldo pendiente
    factura_ids = models.execute_kw(
        db, uid, api_key,
        'account.move', 'search',
        [[
            ('move_type', 'in', ['out_invoice', 'out_refund']),
            ('state', '=', 'posted'),
            ('amount_residual', '>', 0)
        ]],
        {'limit': max_records}
    )

    if not factura_ids:
        return pd.DataFrame()

    print(f"Facturas por cobrar encontradas: {len(factura_ids)}")

    # Leer datos de las facturas
    facturas = models.execute_kw(
        db, uid, api_key,
        'account.move', 'read',
        [factura_ids],
        {'fields': ['id', 'payment_state', 'amount_residual']}
    )

    # Convertir a DataFrame
    df = pd.DataFrame(facturas)

    # Renombrar columnas
    df = df.rename(columns={
        'id': 'id_documento',
        'payment_state': 'estado_pago',
        'amount_residual': 'importe_adeudado'
    })

    return df[['id_documento', 'estado_pago', 'importe_adeudado']]

def main():
    try:
        print("Conectando a Odoo...")
        uid, models = conectar_odoo()

        print("Extrayendo facturas por cobranza...")
        df_cobranza = extraer_facturas_por_cobranza(uid, models)

        print("\nMuestra de facturas por cobrar:")
        print(df_cobranza.head())

        print(f"\nTotal facturas extraídas: {len(df_cobranza)}")

        df_cobranza.to_csv('facturas_por_cobranza.csv', index=False)
        print("Datos guardados en facturas_por_cobranza.csv")

    except Exception as e:
        print(f"\nError durante la ejecución: {str(e)}")

if __name__ == '__main__':
    main()
