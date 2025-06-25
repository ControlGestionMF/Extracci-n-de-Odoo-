import xmlrpc.client

# Datos de conexión
url = 'https://movingfood.konos.cl'
db = 'movingfood-mfood-erp-main-7481157'
username = 'logistica@movingfood.cl'
api_key = '7a1e4e24b1f34abbe7c6fd93fd5fd75dccda90a6'

try:
    print("Conectando al servidor XML-RPC...")
    common = xmlrpc.client.ServerProxy(f'{url}/xmlrpc/2/common')

    version = common.version()
    print("✅ Conexión exitosa. Versión de Odoo:")
    print(version)

    print("\nIntentando autenticar al usuario...")
    uid = common.authenticate(db, username, api_key, {})
    if uid:
        print(f"✅ Autenticación exitosa. UID: {uid}")
    else:
        print("❌ Error de autenticación. Revisa el usuario o la API Key.")
except Exception as e:
    print(f"❌ Error al conectar con el servidor Odoo: {e}")
