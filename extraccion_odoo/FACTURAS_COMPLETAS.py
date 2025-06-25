import xmlrpc.client
import pandas as pd
import logging
from typing import Dict, List, Optional, Tuple, Any
from dataclasses import dataclass
from datetime import datetime
import json
from pathlib import Path


@dataclass
class OdooConfig:
    """Configuración para la conexión a Odoo"""
    url: str
    database: str
    username: str
    api_key: str
    
    @classmethod
    def from_file(cls, config_path: str) -> 'OdooConfig':
        """Carga configuración desde archivo JSON"""
        with open(config_path, 'r') as f:
            config = json.load(f)
        return cls(**config)


class OdooConnector:
    """Maneja la conexión y autenticación con Odoo"""
    
    def __init__(self, config: OdooConfig):
        self.config = config
        self.uid: Optional[int] = None
        self.models = None
        self._setup_logging()
    
    def _setup_logging(self):
        """Configura el logging"""
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler('odoo_extraction.log'),
                logging.StreamHandler()
            ]
        )
        self.logger = logging.getLogger(__name__)
    
    def connect(self) -> Tuple[int, Any]:
        """Establece conexión con Odoo"""
        try:
            common = xmlrpc.client.ServerProxy(f'{self.config.url}/xmlrpc/2/common')
            self.uid = common.authenticate(
                self.config.database, 
                self.config.username, 
                self.config.api_key, 
                {}
            )
            
            if not self.uid:
                raise ConnectionError('Error de autenticación en Odoo')
            
            self.models = xmlrpc.client.ServerProxy(f'{self.config.url}/xmlrpc/2/object')
            self.logger.info(f"Conexión exitosa a Odoo. UID: {self.uid}")
            return self.uid, self.models
            
        except Exception as e:
            self.logger.error(f"Error conectando a Odoo: {str(e)}")
            raise
    
    def execute_kw(self, model: str, method: str, args: List, kwargs: Dict = None) -> Any:
        """Wrapper para ejecutar métodos en Odoo con manejo de errores"""
        if kwargs is None:
            kwargs = {}
            
        try:
            return self.models.execute_kw(
                self.config.database, 
                self.uid, 
                self.config.api_key,
                model, 
                method, 
                args, 
                kwargs
            )
        except Exception as e:
            self.logger.error(f"Error ejecutando {model}.{method}: {str(e)}")
            raise


class InvoiceExtractor:
    """Extrae datos de facturas desde Odoo"""
    
    INVOICE_FIELDS = [
        'id', 'name', 'move_type', 'invoice_date', 'invoice_date_due', 'partner_id',
        'amount_total', 'amount_residual', 'amount_untaxed', 'amount_untaxed_signed',
        'invoice_origin', 'invoice_payment_term_id', 'currency_id',
        'create_date', 'journal_id', 'l10n_latam_document_type_id', 'partner_shipping_id'
    ]
    
    LINE_FIELDS = [
        'id', 'move_id', 'product_id', 'quantity', 'discount', 
        'price_unit', 'price_subtotal', 'price_total'
    ]
    
    def __init__(self, connector: OdooConnector):
        self.connector = connector
        self.logger = connector.logger
    
    def extract_invoices(self, batch_size: int = 100, max_records: int = 5000) -> pd.DataFrame:
        """Extrae datos principales de las facturas"""
        self.logger.info("Iniciando extracción de facturas...")
        
        domain = [
            ('state', '=', 'posted'), 
            ('move_type', 'in', ['out_invoice', 'out_refund'])
        ]
        
        all_invoices = []
        offset = 0
        
        while len(all_invoices) < max_records:
            try:
                invoices = self.connector.execute_kw(
                    'account.move', 'search_read',
                    [domain],
                    {
                        'fields': self.INVOICE_FIELDS,
                        'limit': batch_size,
                        'offset': offset,
                        'order': 'invoice_date desc'
                    }
                )
                
                if not invoices:
                    break
                
                all_invoices.extend(invoices)
                offset += batch_size
                self.logger.info(f"Extraídas {len(all_invoices)} facturas...")
                
            except Exception as e:
                self.logger.error(f"Error extrayendo facturas en offset {offset}: {str(e)}")
                break
        
        df = pd.DataFrame(all_invoices)
        return self._process_invoice_data(df)
    
    def _process_invoice_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Procesa los datos de facturas extraídos"""
        if df.empty:
            return df
        
        # Procesar campos Many2one
        many2one_fields = {
            'partner_id': ('id_cliente', None),
            'invoice_payment_term_id': ('plazo_pago', 1),
            'currency_id': ('moneda', 1),
            'journal_id': ('diario', 1),
            'l10n_latam_document_type_id': ('tipo_documento', 1),
            'partner_shipping_id': ('id_direccion_entrega', 0)
        }
        
        for field, (new_name, index) in many2one_fields.items():
            if index is None:
                df[new_name] = df[field].apply(
                    lambda v: v[0] if isinstance(v, (list, tuple)) and v else None
                )
            else:
                df[new_name] = df[field].apply(
                    lambda v: v[index] if isinstance(v, (list, tuple)) and v else None
                )
        
        # Campo especial para dirección de entrega
        df['direccion_entrega'] = df['partner_shipping_id'].apply(
            lambda v: v[1] if isinstance(v, (list, tuple)) and v else None
        )
        
        # Añadir información de impuestos
        df = self._add_tax_information(df)
        
        # Renombrar columnas
        column_mapping = {
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
        }
        
        df = df.rename(columns=column_mapping)
        
        # Seleccionar columnas finales
        final_columns = [
            'id_documento', 'folio', 'tipo_documento', 'fecha_emision', 'fecha_vencimiento',
            'id_cliente', 'monto_total', 'base_imponible', 'base_imponible_firmada',
            'monto_pendiente', 'referencia_origen', 'plazo_pago', 'moneda', 
            'fecha_creacion', 'id_direccion_entrega', 'direccion_entrega', 
            'diario', 'impuestos'
        ]
        
        return df[final_columns]
    
    def _add_tax_information(self, df: pd.DataFrame) -> pd.DataFrame:
        """Añade información de impuestos a las facturas"""
        try:
            invoice_ids = df['id'].tolist()
            if not invoice_ids:
                df['impuestos'] = ''
                return df
            
            # Obtener líneas con impuestos
            lines = self.connector.execute_kw(
                'account.move.line', 'search_read',
                [[('move_id', 'in', invoice_ids)]],
                {'fields': ['move_id', 'tax_ids']}
            )
            
            # Mapear impuestos por factura
            taxes_by_invoice = {}
            all_tax_ids = set()
            
            for line in lines:
                move_id = line['move_id'][0] if isinstance(line['move_id'], (list, tuple)) else line['move_id']
                for tax_id in line.get('tax_ids', []):
                    taxes_by_invoice.setdefault(move_id, set()).add(tax_id)
                    all_tax_ids.add(tax_id)
            
            # Obtener nombres de impuestos
            if all_tax_ids:
                tax_data = self.connector.execute_kw(
                    'account.tax', 'read',
                    [list(all_tax_ids)],
                    {'fields': ['id', 'name']}
                )
                tax_map = {t['id']: t['name'] for t in tax_data}
            else:
                tax_map = {}
            
            # Agregar información de impuestos al DataFrame
            df['impuestos'] = df['id'].apply(
                lambda fid: " | ".join([
                    tax_map.get(tid, str(tid)) 
                    for tid in taxes_by_invoice.get(fid, [])
                ])
            )
            
        except Exception as e:
            self.logger.error(f"Error añadiendo información de impuestos: {str(e)}")
            df['impuestos'] = ''
        
        return df
    
    def extract_invoice_lines(self, invoice_ids: List[int]) -> pd.DataFrame:
        """Extrae el detalle de líneas de las facturas"""
        if not invoice_ids:
            return pd.DataFrame()
        
        self.logger.info(f"Extrayendo líneas de {len(invoice_ids)} facturas...")
        
        try:
            lines = self.connector.execute_kw(
                'account.move.line', 'search_read',
                [[('move_id', 'in', invoice_ids), ('exclude_from_invoice_tab', '=', False)]],
                {
                    'fields': self.LINE_FIELDS,
                    'limit': 100000
                }
            )
            
            if not lines:
                return pd.DataFrame()
            
            df = pd.DataFrame(lines)
            return self._process_line_data(df)
            
        except Exception as e:
            self.logger.error(f"Error extrayendo líneas: {str(e)}")
            return pd.DataFrame()
    
    def _process_line_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Procesa los datos de líneas de facturas"""
        if df.empty:
            return pd.DataFrame(columns=['id_documento', 'detalle_lineas'])
        
        # Procesar campos Many2one
        df['id_documento'] = df['move_id'].apply(
            lambda v: v[0] if isinstance(v, (list, tuple)) and v else None
        )
        df['id_producto'] = df['product_id'].apply(
            lambda v: v[0] if isinstance(v, (list, tuple)) and v else None
        )
        df['nombre_producto'] = df['product_id'].apply(
            lambda v: v[1] if isinstance(v, (list, tuple)) and len(v) > 1 else f"Producto {v[0] if isinstance(v, (list, tuple)) else v}"
        )
        
        # Obtener información adicional de productos
        df = self._add_product_info(df)
        
        # Renombrar columnas
        column_mapping = {
            'id': 'id_linea',
            'quantity': 'cantidad',
            'discount': 'descuento',
            'price_unit': 'precio_unitario',
            'price_subtotal': 'subtotal',
            'price_total': 'total'
        }
        
        df = df.rename(columns=column_mapping)
        
        # Filtrar solo líneas con productos (excluir líneas de impuestos, etc.)
        df_productos = df[df['id_producto'].notna() & (df['cantidad'] != 0)]
        
        # Crear resumen detallado de líneas por factura
        summary = df_productos.groupby('id_documento').apply(
            lambda g: " | ".join([
                f"{row['nombre_producto']}: {row['cantidad']:.2f} x ${row['precio_unitario']:.2f} = ${row['total']:.2f}" +
                (f" (Desc: {row['descuento']:.1f}%)" if row['descuento'] > 0 else "")
                for _, row in g.iterrows()
            ])
        ).reset_index(name='detalle_lineas')
        
        return summary
    
    def _add_product_info(self, df: pd.DataFrame) -> pd.DataFrame:
        """Añade información completa de productos incluyendo nombres y costos"""
        try:
            product_ids = df['id_producto'].dropna().unique().tolist()
            if not product_ids:
                df['costo_unit'] = 0.0
                df['codigo_producto'] = ''
                return df
            
            # Obtener información completa de productos
            products_data = self.connector.execute_kw(
                'product.product', 'read',
                [product_ids],
                {'fields': ['id', 'name', 'default_code', 'standard_price', 'list_price']}
            )
            
            # Crear mapas de información
            info_map = {}
            for p in products_data:
                info_map[p['id']] = {
                    'name': p.get('name', f"Producto {p['id']}"),
                    'code': p.get('default_code', ''),
                    'cost': p.get('standard_price', 0.0),
                    'price': p.get('list_price', 0.0)
                }
            
            # Aplicar información al DataFrame
            df['nombre_producto'] = df['id_producto'].apply(
                lambda pid: info_map.get(pid, {}).get('name', f"Producto {pid}") if pd.notna(pid) else ''
            )
            df['codigo_producto'] = df['id_producto'].apply(
                lambda pid: info_map.get(pid, {}).get('code', '') if pd.notna(pid) else ''
            )
            df['costo_unit'] = df['id_producto'].apply(
                lambda pid: info_map.get(pid, {}).get('cost', 0.0) if pd.notna(pid) else 0.0
            )
            
            # Actualizar nombre_producto para incluir código si existe
            df['nombre_producto'] = df.apply(
                lambda row: f"[{row['codigo_producto']}] {row['nombre_producto']}" 
                if row['codigo_producto'] else row['nombre_producto'], 
                axis=1
            )
            
        except Exception as e:
            self.logger.error(f"Error añadiendo información de productos: {str(e)}")
            df['costo_unit'] = 0.0
            df['codigo_producto'] = ''
            # Si no pudimos obtener nombres, usar los del campo product_id original
            if 'nombre_producto' not in df.columns:
                df['nombre_producto'] = df['product_id'].apply(
                    lambda v: v[1] if isinstance(v, (list, tuple)) and len(v) > 1 else f"Producto {v[0] if isinstance(v, (list, tuple)) else v}"
                )
        
        return df
    
    def extract_collection_status(self) -> pd.DataFrame:
        """Extrae el estado de cobranza de las facturas"""
        self.logger.info("Extrayendo estado de cobranza...")
        
        try:
            domain = [
                ('move_type', 'in', ['out_invoice', 'out_refund']),
                ('state', '=', 'posted'),
                ('amount_residual', '>', 0)
            ]
            
            invoice_ids = self.connector.execute_kw(
                'account.move', 'search',
                [domain],
                {'limit': 5000}
            )
            
            if not invoice_ids:
                return pd.DataFrame()
            
            invoices = self.connector.execute_kw(
                'account.move', 'read',
                [invoice_ids],
                {'fields': ['id', 'payment_state', 'amount_residual']}
            )
            
            df = pd.DataFrame(invoices)
            df = df.rename(columns={
                'id': 'id_documento',
                'payment_state': 'estado_pago',
                'amount_residual': 'importe_adeudado'
            })
            
            return df[['id_documento', 'estado_pago', 'importe_adeudado']]
            
        except Exception as e:
            self.logger.error(f"Error extrayendo estado de cobranza: {str(e)}")
            return pd.DataFrame()


class DataProcessor:
    """Procesa y combina los datos extraídos"""
    
    def __init__(self, logger):
        self.logger = logger
    
    def combine_data(self, invoices_df: pd.DataFrame, lines_df: pd.DataFrame, 
                    collection_df: pd.DataFrame) -> pd.DataFrame:
        """Combina todos los DataFrames en uno final"""
        self.logger.info("Combinando datos...")
        
        final_df = invoices_df.copy()
        
        # Combinar con líneas
        if not lines_df.empty:
            final_df = final_df.merge(lines_df, on='id_documento', how='left')
            # Rellenar valores nulos en detalle_lineas
            final_df['detalle_lineas'] = final_df['detalle_lineas'].fillna('Sin detalle disponible')
        else:
            final_df['detalle_lineas'] = 'Sin líneas de producto'
        
        # Combinar con estado de cobranza
        if not collection_df.empty:
            final_df = final_df.merge(collection_df, on='id_documento', how='left')
        else:
            final_df['estado_pago'] = None
            final_df['importe_adeudado'] = None
        
        # Log de estadísticas
        total_facturas = len(final_df)
        con_detalle = len(final_df[final_df['detalle_lineas'] != 'Sin líneas de producto'])
        self.logger.info(f"Total facturas: {total_facturas}, Con detalle: {con_detalle}")
        
        return final_df
    
    def save_to_csv(self, df: pd.DataFrame, filename: str) -> None:
        """Guarda el DataFrame a CSV con manejo de errores"""
        try:
            output_path = Path(filename)
            df.to_csv(output_path, index=False, encoding='utf-8')
            self.logger.info(f"Archivo guardado exitosamente: {output_path.absolute()}")
            self.logger.info(f"Total de registros: {len(df)}")
            
        except Exception as e:
            self.logger.error(f"Error guardando archivo: {str(e)}")
            raise


def main():
    """Función principal"""
    try:
        # Configuración
        config = OdooConfig(
            url='https://movingfood.konos.cl',
            database='movingfood-mfood-erp-main-7481157',
            username='logistica@movingfood.cl',
            api_key='7a1e4e24b1f34abbe7c6fd93fd5fd75dccda90a6'
        )
        
        # Crear instancias
        connector = OdooConnector(config)
        connector.connect()
        
        extractor = InvoiceExtractor(connector)
        processor = DataProcessor(connector.logger)
        
        # Extraer datos
        invoices_df = extractor.extract_invoices()
        
        if invoices_df.empty:
            connector.logger.warning("No se encontraron facturas para procesar")
            return
        
        lines_df = extractor.extract_invoice_lines(invoices_df['id_documento'].tolist())
        collection_df = extractor.extract_collection_status()
        
        # Combinar y guardar
        final_df = processor.combine_data(invoices_df, lines_df, collection_df)
        processor.save_to_csv(final_df, 'facturas_completo.csv')
        
        connector.logger.info("Proceso completado exitosamente")
        
    except Exception as e:
        logging.error(f"Error durante la ejecución: {str(e)}")
        raise


if __name__ == '__main__':
    main()