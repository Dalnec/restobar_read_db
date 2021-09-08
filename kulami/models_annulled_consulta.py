import pyodbc
import psycopg2
from base.db import __conectarse

class Venta:
    id_venta = None
    codigo_sunat = None
    external_id = None

    def __str__(self):
        return "{}, {}".format(self.codigo_sunat, self.external_id)

def leer_db_anulados_consultar():
    cnx = __conectarse()
    cursor = cnx.cursor()
    lista_ventas_anulados = []

    sql_header = """
            SELECT                 
                ventas.id_venta,
                documento.codigo_sunat,              
                ventas.observaciones_declaracion
            FROM
                gulash.ventas,
                gulash.documento
            WHERE
                ventas.id_documento = documento.id_documento AND
                ventas.estado_declaracion_anulado = 'POR CONSULTAR' AND
                ventas.estado = 'I' AND
                ventas.estado_declaracion = 'ANULADO' AND
                documento.codigo_sunat in ('01', '03') AND
                ventas.fecha_hora > '2021-07-01'             
            ORDER BY ventas.fecha_hora
        """   
    cursor.execute(sql_header)

    for row in cursor.fetchall():
        venta = Venta()
        
        venta.id_venta = row[0]
        venta.codigo_sunat = row[1]
        venta.external_id = row[2]

        lista_ventas_anulados.append(venta)
    
    cursor.close()
    cnx.close()
    return _generate_lista_anulados_consultar(lista_ventas_anulados)

def _generate_lista_anulados_consultar(ventas_anulados):
    
    header_dics = []
    for venta in ventas_anulados:
        header_dic = {}
        header_dic['id_venta'] = int(venta.id_venta)
        header_dic['codigo_sunat'] = venta.codigo_sunat
        header_dic['data'] = venta.external_id

        header_dics.append(header_dic)
    return header_dics