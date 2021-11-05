import pyodbc
import psycopg2
from base.db import __conectarse

class Venta:
    id_venta = None
    fecha_venta = None
    codigo_tipo_proceso = None
    external_id = None
    motivo_anulacion = None

    def __str__(self):
        # ver para que sirve
        return "{}, {}".format(self.fecha_venta, self.codigo_tipo_proceso)


def leer_db_fanulados():
    cnx = __conectarse()
    cursor = cnx.cursor()
    lista_ventas_anulados = []

    sql_header = """
            SELECT                 
                ventas.id_venta,
                ventas.fecha_hora,
                documento.codigo_sunat,              
                ventas.observaciones_declaracion,
                'Error en documento' as motivo_anulacion,
                ventas.codigo_cliente
            FROM
                gulash.ventas,
                gulash.documento
            WHERE
                documento.id_documento = ventas.id_documento AND
                ventas.estado = 'I' AND
                ventas.estado_declaracion_anulado = '' AND
                ventas.estado_declaracion = 'PENDIENTE' AND
                documento.codigo_sunat = '01' AND
                ventas.observaciones_declaracion != ''
            ORDER BY ventas.fecha_hora
        """
    cursor.execute(sql_header)

    for row in cursor.fetchall():
        venta = Venta()

        venta.id_venta = row[0]
        venta.fecha_venta = row[1]
        venta.codigo_tipo_proceso = row[2]
        venta.external_id = row[3]
        venta.motivo_anulacion = row[4]

        lista_ventas_anulados.append(venta)

    cursor.close()
    cnx.close()
    return _generate_lista_anulados(lista_ventas_anulados)


def leer_db_banulados():
    cnx = __conectarse()
    cursor = cnx.cursor()
    lista_ventas_anulados = []

    sql_header = """
            SELECT                 
                ventas.id_venta,
                ventas.fecha_hora,
                documento.codigo_sunat,              
                ventas.external_id,
                'Error en documento' as motivo_anulacion,
                ventas.codigo_cliente
            FROM
                gulash.ventas,
                gulash.documento
            WHERE
                documento.id_documento = ventas.id_documento AND
                ventas.estado = 'I' AND
                ventas.estado_declaracion_anulado = '' AND
                ventas.estado_declaracion = 'ANULADO' AND
                documento.codigo_sunat = '03' AND
                ventas.observaciones_declaracion != ''
            ORDER BY ventas.fecha_hora
        """
    cursor.execute(sql_header)

    for row in cursor.fetchall():
        venta = Venta()

        venta.id_venta = row[0]
        venta.fecha_venta = row[1]
        venta.codigo_tipo_proceso = row[2]
        venta.external_id = row[3]
        venta.motivo_anulacion = row[4]

        lista_ventas_anulados.append(venta)

    cursor.close()
    cnx.close()
    return _generate_lista_anulados(lista_ventas_anulados)


def _generate_lista_anulados(ventas_anulados):

    header_dics = []
    for venta in ventas_anulados:
        header_dic = {}
        # Adicional
        header_dic['id_venta'] = int(venta.id_venta)
        # Creamos el cuerpo del pse
        header_dic['fecha_de_emision_de_documentos'] = venta.fecha_venta.strftime('%Y-%m-%d')
        if int(venta.codigo_tipo_proceso) == 3:
            header_dic['codigo_tipo_proceso'] = "3"
        # documentos verificar [] no los estoy incluyendo
        docs = []
        documents = {}
        documents['external_id'] = venta.external_id
        documents['motivo_anulacion'] = venta.motivo_anulacion
        docs.append(documents)

        header_dic['documentos'] = docs

        header_dics.append(header_dic)
    return header_dics