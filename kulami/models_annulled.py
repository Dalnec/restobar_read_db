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
                ventas.observaciones_declaracion,
                '' as motivo_anulacion,
                ventas.codigo_cliente
            FROM
                gulash.ventas,
                gulash.documento
            WHERE
                documento.id_documento = ventas.id_documento AND
                ventas.estado = 'I' AND
                ventas.estado_declaracion_anulado = '' AND
                ventas.estado_declaracion = 'PENDIENTE' AND
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
        header_dic['id_venta'] = venta.id_venta

        # Creamos el cuerpo del pse
        header_dic['fecha_de_emision_de_documentos'] = venta.fecha_venta.strftime('%Y-%m-%d')
        header_dic['codigo_tipo_proceso'] = int(venta.codigo_tipo_proceso)

        # documentos
        documents = {}
        documents['external_id'] = str(venta.external_id)
        documents['motivo_anulacion'] = str(venta.motivo_anulacion)

        header_dic['documentos'] = documents

        header_dics.append(header_dic)
    return header_dics


'''def _generate_lista_prueba():
    from datetime import date
    header_dics = []
    header_dic = {}

    fecha = date.fromisoformat('2019-09-17')

    # Creamos el cuerpo del pse
    header_dic['fecha_de_emision_de_documentos'] = fecha.strftime('%Y-%m-%d')
    #header_dic['codigo_tipo_proceso'] = int('1')

    # documentos
    documents = {}
    documents['external_id'] = str('cdae680d-db68-49ed-916f-f4d9ed467256')
    documents['motivo_anulacion'] = str('Error en documento')

    header_dic['documentos'] = documents

    header_dics.append(header_dic)
    return header_dics'''
