import pyodbc
import psycopg2
from base.db import __conectarse, get_date_por_resumen_pgsql, get_resumen_por_consultar_pgsql
import time

def _ver_documentos(dia):
    cnx = __conectarse()
    cursor = cnx.cursor()
    sql_header = """
            SELECT              
                V.id_venta,
                V.fecha_hora,
                V.num_serie,
                V.num_documento,
                D.id_documento                
            FROM gulash.ventas AS V, gulash.documento AS D
            WHERE D.id_documento = V.id_documento 
                AND V.estado_declaracion='POR RESUMIR'
                AND V.external_id != '' 
                AND D.codigo_sunat = '03'
                AND (V.fecha_hora >= '{} 00:00:00') AND (V.fecha_hora <= '{} 23:59:00') --fecha obtenida
            ORDER BY V.fecha_hora;
        """
    cursor.execute(sql_header.format(dia, dia))
    lista = cursor.fetchall()
    cursor.close()
    cnx.close()
    return lista

def leer_db_resumen():
    # obtiene la fecha de una boleta por resumir    
    date_resumen = get_date_por_resumen_pgsql()
    if date_resumen:
        # damos formato a fecha obtenida y obtenemos la lista
        # de la fecha obtenida.
        date_resumen = date_resumen[0].strftime('%Y-%m-%d')        
        lista_boletas = _ver_documentos(date_resumen)
        # retornamos formato json y lista de boletas
        return _generate_formato(date_resumen), lista_boletas
    else:
        return [], []

def _generate_formato(date_resumen):
    header_dic = {}         
    header_dic['fecha_de_emision_de_documentos'] = date_resumen
    header_dic['codigo_tipo_proceso'] = '1'
    return header_dic

def _ver_documentos_por_consultar(json):
    cnx = __conectarse()
    cursor = cnx.cursor()
    
    sql_data = """SELECT id_venta,
                    fecha_hora,
                    num_serie,
                    num_documento,
                    estado,
                    estado_declaracion,
                    estado_declaracion_anulado,
                    observaciones_declaracion
                FROM gulash.ventas
                WHERE observaciones_declaracion = '{}' """
    cursor.execute(sql_data.format(json))
    lista_consultar = cursor.fetchall()
    cursor.close()
    cnx.close()
    return lista_consultar

def leer_db_consulta():
    to_consultar = get_resumen_por_consultar_pgsql()
    if to_consultar and to_consultar[1] != '':
        lista_consultar = _ver_documentos_por_consultar(to_consultar[1])
        return to_consultar[1], lista_consultar
    else:
        return [], []

