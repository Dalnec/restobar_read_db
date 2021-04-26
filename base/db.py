import psycopg2
import configparser
import time

config = configparser.ConfigParser()
config.read('config.ini')

db_name = config['BASE']['DB_NAME']
db_user = config['BASE']['DB_USER']
db_pass = config['BASE']['DB_PASS']
db_host = config['BASE']['DB_HOST']
db_port = config['BASE']['DB_PORT']

def _get_time(format):
        timenow = time.localtime()
        if format == 1:
            timenow = time.strftime("%Y/%m/%d %H:%M:%S", timenow)
        else:
            timenow = time.strftime("%H:%M:%S", timenow)
        return timenow

def __conectarse():
    try:
        # nos conectamos a la bd
        cnx = psycopg2.connect(database=db_name, user=db_user,
                                password=db_pass, host=db_host, port=db_port)
        return cnx
    except (Exception, psycopg2.Error) as error:
        print("Error fetching data from PostgreSQL table", error)


def update_venta_pgsql(ext_id, id):
    try:
        cnx = __conectarse()
        cursor = cnx.cursor()
        cursor.execute(
            "UPDATE gulash.ventas SET observaciones_declaracion = %s, estado_declaracion='PROCESADO' WHERE id_venta = %s", (ext_id, id))
        cnx.commit() #Guarda los cambios en la bd
    finally:
        # closing database connection
        if (cnx):
            cursor.close()
            cnx.close()


def read_empresa_pgsql():
    try:
        cnx = __conectarse()
        cursor = cnx.cursor()
        #cursor.execute("SELECT * FROM gulash.parametros WHERE id_parametros in (21, 22)")
        cursor.execute("SELECT * FROM gulash.parametros WHERE descripcion in ('EFACTUR EMPRESA', 'EFACTUR URL')")
        convenio = cursor.fetchall()   
        return convenio
    finally:
        # closing database connection
        if (cnx):
            cursor.close()
            cnx.close()

def update_venta_anulados_pgsql(ext_id, id):
    try:
        cnx = __conectarse()
        cursor = cnx.cursor()
        cursor.execute(
            "UPDATE gulash.ventas SET observaciones_declaracion = %s, estado_declaracion='ANULADO', estado_declaracion_anulado='PROCESADO' WHERE id_venta = %s", (ext_id, id))
        cnx.commit()
    finally:
        # closing database connection
        if (cnx):
            cursor.close()
            cnx.close()

def update_rechazados_pgsql(ext_id, id):
    try:
        cnx = __conectarse()
        cursor = cnx.cursor()
        cursor.execute(
            "UPDATE gulash.ventas SET observaciones_declaracion = %s, estado_declaracion='PENDIENTE', estado_declaracion_anulado='' WHERE id_venta = %s", (ext_id, id))
        cnx.commit()
    finally:
        # closing database connection
        if (cnx):
            cursor.close()
            cnx.close()