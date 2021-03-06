import time
import sys
import configparser

config = configparser.ConfigParser()
config.read('config.ini')
db_state = eval(config['BACKUP']['BU_STATE'])
db_time = config['BACKUP']['BU_TIME']
db_time2 = config['BACKUP']['BU_TIME2']
state_doc = eval(config['MAIN']['M_DOC'])
state_anul = eval(config['MAIN']['M_ANUL'])

if __name__ == "__main__":
    sys.path.append('kulami')
    sys.path.append('base')
    sys.path.append('pseapi')
    sys.path.append('backup')

    from kulami.models import leer_db_access
    from kulami.models_annulled import leer_db_fanulados, leer_db_banulados
    from pseapi.api import create_document, create_document_anulados
    from backup.postgresql_backup import backup
    from base.db import _get_time

    while True:
        try:
            if state_doc:
                print('Enviando...')
                lista_ventas = leer_db_access()
                create_document(lista_ventas)
        except Exception as e:
            print(_get_time(1) + ": {}".format(e))
            time.sleep(2)
        try:
            if state_anul:
                print('Anulados Facturas...')
                lista_ventas_anulados = leer_db_fanulados()
                create_document_anulados(lista_ventas_anulados, 1)
        except Exception as e:
            print(_get_time(1) + ": {}".format(e))
            time.sleep(2)
        try:
            if state_anul:
                print('Anulados Boletas...')
                lista_ventas_anulados = leer_db_banulados()
                create_document_anulados(lista_ventas_anulados, 3)
        except Exception as e:
            print(_get_time(1) + ": {}".format(e))
            time.sleep(2)
        time_now = _get_time(2)
        if  time_now >= db_time and time_now <= db_time2 and db_state:
            try:
                backup()
            except Exception as e:
                print(_get_time(1) + ": {}".format(e)) 
                time.sleep(1)
