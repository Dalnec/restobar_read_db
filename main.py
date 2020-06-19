import time
import sys
import configparser

config = configparser.ConfigParser()
config.read('config.ini')
db_state = config['BACKUP']['BU_STATE']
db_time = config['BACKUP']['BU_TIME']

time_doc = config['MAIN']['M_TIMEBF']
time_anul = config['MAIN']['M_TIMEANUL']
time_notaC = config['MAIN']['M_TIMENC']

if __name__ == "__main__":
    sys.path.append('kulami')
    sys.path.append('base')
    sys.path.append('pseapi')
    sys.path.append('backup')

    from kulami.models import leer_db_access
    from kulami.models_annulled import leer_db_fanulados, leer_db_banulados
    from pseapi.api import create_document, create_document_anulados
    from backup.postgresql_backup import backup
    
    while True:
        named_tuple = time.localtime()
        time_string = time.strftime("%H:%M:%S", named_tuple)
        print('Consulta...')
        lista_ventas = leer_db_access()
        create_document(lista_ventas)
        
        time.sleep(1)
        print('Anulados...')
        lista_ventas_anulados = leer_db_fanulados()
        create_document_anulados(lista_ventas_anulados, 1)
        lista_ventas_anulados = leer_db_banulados()
        create_document_anulados(lista_ventas_anulados, 3)

        #el
        if time_string == db_time and db_state:
            print("Backups...")
            backup()
        time.sleep(1) 
