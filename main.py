import asyncio
import time
import sys
import configparser

from pseapi.api import create_consulta

config = configparser.ConfigParser()
config.read('config.ini')
db_state = eval(config['BACKUP']['BU_STATE'])
db_time = config['BACKUP']['BU_TIME']
db_time2 = config['BACKUP']['BU_TIME2']
state_doc = eval(config['MAIN']['M_DOC'])
state_anul = eval(config['MAIN']['M_ANUL'])
state_resumen = eval(config['MAIN']['M_RESUMEN'])
state_retry = eval(config['MAIN']['M_RETRY'])

if __name__ == "__main__":
    sys.path.append('kulami')
    sys.path.append('base')
    sys.path.append('pseapi')
    sys.path.append('backup')

    from kulami.models import leer_db_access
    from kulami.models_annulled import leer_db_fanulados, leer_db_banulados
    from kulami.models_rechazados import leer_db_rechazados
    from kulami.models_annulled_consulta import leer_db_anulados_consultar
    from kulami.models_resumen import leer_db_resumen, leer_db_consulta
    from kulami.models_retry import leer_db_to_retry
    from pseapi.api import create_document, create_document_anulados, create_anulados_consultar, create_resumen, get_cpe_docs
    from backup.postgresql_backup import backup
    from base.db import _get_time
    from logger import log
    
    while True:
        try:
            if state_doc:                
                lista_ventas = leer_db_access()
                create_document(lista_ventas)
        except Exception as e:
            log.error(f'Envio Comprobantes: {e}')
            time.sleep(2)

        try:
            if state_anul:                
                lista_rechazados = leer_db_rechazados()
                create_document(lista_rechazados, 'R')
                time.sleep(1)
        except Exception as e:
            log.error(f'Anulados Rechazados: {e}')
            time.sleep(2)

        try:
            if state_anul:
                lista_ventas_anulados = leer_db_fanulados()
                create_document_anulados(lista_ventas_anulados, 1)
        except Exception as e:
            log.error(f'Anulados Facturas: {e}')
            time.sleep(2)
        try:
            if state_anul:
                lista_ventas_anulados = leer_db_banulados()
                create_document_anulados(lista_ventas_anulados, 3)
        except Exception as e:
            log.error(f'Anulados Boletas: {e}')
            time.sleep(2)
        
        try:
            if state_anul:                
                lista_anulados_consultar = leer_db_anulados_consultar()
                create_anulados_consultar(lista_anulados_consultar)
                time.sleep(1)
        except Exception as e:
            log.error(f'Anulados Consulta: {e}')
            time.sleep(2)
        
        time_now = _get_time(2)
        if  time_now >= db_time and time_now <= db_time2 and db_state:
            try:
                backup()
            except Exception as e:
                log.error(f'Backups: {e}')
                time.sleep(2)
        
        try:
            if state_resumen:
                formato, lista_resumen = leer_db_resumen()
                create_resumen(formato, lista_resumen)
                time.sleep(1)
        except Exception as e:
            log.error(f'Resumen Excepcion: {e}')
            time.sleep(2)

        try:
            if state_resumen:
                formato, lista_consultar = leer_db_consulta()
                create_consulta(formato, lista_consultar)
                time.sleep(1)
        except Exception as e:
            log.error(f'Consulta Excepcion: {e}')
            time.sleep(2)
        
        if state_retry: #time_now >= db_time and time_now <= db_time2 and db_state:
            try:
                date_retry, list_retry  = leer_db_to_retry()
                # asyncio.run(get_cpe_docs(date_retry))
                loop = asyncio.get_event_loop()
                loop.run_until_complete(get_cpe_docs(date_retry, list_retry))
                loop.run_until_complete(asyncio.sleep(0))
                # loop.close()
            except Exception as e:
                log.error(f'Retry: {e}') 
                time.sleep(1)
