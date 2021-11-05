import aiohttp
import requests
import json

from base.db import (read_empresa_pgsql, update_consultar_pgsql, update_no_200, update_retry_anulates, update_venta_anulados_consultado_pgsql, update_venta_pgsql, update_venta_anulados_pgsql, update_rechazados_pgsql, update_resumen_pgsql )
from logger import log
from urllib3.exceptions import InsecureRequestWarning

# Disable flag warning
requests.packages.urllib3.disable_warnings(InsecureRequestWarning)


def create_document(header_dics, estado=None):
    #"crea boletas y facturas"
    convenio = read_empresa_pgsql()
    url = convenio[1][2] + "/api/documents"
    token = convenio[0][2]
    # url = convenio[0][2] + "/api/documents" #para Delivery
    # token = convenio[1][2]
    _send_cpe(url, token, header_dics, estado)


def _send_cpe(url, token, data, estado):
    header = {
        'Content-Type': 'application/json',
        'Authorization': 'Bearer {}'.format(token)
    }
    for venta in data:
        # Manejamos las excepciones
        try:
            # Realizamos la llamada al API de envío de documentos
            res = requests.post(url, json=venta, headers=header)
            # Obtenemos la respuesta y lo decodificamos
            data = ObjJSON(res.content.decode("UTF8")).decoder()
            # Adaptamos la respuesta para guardarlo
            if res.status_code == 200:
                external_id = data['data']['external_id']
                # if estado == 'R': # Es rechazado?
                #     update_rechazados_pgsql(external_id, int(venta['id_venta']))
                # else:
                if venta['codigo_tipo_documento'] == '01':
                    update_venta_pgsql(external_id, int( venta['id_venta']), 'PROCESADO')
                else:
                    update_venta_pgsql(external_id, int( venta['id_venta']), 'POR RESUMIR')

                rest = RespuestaREST(
                    data['success'], "filename:{};estado:{}".format(data['data']['filename'],
                                                                    data['data']['state_type_description']), data)
                log.info(f'{rest.message}')
            else:
                rest = RespuestaREST(False, data['message'], data)
                log.error(f'{rest.message}')
                if (rest.message.find('ya se encuentra registrado') != -1):
                    update_venta_pgsql( '', int(venta['id_venta']), 'PROCESADO')

        except requests.ConnectionError as e:
            log.warning(e)
            rest = RespuestaREST(False, "No se puede establecer una conexión")
            log.warning(f'{rest.message}')
        except requests.ConnectTimeout as e:
            log.warning(e)
            rest = RespuestaREST(False, "Tiempo de espera de conexión agotada")
            log.warning(f'{rest.message}')
        except requests.HTTPError as e:
            log.warning(e)
            rest = RespuestaREST(False, "Ruta de enlace no encontrada")
            log.warning(f'{rest.message}')
        except requests.RequestException as e:
            log.warning(e)
            rest = RespuestaREST(False, "No se puede conectar al servicio")
            log.warning(f'{rest.message}')


def create_document_anulados(header_dics, tipo):
    #"crea facturas y boletas anuladas"
    convenio = read_empresa_pgsql()
    if tipo == 1:
        # url = convenio[0][2] + "/api/voided"
        url = convenio[1][2] + "/api/voided"
    else:
        # url = convenio[0][2] + "/api/summaries"
        url = convenio[1][2] + "/api/summaries"
    # token = convenio[1][2]
    token = convenio[0][2]
    _send_cpe_anulados(url, token, header_dics)


def _send_cpe_anulados(url, token, data):
    header = {
        'Content-Type': 'application/json',
        'Authorization': 'Bearer {}'.format(token)
    }
    for venta in data:
        try:
            res = requests.post(url, json=venta, headers=header)
            data = ObjJSON(res.content.decode("UTF8")).decoder()
            if res.status_code == 200:
                external_id = "{}".format(data['data']) 
                update_venta_anulados_pgsql(external_id, int( venta['id_venta']), 'POR CONSULTAR')
                rest = RespuestaREST(data['success'], "ticket:{} {} {}".format(
                    data['data']['ticket'], venta['fecha_de_emision_de_documentos'], venta['id_venta']), data)
                log.info(f'{rest.message}')
            else:
                rest = RespuestaREST(False, data['message'], data)
                log.error(f'{rest.message}')

        except requests.ConnectionError as e:
            log.warning(e)
            rest = RespuestaREST(False, "No se puede establecer una conexión")
            log.warning(f'{rest.message}')
        except requests.ConnectTimeout as e:
            log.warning(e)
            rest = RespuestaREST(False, "Tiempo de espera de conexión agotada")
            log.warning(f'{rest.message}')
        except requests.HTTPError as e:
            log.warning(e)
            rest = RespuestaREST(False, "Ruta de enlace no encontrada")
            log.warning(f'{rest.message}')
        except requests.RequestException as e:
            log.warning(e)
            rest = RespuestaREST(False, "No se puede conectar al servicio")
            log.warning(f'{rest.message}')


def create_anulados_consultar(header_dics):
    "consulta facturas y boletas anuladas"
    convenio = read_empresa_pgsql()
    # para facturas
    # urlf = convenio[0][2] + "/api/voided/status"
    urlf = convenio[1][2] + "/api/voided/status"
    # para Boletas
    # urlb = convenio[0][2] + "/api/summaries/status"
    urlb = convenio[1][2] + "/api/summaries/status"
    # token = convenio[1][2]
    token = convenio[0][2]
    _send_cpe_anulados_consultar(urlf, urlb, token, header_dics)


def _send_cpe_anulados_consultar(urlf, urlb, token, data):
    header = {
        'Content-Type': 'application/json',
        'Authorization': 'Bearer {}'.format(token)
    }
    for venta in data:
        try:
            datos = venta['data'].replace("\'", "\"")
            if venta['codigo_sunat'] == '01':
                res = requests.post(urlf, json=ObjJSON( datos).decoder(), headers=header)
            else:
                res = requests.post(urlb, json=ObjJSON( datos).decoder(), headers=header)
            # Obtenemos la respuesta y lo decodificamos
            data = ObjJSON(res.content.decode("UTF8")).decoder()
            # Adaptamos la respuesta para guardarlo
            if res.status_code == 200:
                external_id = "{}".format(data['data']['external_id'])
                update_venta_anulados_consultado_pgsql( external_id, int(venta['id_venta']), 'PROCESADO')
                rest = RespuestaREST(data['success'], "Anulado Consulta estado:{} {}".format( data['response']['description'], venta['id_venta']), data)
                log.info(f'{rest.message}')
            else:
                rest = RespuestaREST(False, data['message'], data)
                log.error(f'{rest.message}')

        except requests.ConnectionError as e:
            log.warning(e)
            rest = RespuestaREST(False, "No se puede establecer una conexión")
            log.warning(f'{rest.message}')
        except requests.ConnectTimeout as e:
            log.warning(e)
            rest = RespuestaREST(False, "Tiempo de espera de conexión agotada")
            log.warning(f'{rest.message}')
        except requests.HTTPError as e:
            log.warning(e)
            rest = RespuestaREST(False, "Ruta de enlace no encontrada")
            log.warning(f'{rest.message}')
        except requests.RequestException as e:
            log.warning(e)
            rest = RespuestaREST(False, "No se puede conectar al servicio")
            log.warning(f'{rest.message}')


def create_resumen(formato, lista_resumen):
    convenio = read_empresa_pgsql()
    # url = convenio[0][2] + "/api/summaries"
    # token = convenio[1][2]
    url = convenio[1][2] + "/api/summaries"
    token = convenio[0][2]

    _send_cpe_resumen(url, token, formato, lista_resumen)


def _send_cpe_resumen(url, token, formato, lista_resumen):
    header = {
        'Content-Type': 'application/json',
        'Authorization': 'Bearer {}'.format(token)
    }

    try:
        if lista_resumen:
            res = requests.post(url, json=formato, headers=header)
            data = ObjJSON(res.content.decode("UTF8")).decoder()

            if res.status_code == 200:
                external_id = ObjJSON(data['data']).encoder()
                for venta in lista_resumen:
                    update_resumen_pgsql( external_id, int(venta[0]), 'POR CONSULTAR')

                rest = RespuestaREST(data['success'], "Resumen Enviado ticket:{} {}".format(
                    data['data']['ticket'], formato['fecha_de_emision_de_documentos']), data)
                log.info(f'{rest.message}')
            else:
                rest = RespuestaREST(False, data['message'], data)
                log.error(f'{rest.message}')
                if (rest.message.find('No se encontraron documentos con fecha de emisión') != -1):
                    for venta in lista_resumen:
                        update_resumen_pgsql('', int(venta[0]), 'PROCESADO R')

    except requests.ConnectionError as e:
        log.warning(e)
        rest = RespuestaREST(False, "No se puede establecer una conexión")
        log.warning(f'{rest.message}')
    except requests.ConnectTimeout as e:
        log.warning(e)
        rest = RespuestaREST(False, "Tiempo de espera de conexión agotada")
        log.warning(f'{rest.message}')
    except requests.HTTPError as e:
        log.warning(e)
        rest = RespuestaREST(False, "Ruta de enlace no encontrada")
        log.warning(f'{rest.message}')
    except requests.RequestException as e:
        log.warning(e)
        rest = RespuestaREST(False, "No se puede conectar al servicio")
        log.warning(f'{rest.message}')


def create_consulta(formato, lista_consultar):
    convenio = read_empresa_pgsql()
    url = convenio[1][2] + "/api/summaries/status"
    token = convenio[0][2]
    _send_cpe_consulta(url, token, formato, lista_consultar)


def _send_cpe_consulta(url, token, formato, lista_consultar):
    header = {
        'Content-Type': 'application/json',
        'Authorization': 'Bearer {}'.format(token)
    }
    try:
        if lista_consultar:
            formato = ObjJSON(formato).decoder()
            res = requests.post(url, json=formato, headers=header)
            data = ObjJSON(res.content.decode("UTF8")).decoder()

            if res.status_code == 200:
                external_id = ObjJSON(data['data']).encoder()
                for venta in lista_consultar:
                    if venta[4] == 'I':
                        update_consultar_pgsql( 'ANULADO', external_id, int(venta[0]))
                    else:
                        update_consultar_pgsql( 'PROCESADO', external_id, int(venta[0]))

                rest = RespuestaREST(
                    data['success'], "Consulta estado:{} {}".format(data['response']['description'], venta[1].strftime('%Y-%m-%d')), data)
                log.info(f'{rest.message}')
            else:
                rest = RespuestaREST(False, data['message'], data)
                log.error(f'{rest.message}')
                if (rest.message.find('El ticket no existe') != -1) or (rest.message.find('Description: Internal Error (from server)') != -1):
                    for venta in lista_consultar:
                        update_no_200('PROCESADO C', int(venta[0]))
                        # if venta[4] == 'ANULADO POR RESUMIR':
                        #     update_no_200('ANULADO C', int(venta[0]))
                        # else:
                        #     update_no_200('PROCESADO C', int(venta[0]))

    except requests.ConnectionError as e:
        log.warning(e)
        rest = RespuestaREST(False, "No se puede establecer una conexión")
        log.warning(f'{rest.message}')
    except requests.ConnectTimeout as e:
        log.warning(e)
        rest = RespuestaREST(False, "Tiempo de espera de conexión agotada")
        log.warning(f'{rest.message}')
    except requests.HTTPError as e:
        log.warning(e)
        rest = RespuestaREST(False, "Ruta de enlace no encontrada")
        log.warning(f'{rest.message}')
    except requests.RequestException as e:
        log.warning(e)
        rest = RespuestaREST(False, "No se puede conectar al servicio")
        log.warning(f'{rest.message}')

async def get_cpe_docs(date, lista):

    async with aiohttp.ClientSession() as session:
        convenio = read_empresa_pgsql()
        url = f"{convenio[1][2]}/api/documents/lists/{date}/{date}"
        token = convenio[0][2]
        header = {
            'Content-Type': 'application/json',
            'Authorization': 'Bearer {}'.format(token)
        }
        if lista:
            async with session.get(url, headers=header) as res:
                json_body = await res.json()
                for data in lista:
                    serie = "{}-{}".format(data[2], int(data[3]))
                    filtrado = list(filter(lambda number: number['number'] == serie, json_body['data']))
                    if len(filtrado) > 0 :
                        item = filtrado[0]
                        if item['state_type_id'] == '11': # anulado?
                            log.info(f'Updating States 11, {serie}')
                            update_retry_anulates('ANULADO', 'PROCESADO', item['external_id'], int(data[0]))
                        elif item['state_type_id'] == '05': # aceptado?
                            log.info(f'Updating States 05, {serie}')
                            update_retry_anulates('ANULADO', "", item['external_id'], int(data[0]))
                    else:
                        update_retry_anulates('ANULADO', "", "", int(data[0]))

# Clase para controlar el formato de respuesta
class RespuestaREST:
    def __init__(self, success, message, data=None, anulado=None):
        self.__success = success
        self.message = message
        self.data = data

    def isSuccess(self):
        return self.__success

# Clase para el control del tipo de codificación en JSON
class ObjModelEncoder(json.JSONEncoder):
    def default(self, obj):
        return obj.__dict__

# Clase para la codificación y decodificación de documentos y json
class ObjJSON:
    def __init__(self, obj):
        self.obj = obj

    def encoder(self):
        return json.dumps(self.obj, cls=ObjModelEncoder, indent=4, ensure_ascii=False)

    def decoder(self):
        return json.loads(self.obj)
