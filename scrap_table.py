import json
import logging
import traceback
from datetime import datetime

try:
    import requests
    from bs4 import BeautifulSoup
except ImportError as e:
    print(f"Error de importación (requests/beautifulsoup4): {e}")

try:
    import boto3
    from boto3.dynamodb.conditions import Key
except ImportError as e:
    print(f"Error de importación (boto3): {e}")

logger = logging.getLogger()
logger.setLevel(logging.INFO)

TABLE_NAME = "TablaSismosIGP"
URL_IGP = "https://ultimosismo.igp.gob.pe/ultimo-sismo/sismos-reportados"

def obtener_sismos_desde_api():
    """Intenta obtener los sismos desde la API del IGP"""
    año_actual = datetime.now().year
    API_URL = f"https://ultimosismo.igp.gob.pe/api/ultimo-sismo/ajaxb/{año_actual}"
    
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/142.0.0.0 Safari/537.36",
        "Accept": "application/json, text/plain, */*",
        "Accept-Language": "es,es-ES;q=0.9,en;q=0.8",
        "Referer": "https://ultimosismo.igp.gob.pe/ultimo-sismo/sismos-reportados",
        "Connection": "keep-alive",
    }
    
    response = requests.get(API_URL, headers=headers, timeout=15)
    response.raise_for_status()
    
    logger.info(f"API - Status code: {response.status_code}")
    logger.info(f"API - Content-Type: {response.headers.get('Content-Type', 'N/A')}")
    
    if not response.text or response.text.strip() == "":
        raise ValueError("La API devolvió una respuesta vacía")
    
    datos = response.json()
    
    if not datos or not isinstance(datos, list):
        raise ValueError("La API no devolvió datos en el formato esperado")
    
    sismos = []
    for item in datos[:10]:
        fecha_local = item.get("fecha_local", "")
        hora_local = item.get("hora_local", "")
        fecha_hora_local = ""
        if fecha_local and hora_local:
            try:
                fecha = datetime.fromisoformat(fecha_local.replace("Z", "+00:00"))
                hora = datetime.fromisoformat(hora_local.replace("Z", "+00:00"))
                fecha_hora_local = f"{fecha.strftime('%d/%m/%Y')} {hora.strftime('%H:%M:%S')}"
            except:
                fecha_hora_local = f"{fecha_local} {hora_local}"
        
        sismos.append({
            "reporte_sismico": item.get("codigo", ""),
            "referencia": item.get("referencia", ""),
            "fecha_hora_local": fecha_hora_local,
            "magnitud": str(item.get("magnitud", "")),
            "url_reporte": item.get("reporte_acelerometrico_pdf", ""),
        })
    
    logger.info(f"API - Se encontraron {len(sismos)} sismos")
    return sismos

def obtener_sismos_desde_html():
    """Fallback: Intenta obtener los sismos haciendo scraping del HTML"""
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
        "Accept-Language": "es-ES,es;q=0.9,en;q=0.8",
        "Connection": "keep-alive",
    }
    
    response = requests.get(URL_IGP, headers=headers, timeout=15)
    response.raise_for_status()
    
    logger.info(f"HTML - Status code: {response.status_code}")
    logger.info(f"HTML - Content length: {len(response.text)}")
    
    soup = BeautifulSoup(response.text, "html.parser")
    
    tabla = soup.find("table")
    if tabla is None:
        raise ValueError("No se encontró ninguna tabla en el HTML")
    
    tbody = tabla.find("tbody")
    if tbody is None:
        raise ValueError("No se encontró el elemento tbody en la tabla")
    
    filas = tbody.find_all("tr")[:10]
    
    if len(filas) == 0:
        raise ValueError("No se encontraron filas en la tabla")
    
    sismos = []
    for tr in filas:
        tds = tr.find_all("td")
        
        if len(tds) < 5:
            logger.warning(f"Fila con menos de 5 columnas: {len(tds)}")
            continue
        
        reporte_sismico = tds[0].get_text(strip=True)
        referencia = tds[1].get_text(strip=True)
        fecha_hora_local = tds[2].get_text(strip=True)
        magnitud = tds[3].get_text(strip=True)
        
        link_tag = tds[4].find("a")
        url_reporte = link_tag["href"] if link_tag else ""
        
        sismos.append({
            "reporte_sismico": reporte_sismico,
            "referencia": referencia,
            "fecha_hora_local": fecha_hora_local,
            "magnitud": magnitud,
            "url_reporte": url_reporte,
        })
    
    logger.info(f"HTML - Se encontraron {len(sismos)} sismos")
    return sismos

def obtener_10_ultimos_sismos():
    """Obtiene los 10 últimos sismos. Intenta primero con scraping HTML, si falla intenta con la API"""
    metodo_usado = None
    
    # Intentar primero con scraping HTML
    try:
        logger.info("Intentando obtener sismos desde HTML (scraping)...")
        sismos = obtener_sismos_desde_html()
        metodo_usado = "HTML"
        logger.info(f"✓ Éxito obteniendo sismos desde HTML")
        return sismos
    except Exception as e:
        logger.warning(f"✗ Error al obtener sismos desde HTML: {str(e)}")
        logger.info("Intentando fallback: API del IGP...")
    
    # Si el scraping HTML falla, intentar con la API
    try:
        sismos = obtener_sismos_desde_api()
        metodo_usado = "API"
        logger.info(f"✓ Éxito obteniendo sismos desde la API (fallback)")
        return sismos
    except Exception as e:
        logger.error(f"✗ Error al obtener sismos desde la API: {str(e)}")
        raise Exception(f"Error al obtener sismos. HTML falló: {str(e)}. API también falló: {str(e)}")

def guardar_sismos_en_dynamodb(sismos):
    try:
        dynamodb = boto3.resource('dynamodb')
        table = dynamodb.Table(TABLE_NAME)
        
        items_guardados = []
        items_actualizados = []
        
        for sismo in sismos:
            item_id = sismo.get("reporte_sismico", "")
            if not item_id:
                logger.warning("Sismo sin reporte_sismico, saltando...")
                continue
            
            item = {
                "id": item_id,
                "reporte_sismico": sismo.get("reporte_sismico", ""),
                "referencia": sismo.get("referencia", ""),
                "fecha_hora_local": sismo.get("fecha_hora_local", ""),
                "magnitud": sismo.get("magnitud", ""),
                "url_reporte": sismo.get("url_reporte", ""),
                "fecha_actualizacion": datetime.now().isoformat()
            }
            
            try:
                # Intentar obtener el item existente
                response = table.get_item(Key={"id": item_id})
                
                if "Item" in response:
                    # Si existe, actualizar
                    table.put_item(Item=item)
                    items_actualizados.append(item_id)
                    logger.info(f"Sismo {item_id} actualizado en DynamoDB")
                else:
                    # Si no existe, crear nuevo
                    table.put_item(Item=item)
                    items_guardados.append(item_id)
                    logger.info(f"Sismo {item_id} guardado en DynamoDB")
            except Exception as e:
                logger.error(f"Error al guardar sismo {item_id}: {str(e)}")
                raise
        
        return {
            "guardados": items_guardados,
            "actualizados": items_actualizados,
            "total": len(items_guardados) + len(items_actualizados)
        }
    except Exception as e:
        logger.error(f"Error al guardar en DynamoDB: {str(e)}")
        raise Exception(f"Error al guardar en DynamoDB: {e}")

def lambda_handler(event, context):
    try:
        logger.info("Iniciando scraping de IGP")
        logger.info(f"Event: {json.dumps(event)}")
        
        sismos = obtener_10_ultimos_sismos()
        
        resultado_guardado = guardar_sismos_en_dynamodb(sismos)
        
        logger.info(f"Sismos guardados: {resultado_guardado['guardados']}")
        logger.info(f"Sismos actualizados: {resultado_guardado['actualizados']}")
        logger.info(f"Total procesados: {resultado_guardado['total']}")
        
        response_data = {
            "mensaje": f"Web scraping completado. Se procesaron {resultado_guardado['total']} sismos en DynamoDB",
            "sismos_guardados": len(resultado_guardado['guardados']),
            "sismos_actualizados": len(resultado_guardado['actualizados']),
            "sismos": sismos
        }
        
        response_body = json.dumps(response_data, ensure_ascii=False)
        
        return {
            "statusCode": 200,
            "headers": {
                "Content-Type": "application/json",
                "Access-Control-Allow-Origin": "*"
            },
            "body": response_body
        }
    except ValueError as e:
        error_msg = str(e)
        logger.error(f"Error de validación: {error_msg}")
        logger.error(f"Traceback: {traceback.format_exc()}")
        return {
            "statusCode": 500,
            "headers": {
                "Content-Type": "application/json",
                "Access-Control-Allow-Origin": "*"
            },
            "body": json.dumps({
                "error": error_msg,
                "type": "ValueError",
                "message": "Error al obtener los sismos. La página puede requerir JavaScript para cargar el contenido."
            }, ensure_ascii=False)
        }
    except Exception as e:
        error_msg = str(e)
        error_type = type(e).__name__
        error_traceback = traceback.format_exc()
        
        logger.error(f"Error en lambda_handler: {error_msg}")
        logger.error(f"Tipo: {error_type}")
        logger.error(f"Traceback completo: {error_traceback}")
        
        return {
            "statusCode": 500,
            "headers": {
                "Content-Type": "application/json",
                "Access-Control-Allow-Origin": "*"
            },
            "body": json.dumps({
                "error": error_msg,
                "type": error_type,
                "message": "Error interno al procesar la solicitud"
            }, ensure_ascii=False)
        }

if __name__ == "__main__":
    print("Obteniendo los 10 últimos sismos del IGP...")
    datos = obtener_10_ultimos_sismos()
    print(f"\nTotal sismos obtenidos: {len(datos)}")
    print("\n" + "="*60)
    print("DATOS OBTENIDOS:")
    print("="*60)
    for s in datos:
        print(s)
    print("\n" + "="*60)
    print("NOTA: Para guardar en DynamoDB, ejecuta en AWS Lambda")
    print("="*60)
