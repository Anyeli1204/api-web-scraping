import json
import logging
import traceback

try:
    import requests
    from bs4 import BeautifulSoup
except ImportError as e:
    print(f"Error de importación: {e}")

logger = logging.getLogger()
logger.setLevel(logging.INFO)

URL = "https://ultimosismo.igp.gob.pe/ultimo-sismo/sismos-reportados"

def obtener_10_ultimos_sismos():
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
        "Accept-Language": "es-ES,es;q=0.9,en;q=0.8",
        "Connection": "keep-alive",
    }
    
    try:
        response = requests.get(URL, headers=headers, timeout=15)
        response.raise_for_status()
        
        logger.info(f"Status code: {response.status_code}")
        logger.info(f"Content length: {len(response.text)}")
        
        soup = BeautifulSoup(response.text, "html.parser")
        
        tabla = soup.find("table")
        if tabla is None:
            logger.error("No se encontró tabla en el HTML")
            logger.info(f"HTML preview (primeros 500 chars): {response.text[:500]}")
            raise ValueError("No se encontró ninguna tabla en la página. La página puede cargar contenido dinámicamente con JavaScript.")
        
        tbody = tabla.find("tbody")
        if tbody is None:
            raise ValueError("No se encontró el elemento tbody en la tabla.")

        filas = tbody.find_all("tr")[:10]
        
        if len(filas) == 0:
            raise ValueError("No se encontraron filas en la tabla.")

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

        logger.info(f"Se encontraron {len(sismos)} sismos")
        return sismos
    
    except requests.exceptions.RequestException as e:
        logger.error(f"Error en petición HTTP: {str(e)}")
        raise Exception(f"Error al hacer la petición HTTP: {e}")
    except Exception as e:
        logger.error(f"Error inesperado: {str(e)}")
        raise

def lambda_handler(event, context):
    try:
        logger.info("Iniciando scraping de IGP")
        logger.info(f"Event: {json.dumps(event)}")
        
        sismos = obtener_10_ultimos_sismos()
        
        response_body = json.dumps(sismos, ensure_ascii=False)
        
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
    datos = obtener_10_ultimos_sismos()
    for s in datos:
        print(s)
