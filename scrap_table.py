import requests
from bs4 import BeautifulSoup
import json

URL = "https://ultimosismo.igp.gob.pe/ultimo-sismo/sismos-reportados"

def obtener_10_ultimos_sismos():
    # Headers para simular un navegador real
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
        "Accept-Language": "es-ES,es;q=0.9,en;q=0.8",
        "Accept-Encoding": "gzip, deflate, br",
        "Connection": "keep-alive",
    }
    
    try:
        # Hacer petición GET a la URL
        response = requests.get(URL, headers=headers, timeout=10)
        response.raise_for_status()  # Lanza excepción si hay error HTTP
        
        # Parsear el HTML con BeautifulSoup
        soup = BeautifulSoup(response.text, "html.parser")
        
        # Buscar la tabla
        tabla = soup.find("table")
        if tabla is None:
            raise ValueError("No se encontró ninguna tabla en la página. La estructura del sitio puede haber cambiado.")
        
        tbody = tabla.find("tbody")
        if tbody is None:
            raise ValueError("No se encontró el elemento tbody en la tabla.")

        filas = tbody.find_all("tr")[:10]

        sismos = []
        for tr in filas:
            tds = tr.find_all("td")

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

        return sismos
    
    except requests.exceptions.RequestException as e:
        raise Exception(f"Error al hacer la petición HTTP: {e}")

def lambda_handler(event, context):
    """
    Handler para AWS Lambda
    """
    try:
        sismos = obtener_10_ultimos_sismos()
        return {
            "statusCode": 200,
            "headers": {
                "Content-Type": "application/json",
                "Access-Control-Allow-Origin": "*"
            },
            "body": json.dumps(sismos)
        }
    except Exception as e:
        return {
            "statusCode": 500,
            "headers": {
                "Content-Type": "application/json",
                "Access-Control-Allow-Origin": "*"
            },
            "body": json.dumps({"error": str(e)})
        }

