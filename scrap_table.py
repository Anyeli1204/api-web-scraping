import requests
from bs4 import BeautifulSoup
import json

URL = "https://ultimosismo.igp.gob.pe/ultimo-sismo/sismos-reportados"

def obtener_10_ultimos_sismos():
    try:
        # Hacer petición GET a la URL
        response = requests.get(URL, timeout=10)
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
