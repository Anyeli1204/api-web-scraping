import json
import uuid

import boto3
import requests
from bs4 import BeautifulSoup

URL = "https://www.igp.gob.pe/servicios/centro-sismologico-nacional/ultimo-sismo/sismos-reportados"
TABLE_NAME = "TablaSismosIGP"


def obtener_10_ultimos_sismos():
    resp = requests.get(URL, timeout=10)
    resp.raise_for_status()

    soup = BeautifulSoup(resp.text, "html.parser")

    tabla = soup.find("table")
    if tabla is None:
        raise ValueError("No se encontró la tabla en la página del IGP.")

    tbody = tabla.find("tbody")
    if tbody is None:
        raise ValueError("No se encontró tbody en la tabla.")

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

        sismos.append(
            {
                "reporte_sismico": reporte_sismico,
                "referencia": referencia,
                "fecha_hora_local": fecha_hora_local,
                "magnitud": magnitud,
                "url_reporte": url_reporte,
            }
        )

    return sismos


def lambda_handler(event, context):
    try:
        sismos = obtener_10_ultimos_sismos()
    except Exception as e:
        return {
            "statusCode": 500,
            "body": json.dumps(
                {"message": "Error haciendo scraping", "error": str(e)},
                ensure_ascii=False,
            ),
        }

    dynamodb = boto3.resource("dynamodb")
    table = dynamodb.Table(TABLE_NAME)

    # Borrar registros anteriores
    scan = table.scan()
    with table.batch_writer() as batch:
        for item in scan.get("Items", []):
            batch.delete_item(Key={"id": item["id"]})

    # Insertar los 10 sismos
    items = []
    with table.batch_writer() as batch:
        for s in sismos:
            item = {
                "id": str(uuid.uuid4()),
                "reporte_sismico": s["reporte_sismico"],
                "referencia": s["referencia"],
                "fecha_hora_local": s["fecha_hora_local"],
                "magnitud": s["magnitud"],
                "url_reporte": s["url_reporte"],
            }
            batch.put_item(Item=item)
            items.append(item)

    return {
        "statusCode": 200,
        "body": json.dumps(items, ensure_ascii=False),
    }
