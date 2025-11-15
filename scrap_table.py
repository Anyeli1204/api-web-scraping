from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from bs4 import BeautifulSoup
import time

URL = "https://ultimosismo.igp.gob.pe/ultimo-sismo/sismos-reportados"
TABLE_NAME = "TablaSismosIGP" 

def obtener_10_ultimos_sismos():
    chrome_options = Options()
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36")
    
    try:
        driver = webdriver.Chrome(options=chrome_options)
    except Exception as e:
        raise Exception(f"Error al inicializar Chrome WebDriver. Asegúrate de tener Chrome instalado. Error: {e}")
    
    try:
        driver.get(URL)
        
        wait = WebDriverWait(driver, 10)
        wait.until(EC.presence_of_element_located((By.TAG_NAME, "table")))
        
        time.sleep(2)
        
        html = driver.page_source
        
        soup = BeautifulSoup(html, "html.parser")
        
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
    
    finally:
        driver.quit()
        
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

    scan = table.scan()
    with table.batch_writer() as batch:
        for item in scan.get("Items", []):
            batch.delete_item(Key={"id": item["id"]})

    items = []
    with table.batch_writer() as batch:
        for i, s in enumerate(sismos, start=1):
            item = {
                "id": str(uuid.uuid4()),
                "#": i,
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
