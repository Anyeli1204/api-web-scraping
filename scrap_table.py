import json
import os
from datetime import datetime
from bs4 import BeautifulSoup

def obtener_10_ultimos_sismos():
    """
    Realiza web scraping real de la página del IGP usando Selenium.
    Espera a que JavaScript cargue la tabla y luego extrae los datos del HTML renderizado.
    """
    # Deshabilitar Selenium Manager ANTES de importar selenium
    os.environ['SE_MANAGER'] = 'false'
    os.environ['SELENIUM_MANAGER'] = 'false'
    
    try:
        from selenium import webdriver
        from selenium.webdriver.chrome.options import Options
        from selenium.webdriver.chrome.service import Service
        from selenium.webdriver.common.by import By
        from selenium.webdriver.support.ui import WebDriverWait
        from selenium.webdriver.support import expected_conditions as EC
        from selenium.common.exceptions import TimeoutException
    except ImportError:
        raise Exception("Selenium no está instalado. Ejecuta: pip install selenium")
    
    url = "https://ultimosismo.igp.gob.pe/ultimo-sismo/sismos-reportados"
    
    # Configurar Chrome para Lambda
    chrome_options = Options()
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--window-size=1920,1080")
    chrome_options.add_argument("--disable-blink-features=AutomationControlled")
    chrome_options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
    
    # Para Lambda, Chrome debe estar en /opt/bin/headless-chromium
    # En desarrollo local, usará Chrome del sistema
    driver = None
    
    try:
        # Intentar configuración para Lambda primero
        if os.path.exists("/opt/bin/headless-chromium"):
            # Lambda con Layer de Chrome
            chrome_options.binary_location = "/opt/bin/headless-chromium"
            chrome_options.add_argument("--single-process")
            chrome_options.add_argument("--disable-software-rasterizer")
            
            # Buscar chromedriver en diferentes ubicaciones posibles
            chromedriver_paths = [
                "/opt/bin/chromedriver",
                "/opt/chromedriver",
                "/usr/local/bin/chromedriver"
            ]
            
            chromedriver_path = None
            for path in chromedriver_paths:
                if os.path.exists(path):
                    chromedriver_path = path
                    break
            
            if chromedriver_path:
                # Usar el chromedriver de la capa explícitamente
                service = Service(chromedriver_path)
                driver = webdriver.Chrome(service=service, options=chrome_options)
            else:
                # Si no encontramos chromedriver en la capa, usar chromedriver-binary como respaldo
                try:
                    from chromedriver_binary import chromedriver_filename
                    service = Service(chromedriver_filename)
                    driver = webdriver.Chrome(service=service, options=chrome_options)
                except ImportError:
                    # Si chromedriver-binary no está disponible, buscar en PATH
                    import shutil
                    chromedriver = shutil.which("chromedriver")
                    if chromedriver:
                        service = Service(chromedriver)
                        driver = webdriver.Chrome(service=service, options=chrome_options)
                    else:
                        raise Exception("No se encontró chromedriver en Lambda. Verifica que la capa de Chrome esté correctamente configurada o que chromedriver-binary esté instalado.")
                except Exception as e:
                    raise Exception(f"No se pudo inicializar Chrome con chromedriver-binary. Error: {e}")
        else:
            # Desarrollo local - usar Chrome del sistema
            try:
                driver = webdriver.Chrome(options=chrome_options)
            except Exception as e:
                # Si Chrome no está instalado localmente, usar chromedriver-binary
                try:
                    from chromedriver_binary import chromedriver_filename
                    service = Service(chromedriver_filename)
                    driver = webdriver.Chrome(service=service, options=chrome_options)
                except:
                    raise Exception(f"No se pudo inicializar Chrome. Error: {e}")
        
        # Navegar a la página
        driver.get(url)
        
        # Esperar a que la tabla se cargue (JavaScript ejecuta y llena la tabla)
        wait = WebDriverWait(driver, 30)
        try:
            # Esperar a que aparezca la tabla con datos
            wait.until(EC.presence_of_element_located((By.TAG_NAME, "table")))
            # Esperar un poco más para que los datos se carguen completamente
            wait.until(EC.presence_of_element_located((By.TAG_NAME, "tbody")))
            # Esperar a que haya al menos una fila en la tabla
            wait.until(lambda d: len(d.find_elements(By.CSS_SELECTOR, "tbody tr")) > 0)
        except TimeoutException:
            raise Exception("Timeout: La tabla no se cargó después de esperar 30 segundos")
        
        # Obtener el HTML renderizado (ya con JavaScript ejecutado)
        html = driver.page_source
        
        # Parsear HTML con BeautifulSoup para hacer scraping
        soup = BeautifulSoup(html, "html.parser")
        
        # Buscar la tabla en el HTML renderizado
        tabla = soup.find("table", class_=lambda x: x and "table" in str(x).lower())
        if tabla is None:
            tabla = soup.find("table")
        
        if tabla is None:
            raise ValueError("No se encontró la tabla en el HTML renderizado")
        
        tbody = tabla.find("tbody")
        if tbody is None:
            raise ValueError("No se encontró el tbody en la tabla")
        
        # Extraer datos de la tabla (web scraping real del HTML renderizado)
        filas = tbody.find_all("tr")[:10]
        sismos = []
        
        for tr in filas:
            tds = tr.find_all("td")
            if len(tds) >= 5:
                reporte_sismico = tds[0].get_text(strip=True)
                referencia = tds[1].get_text(strip=True)
                fecha_hora_local = tds[2].get_text(strip=True)
                magnitud = tds[3].get_text(strip=True)
                
                link_tag = tds[4].find("a")
                url_reporte = link_tag["href"] if link_tag and link_tag.get("href") else ""
                
                # Extraer código del reporte (ej: "2025-0111" de "IGP/CENSIS/RS 2025-0111")
                codigo = ""
                if reporte_sismico and "RS" in reporte_sismico:
                    partes = reporte_sismico.split("RS")
                    if len(partes) > 1:
                        codigo = partes[1].strip()
                
                sismos.append({
                    "reporte_sismico": reporte_sismico,
                    "referencia": referencia,
                    "fecha_hora_local": fecha_hora_local,
                    "magnitud": magnitud,
                    "url_reporte": url_reporte,
                    "codigo": codigo,  # Para usar como ID en DynamoDB
                })
        
        if not sismos:
            raise ValueError("No se encontraron sismos en la tabla")
        
        return sismos
    
    finally:
        if driver:
            driver.quit()


def guardar_en_dynamodb(sismos):
    """
    Guarda los sismos en DynamoDB
    """
    import boto3
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table('TablaSismosIGP')
    
    items_guardados = []
    
    for sismo in sismos:
        # Usar código como ID único, o generar uno basado en el reporte
        item_id = sismo.get("codigo", "")
        if not item_id:
            # Si no hay código, generar ID desde reporte_sismico
            reporte = sismo.get("reporte_sismico", "")
            if "RS" in reporte:
                partes = reporte.split("RS")
                item_id = partes[1].strip() if len(partes) > 1 else f"sismo-{datetime.now().timestamp()}"
            else:
                item_id = f"sismo-{datetime.now().timestamp()}"
        
        item = {
            "id": item_id,
            "reporte_sismico": sismo.get("reporte_sismico", ""),
            "referencia": sismo.get("referencia", ""),
            "fecha_hora_local": sismo.get("fecha_hora_local", ""),
            "magnitud": sismo.get("magnitud", ""),
            "url_reporte": sismo.get("url_reporte", ""),
            "timestamp": datetime.now().isoformat(),  # Timestamp de cuando se guardó
        }
        
        try:
            # Guardar en DynamoDB (sobreescribe si ya existe)
            table.put_item(Item=item)
            items_guardados.append(item_id)
        except Exception as e:
            print(f"Error al guardar {item_id}: {str(e)}")
            raise Exception(f"Error al guardar en DynamoDB: {str(e)}")
    
    return items_guardados

def lambda_handler(event, context):
    """
    Handler para AWS Lambda
    Realiza web scraping de los 10 últimos sismos y los guarda en DynamoDB
    """
    try:
        # Obtener los 10 últimos sismos
        sismos = obtener_10_ultimos_sismos()
        
        if not sismos:
            return {
                "statusCode": 404,
                "headers": {
                    "Content-Type": "application/json",
                    "Access-Control-Allow-Origin": "*"
                },
                "body": json.dumps({"error": "No se encontraron sismos"})
            }
        
        # Guardar en DynamoDB
        items_guardados = guardar_en_dynamodb(sismos)
        
        return {
            "statusCode": 200,
            "headers": {
                "Content-Type": "application/json",
                "Access-Control-Allow-Origin": "*"
            },
            "body": json.dumps({
                "mensaje": f"Web scraping completado. Se guardaron {len(items_guardados)} sismos en DynamoDB",
                "sismos_guardados": items_guardados,
                "sismos": sismos
            })
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

if __name__ == "__main__":
    # Prueba local: solo obtener datos (sin guardar en DynamoDB)
    print("Prueba local - Obteniendo los 10 últimos sismos...\n")
    try:
        sismos = obtener_10_ultimos_sismos()
        print(f"Total sismos encontrados: {len(sismos)}")
        print(f"\n{'='*60}")
        print("LOS 10 SISMOS:")
        print(f"{'='*60}")
        for i, sismo in enumerate(sismos, 1):
            print(f"\n{i}. Reporte: {sismo['reporte_sismico']}")
            print(f"   Referencia: {sismo['referencia']}")
            print(f"   Fecha/Hora: {sismo['fecha_hora_local']}")
            print(f"   Magnitud: {sismo['magnitud']}")
            print(f"   URL: {sismo['url_reporte']}")
        
        print(f"\n{'='*60}")
        print("Web scraping completado usando Selenium + BeautifulSoup")
        print("NOTA: Para probar guardado en DynamoDB, ejecuta en AWS Lambda")
        print("IMPORTANTE: En Lambda necesitas agregar una Layer con Chrome Headless")
        print(f"{'='*60}")
    except Exception as e:
        print(f"Error: {e}")
