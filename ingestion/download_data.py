from concurrent.futures import ThreadPoolExecutor
from tqdm import tqdm
import requests
import os

def download_data():
    
    url = 'https://s3.opensky-network.org/data-samples/raw/20170109_16_anonymized.avro.bz2'        
    archivo_destino = r"D:\Proyectos_personales\Wide_Sky_flights_lab\20170109_16.avro.bz2"

    # Configuración de velocidad
    NUM_CONEXIONES = 8  
    CHUNK_SIZE = 5242880  

    def descargar_parte(rango):
        inicio, fin, parte_id = rango
        headers = {'Range': f'bytes={inicio}-{fin}'}
        
        response = requests.get(url, headers=headers, stream=True)
        nombre_parte = f"{archivo_destino}.part{parte_id}"
        
        with open(nombre_parte, "wb") as f:
            for chunk in response.iter_content(chunk_size=CHUNK_SIZE):
                if chunk:
                    f.write(chunk)
                    bar.update(len(chunk))

    # 1. Obtener tamaño total
    head_response = requests.head(url)
    total_size = int(head_response.headers.get('content-length', 0))

    # 2. Calcular los rangos de bytes para dividir el archivo
    tamano_parte = total_size // NUM_CONEXIONES
    rangos = []
    for i in range(NUM_CONEXIONES):
        inicio = i * tamano_parte
        fin = total_size - 1 if i == NUM_CONEXIONES - 1 else (inicio + tamano_parte - 1)
        rangos.append((inicio, fin, i))

    # 3. Lanzar la descarga paralela con barra de progreso
    print(f"Iniciando descarga acelerada en {NUM_CONEXIONES} conexiones...")
    with tqdm(
        desc="Descargando",
        total=total_size,
        unit='iB',
        unit_scale=True,
        unit_divisor=1024,
    ) as bar:
        with ThreadPoolExecutor(max_workers=NUM_CONEXIONES) as executor:
            executor.map(descargar_parte, rangos)

    # 4. Unir las partes temporales en tu archivo definitivo de 3 GB
    print("\nUniendo partes en el disco D... No cierres el programa.")
    with open(archivo_destino, "wb") as archivo_final:
        for i in range(NUM_CONEXIONES):
            nombre_parte = f"{archivo_destino}.part{i}"
            with open(nombre_parte, "rb") as parte:
                archivo_final.write(parte.read())
            os.remove(nombre_parte)

    print("¡Descarga completa y archivo unificado con éxito!")
