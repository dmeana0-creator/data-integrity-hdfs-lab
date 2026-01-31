# 1. Importamos las librerías necesarias
import os
import random
import polars as pl
from datetime import datetime
from faker import Faker

# 2. Configuramos la ruta para guardar los datos fuera del repositorio
# Obtenemos la ruta de este script y retrocedemos dos carpetas hacia atrás
ruta_script = os.path.dirname(os.path.abspath(__file__))
ruta_fuera_del_repo = os.path.join(ruta_script, "..", "..")
BASE_DIR = os.path.join(ruta_fuera_del_repo, "data_local")

# 3. Creamos una subcarpeta dentro del directorio base con la fecha de hoy
FECHA_HOY = datetime.now().strftime('%Y-%m-%d')
OUTPUT_DIR = f"{BASE_DIR}/{FECHA_HOY}"

# 4. Definimos los nombres y rutas de los dos archivos que vamos a generar (Logs e IoT)
LOG_FILE = f"{OUTPUT_DIR}/logs_{FECHA_HOY}.log"
IOT_FILE = f"{OUTPUT_DIR}/iot_{FECHA_HOY}.jsonl"

# 5. Configuramos el tamaño objetivo y el tamaño del bloque de escritura
# * 1024 * 1024 convierte Megabytes a Bytes
META_BYTES = 512 * 1024 * 1024 # Cnatidad de Megabytes que tendrá cada archivo que generamos
LOTE = 100000 # Cantidad de líneas que se generan y escriben a la vez

# 6. Definimos las funciones auxiliares para escribir los datos en disco
def guardar_como_log(dataframe, ruta_archivo):
    # "ab" (Append Binary) permite añadir datos al final del archivo sin borrar lo anterior
    with open(ruta_archivo, "ab") as f:
        dataframe.write_csv(f, separator="|", include_header=False)

def guardar_como_jsonl(dataframe, ruta_archivo):
    # Guardamos en formato NDJSON (JSON Lines)
    with open(ruta_archivo, "ab") as f:
        dataframe.write_ndjson(f)

# 7. Iniciamos la función principal del script
def generar_datos():
    # 8. Creamos el directorio físico si no existe
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    
    # 9. Limpiamos archivos anteriores para asegurar una ejecución limpia
    if os.path.exists(LOG_FILE): os.remove(LOG_FILE)
    if os.path.exists(IOT_FILE): os.remove(IOT_FILE)
    
    # Imprimimos la ruta absoluta para verificar que está fuera del repositorio
    print(f"--> Generando datos en: {os.path.abspath(OUTPUT_DIR)}")

    # 10. Generamos listas de datos simulados para optimizar la velocidad
    fake = Faker()
    LISTA_USUARIOS = [fake.user_name() for _ in range(2000)]
    LISTA_SENSORES = [f"sensor_{i:04d}" for i in range(500)]
    ACCIONES = ["LOGIN", "LOGOUT", "COMPRA", "ERROR", "CLICK"]
    ESTADOS = ["INFO", "ALERTA", "CRITICO", "DEBUG"]

    # 11. Entramos en el bucle de generación de datos
    while True:
        # 12. Medimos el tamaño actual de los archivos en tiempo real
        peso_log = os.path.getsize(LOG_FILE) if os.path.exists(LOG_FILE) else 0
        peso_iot = os.path.getsize(IOT_FILE) if os.path.exists(IOT_FILE) else 0
        
        # 13. Comprobamos si ambos archivos han llegado a la meta en mi caso 512 MB para detenernos
        if peso_log >= META_BYTES and peso_iot >= META_BYTES:
            print(f"\nArchivos generados correctamente.")
            break

        # 14. Generamos y guardamos un bloque de LOGS (si aún le falta peso)
        if peso_log < META_BYTES:
            df_logs = pl.DataFrame({
                "fecha": [datetime.now().isoformat() for _ in range(LOTE)],
                "usuario": random.choices(LISTA_USUARIOS, k=LOTE),
                "accion": random.choices(ACCIONES, k=LOTE),
                "estado": random.choices(ESTADOS, k=LOTE)
            })
            guardar_como_log(df_logs, LOG_FILE)
            
        # 15. Generamos y guardamos un bloque de IOT (si aún le falta peso)
        if peso_iot < META_BYTES:
            df_iot = pl.DataFrame({
                "fecha": [datetime.now().isoformat() for _ in range(LOTE)],
                "sensor_id": random.choices(LISTA_SENSORES, k=LOTE),
                "metrica": random.choices(["temp", "humedad", "co2"], k=LOTE),
                "valor": [round(random.uniform(10.0, 99.9), 2) for _ in range(LOTE)]
            })
            guardar_como_jsonl(df_iot, IOT_FILE)

        # 16. Mostramos el progreso en pantalla actualizando la misma línea
        mb_actual_log = peso_log // 1024 // 1024
        mb_actual_iot = peso_iot // 1024 // 1024
        print(f"Progreso -> Logs: {mb_actual_log} MB | IoT: {mb_actual_iot} MB", end='\r')

# 17. Punto de entrada: ejecutamos la función main
if __name__ == "__main__":
    generar_datos()