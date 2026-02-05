import os
import random
import polars as pl     # Librería 'Polars': Es como Pandas pero mucho más rápida (ideal para grandes volúmenes)
from datetime import datetime
from faker import Faker # Librería para inventar nombres, emails y datos falsos realistas

# ---------------------------------------------------------
# 1. CONFIGURACIÓN DE RUTAS Y CARPETAS
# ---------------------------------------------------------

# Truco para trabajar "fuera" del proyecto:
# __file__ es este archivo. .dirname() saca su carpeta. 
# Usamos ".." para subir niveles hacia atrás.
# Objetivo: Guardar los datos en una carpeta 'data_local' separada del código fuente.
ruta_script = os.path.dirname(os.path.abspath(__file__))
ruta_fuera_del_repo = os.path.join(ruta_script, "..", "..")



BASE_DIR = os.path.join(ruta_fuera_del_repo, "data_local")

# Definimos la carpeta de destino usando la fecha de hoy.
# Esto nos ayuda a tener los datos organizados por días (ej: 2026-02-05).
FECHA_HOY = datetime.now().strftime('%Y-%m-%d')
OUTPUT_DIR = f"{BASE_DIR}/{FECHA_HOY}"

# Nombres de los archivos finales
LOG_FILE = f"{OUTPUT_DIR}/logs_{FECHA_HOY}.log"
IOT_FILE = f"{OUTPUT_DIR}/iot_{FECHA_HOY}.jsonl"

# ---------------------------------------------------------
# 2. CONFIGURACIÓN DE TAMAÑO Y RENDIMIENTO
# ---------------------------------------------------------

# META_BYTES: El tamaño objetivo del archivo en Bytes.
# 250 (MB) * 1024 (KB) * 1024 (Bytes)
META_BYTES = 250 * 1024 * 1024 

# LOTE (Batch): Número de líneas que generamos en memoria antes de guardar.
# No guardamos línea a línea (muy lento) ni todo de golpe (se llenaría la RAM).
# Guardamos de 100.000 en 100.000.
LOTE = 100000 

# ---------------------------------------------------------
# 3. FUNCIONES DE ESCRITURA EN DISCO
# ---------------------------------------------------------

def guardar_como_log(dataframe, ruta_archivo):
    """
    Guarda un dataframe en formato CSV separado por tuberías (|).
    Modo 'ab' (Append Binary): Abre el archivo y añade al final sin borrar lo anterior.
    """
    with open(ruta_archivo, "ab") as f:
        dataframe.write_csv(f, separator="|", include_header=False)

def guardar_como_jsonl(dataframe, ruta_archivo):
    """
    Guarda un dataframe en formato JSON Lines (un objeto JSON por línea).
    Ideal para datos IoT o NoSQL.
    """
    with open(ruta_archivo, "ab") as f:
        dataframe.write_ndjson(f)

# ---------------------------------------------------------
# 4. FUNCIÓN PRINCIPAL (GENERADOR)
# ---------------------------------------------------------
def generar_datos():
    
    # --- PREPARACIÓN DEL ENTORNO ---
    # Creamos la carpeta física. exist_ok=True evita errores si ya existe.
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    
    # Limpieza inicial: Si existen archivos de una ejecución anterior, los borramos
    # para empezar de cero y no duplicar datos.
    if os.path.exists(LOG_FILE): os.remove(LOG_FILE)
    if os.path.exists(IOT_FILE): os.remove(IOT_FILE)
    
    print(f"--> Iniciando generación de datos en: {os.path.abspath(OUTPUT_DIR)}")

    # --- OPTIMIZACIÓN DE DATOS FALSOS ---
    # En lugar de generar un nombre nuevo cada vez (muy lento),
    # creamos una lista fija de 2000 usuarios y 500 sensores.
    # Luego elegiremos aleatoriamente de aquí (mucho más rápido).
    fake = Faker()
    LISTA_USUARIOS = [fake.user_name() for _ in range(2000)]
    LISTA_SENSORES = [f"sensor_{i:04d}" for i in range(500)]
    ACCIONES = ["LOGIN", "LOGOUT", "COMPRA", "ERROR", "CLICK"]
    ESTADOS = ["INFO", "ALERTA", "CRITICO", "DEBUG"]

    # --- BUCLE INFINITO DE GENERACIÓN ---
    while True:
        
        # 1. MEDICIÓN
        # Comprobamos cuánto pesan los archivos en este momento.
        peso_log = os.path.getsize(LOG_FILE) if os.path.exists(LOG_FILE) else 0
        peso_iot = os.path.getsize(IOT_FILE) if os.path.exists(IOT_FILE) else 0
        
        # 2. CONDICIÓN DE SALIDA (META ALCANZADA)
        # Si AMBOS archivos superan el tamaño objetivo, terminamos el bucle.
        if peso_log >= META_BYTES and peso_iot >= META_BYTES:
            print(f"\n[OK] Meta alcanzada. Archivos generados correctamente.")
            break

        # 3. GENERACIÓN DE LOGS (CSV)
        # Solo generamos si aún no hemos llegado al peso objetivo
        if peso_log < META_BYTES:
            # Polars crea el DataFrame rapidísimo en memoria
            df_logs = pl.DataFrame({
                "fecha": [datetime.now().isoformat() for _ in range(LOTE)],
                "usuario": random.choices(LISTA_USUARIOS, k=LOTE), # Elige k elementos al azar
                "accion": random.choices(ACCIONES, k=LOTE),
                "estado": random.choices(ESTADOS, k=LOTE)
            })
            # Volcamos el lote al disco
            guardar_como_log(df_logs, LOG_FILE)
            
        # 4. GENERACIÓN DE IOT (JSONL)
        if peso_iot < META_BYTES:
            df_iot = pl.DataFrame({
                "fecha": [datetime.now().isoformat() for _ in range(LOTE)],
                "sensor_id": random.choices(LISTA_SENSORES, k=LOTE),
                "metrica": random.choices(["temp", "humedad", "co2"], k=LOTE),
                "valor": [round(random.uniform(10.0, 99.9), 2) for _ in range(LOTE)]
            })
            guardar_como_jsonl(df_iot, IOT_FILE)

        # 5. FEEDBACK AL USUARIO
        # Calculamos MB para mostrarlo bonito.
        # end='\r' hace que la línea se sobrescriba a sí misma (efecto de carga).
        mb_actual_log = peso_log // 1024 // 1024
        mb_actual_iot = peso_iot // 1024 // 1024
        print(f"Progreso -> Logs: {mb_actual_log} MB | IoT: {mb_actual_iot} MB", end='\r')

# ---------------------------------------------------------
# PUNTO DE ENTRADA
# ---------------------------------------------------------
if __name__ == "__main__":
    generar_datos()