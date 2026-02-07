# Importamos las librerías necesarias
import random
import polars as pl
from datetime import datetime
from faker import Faker
from pathlib import Path

# Función auxiliar (lambda) para obtener la hora exacta del momento.
# Se usará en los 'print' para saber a qué hora ocurrió cada paso (Logs).
ahora = lambda: datetime.now().strftime('%Y-%m-%d %H:%M:%S')

# ---------------------------------------------------------
# 1. CONFIGURACIÓN DE RUTAS Y CARPETAS
# ---------------------------------------------------------

# __file__ es este archivo. .dirname() saca su carpeta. 
# Usamos ".." para subir niveles hacia atrás.
# Objetivo: Guardar los datos en una carpeta 'data_local' separada del código fuente.
BASE_DIR = Path(__file__).resolve().parents[2] / "data_local"

# Definimos la carpeta de destino usando la fecha de hoy.
DT = datetime.now().strftime('%Y-%m-%d')
OUTPUT_DIR = BASE_DIR / DT

# Nombres de los archivos finales
LOG_FILE = OUTPUT_DIR / f"logs_{DT}.log"
IOT_FILE = OUTPUT_DIR / f"iot_{DT}.jsonl"

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
    Ideal para datos IoT.
    """
    with open(ruta_archivo, "ab") as f:
        dataframe.write_ndjson(f)

# ---------------------------------------------------------
# 4. FUNCIÓN PRINCIPAL (GENERADOR)
# ---------------------------------------------------------
def generar_datos():
    
    print(f"[{ahora()}] [INFO]  --> INICIO GENERADOR DE DATOS | FECHA={DT}")
    
    # --- PREPARACIÓN DEL ENTORNO ---
    # Creamos la carpeta física. exist_ok=True evita errores si ya existe.
    # parents=True crea las carpetas intermedias si faltan.
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    # .resolve() nos da la ruta absoluta para imprimirla
    print(f"[{ahora()}] [INFO]  Directorio de salida: {OUTPUT_DIR.resolve()}")
    
    # Limpieza inicial: Si existen archivos de una ejecución anterior, los borramos
    # para empezar de cero y no duplicar datos.
    if LOG_FILE.exists(): LOG_FILE.unlink()
    print(f"[{ahora()}] [INFO]  Limpiando archivo anterior: {LOG_FILE.name}")
    
    if IOT_FILE.exists(): IOT_FILE.unlink()
    print(f"[{ahora()}] [INFO]  Limpiando archivo anterior: {IOT_FILE.name}")
    

    # --- OPTIMIZACIÓN DE DATOS FALSOS ---
    # En lugar de generar un nombre nuevo cada vez (muy lento),
    # creamos una lista fija de 2000 usuarios y 500 sensores.
    # Luego elegiremos aleatoriamente de aquí (mucho más rápido).
    fake = Faker()
    LISTA_USUARIOS = [fake.user_name() for _ in range(2000)]
    LISTA_SENSORES = [f"sensor_{i:04d}" for i in range(500)]
    ACCIONES = ["LOGIN", "LOGOUT", "COMPRA", "ERROR", "CLICK"]
    ESTADOS = ["INFO", "ALERTA", "CRITICO", "DEBUG"]
    
    print(f"[{ahora()}] [INFO]  Comenzando bucle de generación masiva (Batch={LOTE} filas)")

    # --- BUCLE INFINITO DE GENERACIÓN ---
    while True:
        
        # 1. MEDICIÓN
        # Comprobamos cuánto pesan los archivos en este momento.
        # .stat().st_size obtiene el tamaño en bytes del archivo.
        peso_log = LOG_FILE.stat().st_size if LOG_FILE.exists() else 0
        peso_iot = IOT_FILE.stat().st_size if IOT_FILE.exists() else 0
        
        # 2. CONDICIÓN DE SALIDA (META ALCANZADA)
        # Si AMBOS archivos superan el tamaño objetivo, terminamos el bucle.
        if peso_log >= META_BYTES and peso_iot >= META_BYTES:
            print("") 
            print(f"[{ahora()}] [OK]    META ALCANZADA (Logs: {peso_log//1024//1024}MB | IoT: {peso_iot//1024//1024}MB)")
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
        # Calculamos MB para mostrarlo.
        # end='\r' hace que la línea se sobrescriba a sí misma (efecto de carga).
        mb_log = peso_log // 1024 // 1024
        mb_iot = peso_iot // 1024 // 1024
        print(f"[{ahora()}] [PROG]  Generando... Logs: {mb_log} MB / IoT: {mb_iot} MB", end='\r')
        
    print(f"[{ahora()}] [INFO]  --> FIN DEL PROCESO DE GENERACIÓN DE DATOS")

# ---------------------------------------------------------
# PUNTO DE ENTRADA
# ---------------------------------------------------------
if __name__ == "__main__":
    generar_datos()