# Importamos las librerías necesarias
import subprocess
import time
from datetime import datetime
from pathlib import Path 

# Función auxiliar (lambda) para obtener la hora exacta del momento.
# Se usará en los 'print' para saber a qué hora ocurrió cada paso (Logs).
ahora = lambda: datetime.now().strftime('%Y-%m-%d %H:%M:%S')

# ---------------------------------------------------------
# 1. CONFIGURACIÓN Y RUTAS
# ---------------------------------------------------------

# Calculamos la fecha de hoy para sincronizarnos con el generador de datos
DT = datetime.now().strftime("%Y-%m-%d")

# __file__: Es la ruta de este script.
# .resolve(): Obtiene la ruta absoluta completa.
# .parents[2]: Retrocede 3 carpetas hacia atrás (equivale a cd ../../..)
# data_local/DT: Es donde el script anterior dejó los archivos CSV/JSON.
LOCAL_DIR = Path(__file__).resolve().parents[2] / "data_local" / DT

# Mapa de destinos en HDFS (Routing)
# Diccionario que actúa como un semáforo:
# "Si el archivo contiene 'logs_', envíalo a /data/logs/raw..."
DESTINOS = {
    "logs_": f"/data/logs/raw/dt={DT}",
    "iot_":  f"/data/iot/raw/dt={DT}"
}

# ---------------------------------------------------------
# 3. FUNCIÓN PRINCIPAL
# ---------------------------------------------------------
def ingestar():
    
    # Iniciamos el cronómetro para medir el rendimiento (KPIs)
    inicio = time.time()
    
    print(f"[{ahora()}] [INFO]  --> INICIO PROCESO DE INGESTA | FECHA={DT}")

    # --- CHEQUEO DE SEGURIDAD INICIAL ---
    # Antes de intentar nada, verificamos si la carpeta de origen existe.
    # Si no existe, cortamos el programa para evitar errores en cascada.
    if not LOCAL_DIR.exists():
        print(f"[{ahora()}] [WARN]  No se encontraron datos locales en:")
        print(f"                      -> {LOCAL_DIR}")
        print(f"[{ahora()}] [INFO]  Abortando proceso de forma segura.")
        return

    print(f"[{ahora()}] [INFO]  Directorio local detectado: {LOCAL_DIR}")
    
    # --- FASE 1: PROCESAMIENTO Y CARGA ---
    # Iteramos sobre cada elemento dentro de la carpeta local
    for archivo in LOCAL_DIR.iterdir():
        
        # Filtro de Seguridad:
        # iterdir() devuelve todo (archivos y carpetas).
        # Si encontramos una subcarpeta, la ignoramos con 'continue' para no romper el script.
        if not archivo.is_file(): continue

        # Búsqueda de Destino:
        # Analizamos el nombre del archivo para ver si coincide con alguna clave de nuestro diccionario DESTINOS.
        # Si encuentra coincidencia devuelve la ruta, si no, devuelve None.
        destino = next((ruta for clave, ruta in DESTINOS.items() if clave in archivo.name), None)

        if destino:
            print(f"[{ahora()}] [INFO]  Procesando: {archivo.name}")
            
            # BLOQUE 'TRY-EXCEPT': GESTIÓN DE ERRORES
            # Si un archivo falla, capturamos el error aquí para que el script 
            # siga intentándolo con los siguientes archivos.
            try:
                # --- PASO A: EL PUENTE (Windows -> Contenedor) ---
                # Windows no puede hablar directamente con HDFS.
                # Copiamos el archivo físico dentro del contenedor 'namenode' (carpeta /tmp).
                subprocess.run(f'docker cp "{archivo}" namenode:/tmp/{archivo.name}', shell=True, check=True, stderr=subprocess.PIPE)
                
                # --- PASO B: LA INGESTA (Contenedor -> HDFS) ---
                # Una vez el archivo está en Linux (dentro del contenedor), usamos el cliente HDFS nativo.
                # -put: Sube el archivo.
                # -f: Fuerza la sobreescritura si ya existe.
                subprocess.run(f"docker exec namenode hdfs dfs -put -f /tmp/{archivo.name} {destino}/", shell=True, check=True, stderr=subprocess.PIPE)
                
                # --- PASO C: LIMPIEZA (Housekeeping) ---
                # Borramos el archivo temporal de /tmp para no llenar el disco del contenedor de basura.
                # -u 0: Usamos usuario root para asegurar que tenemos permiso de borrarlo.
                subprocess.run(f"docker exec -u 0 namenode rm /tmp/{archivo.name}", shell=True)
                
                print(f"[{ahora()}] [OK]    Carga exitosa -> {destino}")

            except subprocess.CalledProcessError as e:
                # Si algo falla (Docker apagado, red caída, etc.), mostramos el error limpio.
                    mensaje = e.stderr.decode().strip() if e.stderr else "Error desconocido"
                    print(f"[{ahora()}] [ERROR] Falló la ingesta de {archivo.name}")
                    print(f"                      -> Detalles: {mensaje}")

    # --- FASE 2: VERIFICACIÓN Y EVIDENCIAS ---
    print("\n" + "-"*60)
    print(f"[{ahora()}] [INFO]  GENERANDO REPORTE DE EVIDENCIAS (HDFS)")
    print("-"*(60))

    # Recorremos las rutas de destino para preguntar a Hadoop qué ha guardado
    for ruta in DESTINOS.values():
        try:
            print(f"\n[CARPETA] {ruta}")
            
            # Ejecutamos 'du -h' (Disk Usage - Human Readable).
            # Esto nos dice cuánto ocupa la carpeta en MB/GB directamente.
            subprocess.run(f"docker exec namenode hdfs dfs -du -h {ruta}", shell=True, check=True)
            
        except subprocess.CalledProcessError:
             print(f"[{ahora()}] [WARN]  Carpeta vacía o no existe en HDFS.")
        except Exception as e:
             print(f"[{ahora()}] [ERROR] Error calculando métricas: {e}")

    # Cálculo final del tiempo de ejecución
    fin = time.time()
    print("-"*(60))
    print(f"[{ahora()}] [METRICAS] Tiempo Total de Ingestion: {fin - inicio:.2f} segundos")
    
    print(f"[{ahora()}] [INFO]  --> FIN DEL PROCESO DE INGESTA DE DATOS")
    print("="*60 + "\n")
    
# ---------------------------------------------------------
# PUNTO DE ENTRADA
# ---------------------------------------------------------
# Asegura que el script solo corre si se ejecuta directamente
if __name__ == "__main__": 
    ingestar()