# Importamos las librerías necesarias
import subprocess
from datetime import datetime
from pathlib import Path

# Función auxiliar (lambda) para obtener la hora exacta del momento.
# Se usará en los 'print' para saber a qué hora ocurrió cada paso (Logs).
ahora = lambda: datetime.now().strftime('%Y-%m-%d %H:%M:%S')

# ---------------------------------------------------------
# 1. CONFIGURACIÓN DE RUTAS Y ENTORNO
# ---------------------------------------------------------

# Navegación por el árbol de directorios:
# __file__: Este archivo script.
# .resolve(): Ruta absoluta.
# .parent: Carpeta 'scripts'.
# .parent: Carpeta raíz del proyecto.
RAIZ_PROYECTO = Path(__file__).resolve().parent.parent

# Definimos la carpeta compartida entre tu equipo y Docker.
# Guardamos el archivo aquí para que luego, cuando abras Jupyter
# dentro del contenedor, puedas ver este fichero sin tener que copiarlo manualmente.
# Ruta: proyecto/docker/clusterA/notebooks/raw_audits
DIR_COMPARTIDO = RAIZ_PROYECTO / "docker" / "clusterA" / "notebooks" / "raw_audits"

# Creamos la carpeta física si no existe (mkdir -p)
DIR_COMPARTIDO.mkdir(parents=True, exist_ok=True)

# Configuración de fecha y nombres de archivo
DT = datetime.now().strftime('%Y-%m-%d')
NOMBRE_ARCHIVO = f"fsck_backup_dt={DT}.txt"

# Ruta completa en tu disco duro (Windows)
RUTA_LOCAL_FINAL = DIR_COMPARTIDO / NOMBRE_ARCHIVO

# Ruta destino dentro del sistema distribuido (HDFS)
DESTINO_HDFS = f"/audit/fsck/dt={DT}"

# ---------------------------------------------------------
# 2. FUNCION AUXILIAR
# ---------------------------------------------------------
def run_silent(comando):
    """
    Ejecuta comandos silenciando la salida estándar (stdout) 
    pero capturando errores (stderr) por si acaso.
    """
    subprocess.run(comando, shell=True, check=True, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE)

# ---------------------------------------------------------
# 3. FUNCIÓN PRINCIPAL DE AUDITORÍA
# ---------------------------------------------------------
def auditar():
    print(f"[{ahora()}] [INFO]  --> INICIO AUDITORÍA FSCK EN /backup | FECHA={DT}")
    print(f"[{ahora()}] [INFO]  Ruta local de evidencia: {RUTA_LOCAL_FINAL}")

    try:
        # --- PASO 1: EJECUCIÓN DEL DIAGNÓSTICO (FSCK) ---
        # Ejecutamos el comando hdfs fsck
        # check=False Si fsck encuentra errores (corrupción), devuelve código de salida 1.
        # No queremos que el script de Python falle si HDFS está "enfermo", queremos ver el reporte.
        
        fsck = subprocess.run(
            "docker exec namenode hdfs fsck /backup -files -blocks -locations", 
            shell=True, 
            capture_output=True, # Capturamos el texto de respuesta en una variable
            text=True,           # Lo tratamos como texto, no como bytes
            check=False          # No lanzar error si fsck reporta problemas de salud
        )
        
        # Guardamos el texto del reporte en una variable Python
        reporte = fsck.stdout
        print(f"[{ahora()}] [OK]    Diagnóstico finalizado.")
        
        # --- PASO 2: GUARDADO LOCAL (PARA JUPYTER) ---
        # Escribimos el reporte en la carpeta que Jupyter puede ver.
        print(f"[{ahora()}] [INFO]  Guardando reporte localmente...")
        RUTA_LOCAL_FINAL.write_text(reporte, encoding="utf-8")
        print(f"[{ahora()}] [OK]    Reporte fsck de /backup guardado en disco.")
        
        # --- PASO 3: SUBIDA A HDFS ---
        # Subimos el propio reporte de salud a HDFS para tener un histórico.
        print(f"[{ahora()}] [INFO]  Subiendo reporte fsck de /backup a HDFS...")
        
        # A. Copiamos de local -> Contenedor (/tmp)
        run_silent(f'docker cp "{RUTA_LOCAL_FINAL}" namenode:/tmp/{NOMBRE_ARCHIVO}')
        
        # B. Movemos de Contenedor -> HDFS
        # Creamos la carpeta destino primero (mkdir -p) por seguridad
        run_silent(f"docker exec namenode hdfs dfs -mkdir -p {DESTINO_HDFS}")
        run_silent(f"docker exec namenode hdfs dfs -put -f /tmp/{NOMBRE_ARCHIVO} {DESTINO_HDFS}/{NOMBRE_ARCHIVO}")
        
        # C. Limpiamos la basura del contenedor
        # Usamos -u 0 (root) para asegurar permisos de borrado
        run_silent(f"docker exec -u 0 namenode rm /tmp/{NOMBRE_ARCHIVO}")
        
        # --- REPORTE FINAL ---
        print("\n" + "-"*60)
        print(f"[{ahora()}] [OK]    Carga exitosa -> {DESTINO_HDFS}/{NOMBRE_ARCHIVO}")
        print(f"[{ahora()}] [INFO]  RESUMEN DEL REPORTE GENERADO")
        print("-"*(60))
        print(reporte) # mostramos el reporte fsck por pantalla 
        print("-"*(60))
        
        print(f"[{ahora()}] [INFO]  --> FIN DEL PROCESO DE AUDITORÍA SOBRE /backup")

    except Exception as e:
        print(f"[ERROR] Falló la auditoría: {e}")

# ---------------------------------------------------------
# PUNTO DE ENTRADA
# ---------------------------------------------------------
if __name__ == "__main__":
    auditar()