import subprocess
from datetime import datetime
from pathlib import Path

# ---------------------------------------------------------
# 1. CONFIGURACIÓN DE RUTAS Y ENTORNO
# ---------------------------------------------------------

# Navegación inteligente por el árbol de directorios:
# __file__: Este archivo script.
# .resolve(): Ruta absoluta completa.
# .parent: Carpeta 'scripts'.
# .parent: Carpeta raíz del proyecto.
RAIZ_PROYECTO = Path(__file__).resolve().parent.parent

# Definimos la carpeta compartida ("Volumen") entre Windows y Docker.
# IMPORTANTE: Guardamos el archivo aquí para que luego, cuando abras Jupyter
# dentro del contenedor, puedas ver este fichero sin tener que copiarlo manualmente.
# Ruta: proyecto/docker/clusterA/notebooks/raw_audits
DIR_COMPARTIDO = RAIZ_PROYECTO / "docker" / "clusterA" / "notebooks" / "raw_audits"

# Creamos la carpeta física si no existe (mkdir -p)
DIR_COMPARTIDO.mkdir(parents=True, exist_ok=True)

# Configuración de fecha y nombres de archivo
DT = datetime.now().strftime('%Y-%m-%d')
NOMBRE_ARCHIVO = f"fsck_backup_{DT}.txt" # Nombre específico para el backup

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
    print(f"--> Iniciando Auditoría FSCK de BACKUPS: {DT}")
    print(f"    Ruta local compartida: {RUTA_LOCAL_FINAL}")

    try:
        # --- PASO 1: EJECUCIÓN DEL DIAGNÓSTICO (FSCK) ---
        # Ejecutamos 'hdfs fsck' apuntando a la carpeta /backup
        # Objetivo: Verificar que las copias de seguridad no estén corruptas.
        # check=False: IMPORTANTE. Si fsck encuentra bloques corruptos, devuelve error (exit code 1).
        # Usamos False para que Python no se detenga y podamos capturar el reporte del error.
        print("1) Ejecutando análisis fsck en /backup...")
        
        res = subprocess.run(
            "docker exec namenode hdfs fsck /backup -files -blocks -locations", 
            shell=True, 
            capture_output=True, # Capturamos el texto de respuesta
            text=True,           # Lo tratamos como texto
            check=False          # Evitamos que el script explote si hay corrupción
        )
        
        # Guardamos el texto del reporte en una variable Python
        reporte = res.stdout
        
        # --- PASO 2: GUARDADO LOCAL (PARA JUPYTER) ---
        # Escribimos el reporte en la carpeta compartida con el Notebook.
        print(f"2) Guardando evidencia localmente...")
        RUTA_LOCAL_FINAL.write_text(reporte, encoding="utf-8")
        
        # --- PASO 3: SUBIDA A HDFS (PERSISTENCIA) ---
        # Guardamos el reporte dentro de HDFS como evidencia de auditoría (Regulación R4).
        print("3) Subiendo reporte a HDFS (Evidencia)...")
        
        # A. Copiamos de Windows -> Contenedor (/tmp)
        run_silent(f'docker cp "{RUTA_LOCAL_FINAL}" namenode:/tmp/{NOMBRE_ARCHIVO}')
        
        # B. Movemos de Contenedor -> HDFS
        # Creamos la carpeta primero por seguridad
        run_silent(f"docker exec namenode hdfs dfs -mkdir -p {DESTINO_HDFS}")
        run_silent(f"docker exec namenode hdfs dfs -put -f /tmp/{NOMBRE_ARCHIVO} {DESTINO_HDFS}/{NOMBRE_ARCHIVO}")
        
        # C. Limpiamos la basura del contenedor
        # Usamos -u 0 (root) para asegurar permisos de borrado en /tmp
        run_silent(f"docker exec -u 0 namenode rm /tmp/{NOMBRE_ARCHIVO}")

        # --- REPORTE FINAL ---
        print("\n[EXITO] Proceso completado.")
        print(f" - Disponible en Jupyter: notebooks/raw_audits/{NOMBRE_ARCHIVO}")
        print(f" - Disponible en HDFS:    {DESTINO_HDFS}/{NOMBRE_ARCHIVO}")
        
        print("\n" + "="*40)
        print("RESUMEN DEL REPORTE GENERADO:")
        print(reporte) # Imprimimos el resultado en pantalla
        print("="*40)

    except Exception as e:
        print(f"[ERROR] Falló la auditoría: {e}")

# ---------------------------------------------------------
# PUNTO DE ENTRADA
# ---------------------------------------------------------
if __name__ == "__main__":
    auditar()