# Importamos las librerías necesarias
import subprocess
import time
from datetime import datetime

# Función auxiliar (lambda) para obtener la hora exacta del momento.
# Se usará en los 'print' para saber a qué hora ocurrió cada paso (Logs).
ahora = lambda: datetime.now().strftime('%Y-%m-%d %H:%M:%S')

# ---------------------------------------------------------
# 1. CONFIGURACIÓN DEL ESCENARIO DE FALLO
# ---------------------------------------------------------
# Definimos los datanodes que vamos a tirar: los nombres exactos de los contenedores Docker que vamos a apagar.
NODOS_A_PARAR = "clustera-dnnm-1 clustera-dnnm-2"

print(f"[{ahora()}] [INFO]  --> INICIO SIMULACION DE FALLO")

# ---------------------------------------------------------
# 2. EJECUCIÓN PARALELA (INGESTA)
# ---------------------------------------------------------
# - subprocess.run: Bloquea el script hasta que el comando termina.
# - subprocess.Popen: Lanza el comando "en segundo plano" y deja que este script continúe inmediatamente.
# Necesitamos esto para poder apagar los servidores MIENTRAS se están escribiendo datos.
print(f"[{ahora()}] [INFO]  1. Arrancando ingesta de datos en segundo plano...")
proceso_ingesta = subprocess.Popen("python ./20_ingest_hdfs.py", shell=True)

# ---------------------------------------------------------
# 3. VENTANA DE TIEMPO
# ---------------------------------------------------------
# Esperamos 5 segundos.
# Esto da tiempo a que el script de ingesta conecte con HDFS y empiece a enviar paquetes de datos.
print(f"[{ahora()}] [INFO]     Esperando 5s a que empiece el flujo de datos...")
time.sleep(5)

# ---------------------------------------------------------
# 4. EL SABOTAJE (SIMULACIÓN DE CAÍDA)
# ---------------------------------------------------------
# Ejecutamos 'docker stop'. Esto detiene los contenedores seleccionados anteriormente.
print(f"[{ahora()}] [WARN]  2. EJECUTANDO SABOTAJE: APAGANDO NODOS")
print(f"[{ahora()}] [WARN]      Objetivos: {NODOS_A_PARAR}")

subprocess.run(f"docker stop {NODOS_A_PARAR}", shell=True)

print(f"[{ahora()}] [WARN]      Nodos detenidos.")

# ---------------------------------------------------------
# 5. SINCRONIZACIÓN
# ---------------------------------------------------------
# Ahora usamos .wait().
# Esto le dice a Python: "Espera hasta que el proceso de ingesta termine".
# La ingesta probablemente terminará con errores o timeouts debido al apagón.
print(f"[{ahora()}] [INFO]  3. Esperando reacción del script de ingesta...")
proceso_ingesta.wait()

# ---------------------------------------------------------
# 6. ANÁLISIS FORENSE (AUDITORÍA)
# ---------------------------------------------------------
# Una vez que todo ha terminado, lanzamos la auditoría.
print("\n" + "-"*60)
print(f"[{ahora()}] [INFO]  4. INICIANDO AUDITORÍA FSCK")
print("-"*(60))

subprocess.run("python ./30_fsck_data_audit.py", shell=True)

print(f"[{ahora()}] [INFO]  --> FIN DE LA SIMULACIÓN DE FALLO")