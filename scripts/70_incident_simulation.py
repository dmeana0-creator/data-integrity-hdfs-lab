# Importamos las librerías necesarias
import subprocess
import time

# Función auxiliar (lambda) para obtener la hora exacta del momento.
# Se usará en los 'print' para saber a qué hora ocurrió cada paso (Logs).
ahora = lambda: datetime.now().strftime('%Y-%m-%d %H:%M:%S')

# ---------------------------------------------------------
# 1. CONFIGURACIÓN DEL ESCENARIO DE FALLO
# ---------------------------------------------------------
# Definimos los datanodes que vamos a tirar: los nombres exactos de los contenedores Docker que vamos a apagar.
NODOS_A_PARAR = "clustera-dnnm-1 clustera-dnnm-2"

print("--- INICIO SIMULACION DE FALLO ---")

# ---------------------------------------------------------
# 2. EJECUCIÓN PARALELA (INGESTA)
# ---------------------------------------------------------
# - subprocess.run: Bloquea el script hasta que el comando termina.
# - subprocess.Popen: Lanza el comando "en segundo plano" y deja que este script continúe inmediatamente.
# Necesitamos esto para poder apagar los servidores MIENTRAS se están escribiendo datos.
print("1. Arrancando ingesta de datos...")
proceso_ingesta = subprocess.Popen("python ./20_ingest_hdfs.py", shell=True)

# ---------------------------------------------------------
# 3. VENTANA DE TIEMPO
# ---------------------------------------------------------
# Esperamos 5 segundos.
# Esto da tiempo a que el script de ingesta conecte con HDFS y empiece a enviar paquetes de datos.
time.sleep(5)

# ---------------------------------------------------------
# 4. EL SABOTAJE (SIMULACIÓN DE CAÍDA)
# ---------------------------------------------------------
# Ejecutamos 'docker stop'. Esto detiene los contenedores abruptamente.
print(f"2. PARANDO NODOS DE GOLPE: {NODOS_A_PARAR}")
subprocess.run(f"docker stop {NODOS_A_PARAR}", shell=True)

# ---------------------------------------------------------
# 5. SINCRONIZACIÓN
# ---------------------------------------------------------
# Ahora usamos .wait().
# Esto le dice a Python: "Espera hasta que el proceso de ingesta termine".
# La ingesta probablemente terminará con errores o timeouts debido al apagón.
print("3. Esperando a que la ingesta termine (o falle)...")
proceso_ingesta.wait()

# ---------------------------------------------------------
# 6. ANÁLISIS FORENSE (AUDITORÍA)
# ---------------------------------------------------------
# Una vez que todo ha terminado, lanzamos la auditoría.
print("4. Auditoría final (Evaluación de daños):")
subprocess.run("python ./30_fsck_data_audit.py", shell=True)