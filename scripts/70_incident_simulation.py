import subprocess
import time

# ---------------------------------------------------------
# 1. CONFIGURACIÓN DEL ESCENARIO DE FALLO
# ---------------------------------------------------------
# Definimos las "víctimas": los nombres exactos de los contenedores Docker que vamos a apagar.
# Simula que dos servidores del clúster se han quedado sin electricidad de repente.
NODOS_A_PARAR = "clustera-dnnm-1 clustera-dnnm-2"

print("--- INICIO SIMULACION DE FALLO (CHAOS TESTING) ---")

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
# Queremos que el corte ocurra en el momento de máxima actividad.
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
# Esto nos dirá si hemos perdido datos (CORRUPT) o si HDFS ha aguantado el golpe (HEALTHY).
print("4. Auditoría final (Evaluación de daños):")
subprocess.run("python ./30_fsck_data_audit.py", shell=True)