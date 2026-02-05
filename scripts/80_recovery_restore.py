import subprocess
import time

# ---------------------------------------------------------
# 1. CONFIGURACIÓN DE LA RECUPERACIÓN
# ---------------------------------------------------------
# Definimos exactamente los mismos nodos que "matamos" en el script anterior (70).
# Es vital que sean los mismos para comprobar si el clúster recupera su estado original.
NODOS = "clustera-dnnm-1 clustera-dnnm-2"

print("--- INICIO RECUPERACIÓN (SELF-HEALING TEST) ---")

# ---------------------------------------------------------
# 2. RESURRECCIÓN DE LA INFRAESTRUCTURA
# ---------------------------------------------------------
# Usamos 'docker start' para encender de nuevo los contenedores apagados.
print(f"1. Resucitando nodos caídos: {NODOS}...")
subprocess.run(f"docker start {NODOS}", shell=True)

# ---------------------------------------------------------
# 3. FASE DE AUTOCURACIÓN (ESPERA PASIVA)
# ---------------------------------------------------------
# ESTO ES CRÍTICO: Hadoop tarda un tiempo en estabilizarse.
# 1. Los DataNodes arrancan e informan al NameNode.
# 2. El NameNode recibe el "Heartbeat" y ve que están vivos.
# 3. Los DataNodes envían un "Block Report" (lista de datos que tienen).
# 4. El NameNode cancela la orden de replicación de emergencia porque ve que todo está bien.
#
# Damos 600 segundos (10 minutos) para asegurar que este proceso termine.
print("2. Esperando 10 minutos a que HDFS se cure solo...")
print("   (El NameNode está recibiendo reportes y re-balanceando bloques...)")
time.sleep(600) 

# ---------------------------------------------------------
# 4. VERIFICACIÓN FINAL
# ---------------------------------------------------------
# Volvemos a lanzar el comando FSCK.
# Si la prueba ha sido un éxito, el reporte debería decir "Status: HEALTHY"
# y "Missing blocks: 0".
print("3. Auditoría de confirmación:")
subprocess.run("python ./30_fsck_data_audit.py", shell=True)