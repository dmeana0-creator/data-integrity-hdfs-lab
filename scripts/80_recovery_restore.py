# Importamos las librerías necesarias
import subprocess
import time
from datetime import datetime

# Función auxiliar (lambda) para obtener la hora exacta del momento.
# Se usará en los 'print' para saber a qué hora ocurrió cada paso (Logs).
ahora = lambda: datetime.now().strftime('%Y-%m-%d %H:%M:%S')

# ---------------------------------------------------------
# 1. CONFIGURACIÓN DE LA RECUPERACIÓN
# ---------------------------------------------------------
# Definimos los mismos nodos que apagamos en el script anterior (70).
# Es vital que sean los mismos para comprobar si el clúster recupera su estado original.
NODOS = "clustera-dnnm-1 clustera-dnnm-2"

print(f"[{ahora()}] [INFO]  --> INICIO RECUPERACIÓN DE SERVICIO (SELF-HEALING TEST)")

# ---------------------------------------------------------
# 2. RESURRECCIÓN DE LA INFRAESTRUCTURA
# ---------------------------------------------------------
# Usamos 'docker start' para encender de nuevo los contenedores apagados.
print(f"[{ahora()}] [INFO]  1. Ejecutando arranque de emergencia en nodos caídos...")

subprocess.run(f"docker start {NODOS}", shell=True)

# ---------------------------------------------------------
# 3. FASE DE AUTOCURACIÓN (ESPERA PASIVA)
# ---------------------------------------------------------
# Hadoop tarda un tiempo en estabilizarse.
# 1. Los DataNodes arrancan e informan al NameNode.
# 2. El NameNode recibe el "Heartbeat" y ve que están vivos.
# 3. Los DataNodes envían un "Block Report" (lista de datos que tienen).
# 4. El NameNode re-balancea el sistema.
#
# Damos 600 segundos (10 minutos) para asegurar que este proceso termine.
TIEMPO_ESPERA = 600 # 10 minutos
    
print(f"[{ahora()}] [INFO]  2. Iniciando periodo de estabilización...")
print(f"[{ahora()}] [WARN]      Esperando {TIEMPO_ESPERA} segundos ({TIEMPO_ESPERA//60} min) para replicación de bloques.")
print(f"[{ahora()}] [INFO]      (El NameNode está procesando 'Block Reports' y re-balanceando...)")
    
    # Barra de progreso sencilla para no parecer que se ha colgado
    # Dividimos la espera en bloques de 1 minuto para dar feedback
for i in range(TIEMPO_ESPERA // 60):
    time.sleep(60)
    min_restantes = (TIEMPO_ESPERA // 60) - (i + 1)
    print(f"[{ahora()}] [PROG]      ... Sistema recuperándose. Restan {min_restantes} min...")

print(f"[{ahora()}] [OK]    Tiempo de espera finalizado.")

# ---------------------------------------------------------
# 4. VERIFICACIÓN FINAL
# ---------------------------------------------------------
# Volvemos a lanzar el comando FSCK.
# Si la prueba ha sido un éxito, el reporte debería decir "Status: HEALTHY"
# y "Missing blocks: 0".
print("\n" + "-"*60)
print(f"[{ahora()}] [INFO]  3. AUDITORÍA FINAL DE CONFIRMACIÓN (FSCK)")
print("-"*(60))

subprocess.run("python ./30_fsck_data_audit.py", shell=True)

print(f"[{ahora()}] [INFO]  --> FIN DEL PROCESO DE RECUPERACIÓN")