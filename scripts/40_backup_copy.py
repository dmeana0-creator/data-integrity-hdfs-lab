# 1. Importamos las librerías necesarias
import subprocess
from datetime import datetime
import time

# ---------------------------------------------------------
# 2. CONFIGURACIÓN GENERAL
# ---------------------------------------------------------

# Calculamos la fecha de hoy
# Esto sirve para buscar la carpeta de datos correspondiente al día actual.
DT = datetime.now().strftime('%Y-%m-%d')

# Lista de carpetas (familias) que queremos copiar.
# El script recorrerá esta lista una por una.
FAMILIAS = ["logs", "iot"]

# Función auxiliar (lambda) para obtener la hora exacta del momento.
# Se usará en los 'print' para saber a qué hora ocurrió cada paso (Logs).
ahora = lambda: datetime.now().strftime('%Y-%m-%d %H:%M:%S')

print(f"[{ahora()}] --> INICIO BACKUP DT={DT}")

# ---------------------------------------------------------
# 3. FUNCIÓN PRINCIPAL DE BACKUP
# ---------------------------------------------------------
def backup():
    inicio = time.time()
    # Iniciamos un bucle: Repetimos el proceso para cada familia ("logs" y "iot")
    for familia in FAMILIAS:
        
        # Construimos las rutas dinámicamente usando la fecha de hoy.
        # src (Source/Origen): Donde están los datos ahora.
        # dst (Destination/Destino): Donde queremos guardarlos.
        src = f"/data/{familia}/raw/dt={DT}"
        dst = f"/backup/{familia}/raw/dt={DT}"

        print(f"\n[{ahora()}] Procesando: {familia.upper()}")
        print(f"Origen:  {src}")
        print(f"Destino: {dst}")

        # INICIO DEL BLOQUE DE SEGURIDAD (TRY - EXCEPT)
        # Intentamos ejecutar los comandos. Si alguno falla, saltamos al 'except'.
        try:
            # --- PASO 1: COPIAR ---
            # Ejecutamos el comando 'hdfs dfs -cp' dentro del contenedor Docker.
            # -f: Sobrescribe si ya existe.
            # check=True: Si el comando falla (da error), Python detiene esto y salta al 'except'.
            # capture_output=True: Guarda el mensaje de respuesta por si necesitamos leerlo.
            subprocess.run(f"docker exec namenode hdfs dfs -cp -f {src}/* {dst}", shell=True, check=True, capture_output=True)
            print(f"[{ahora()}] Copia archivos {familia} OK.")

            # --- PASO 2: VALIDAR ---
            # Verificamos que la carpeta se haya creado realmente en el destino.
            # El comando '-test -e' comprueba si existe una ruta.
            # Si NO existe, devuelve error y Python salta al 'except'.
            subprocess.run(f"docker exec namenode hdfs dfs -test -e {dst}", shell=True, check=True)
            print(f"[{ahora()}] Validación OK: La ruta existe en destino.")

        # --- GESTIÓN DE ERRORES ---
        # Aquí caemos solo si algo salió mal en el bloque 'try' de arriba.
        # subprocess.CalledProcessError:
        # Es la "alarma" específica que salta cuando un comando de Linux falla (devuelve error).
        # La variable 'e' captura toda la información del fallo (qué comando fue, qué error dio, etc.)
        except subprocess.CalledProcessError as e:
            
            # Recuperamos el mensaje de error que nos dio el sistema (Docker/HDFS).
            # .decode() convierte los bytes del sistema en texto legible.
            mensaje = e.stderr.decode() if e.stderr else "Error validando ruta destino"
            
            # Analizamos qué pasó:
            # Si el error dice "No such file", es que hoy no había datos para copiar.
            # Esto es un aviso leve (WARNING), no un error grave.
            if "No such file" in mensaje:
                print(f"[{ahora()}] AVISO: No hay datos para copiar hoy.")
            
            # Si es cualquier otro error (ej: el sistema está caído), es grave (ERROR).
            else:
                print(f"[{ahora()}] ERROR: {mensaje.strip()}")

    fin = time.time()
    duracion = fin - inicio
    
    print(f"\n[METRICAS R7] Tiempo de Copia/Migración: {duracion:.2f} segundos")
    
    print(f"\n[{ahora()}] --> FIN DEL PROCESO")

# ---------------------------------------------------------
# PUNTO DE ENTRADA
# ---------------------------------------------------------
if __name__ == "__main__":
    backup()