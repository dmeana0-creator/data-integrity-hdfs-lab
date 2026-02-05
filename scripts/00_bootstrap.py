from datetime import datetime
from hdfs import InsecureClient  # Librería específica para conectar Python con Hadoop (HDFS)

# ---------------------------------------------------------
# FUNCIÓN PRINCIPAL DE CREACIÓN DE DIRECTORIOS
# ---------------------------------------------------------
def crear_directorios_hdfs():
    
    # ---------------------------------------------------------
    # 1. CONFIGURACIÓN DE LA CONEXIÓN
    # ---------------------------------------------------------
    # Definimos dónde está "escuchando" nuestro sistema Hadoop.
    # host: La dirección del servidor (localhost porque estamos en la misma máquina).
    # port: 9870 es el puerto web estándar para Hadoop 3.
    # user: El usuario con permisos para crear carpetas.
    host = "localhost"
    port = 9870
    user = "hdadmin"
    url = f'http://{host}:{port}'
    
    # ---------------------------------------------------------
    # 2. CONFIGURACIÓN DE LA FECHA (PARTICIONAMIENTO)
    # ---------------------------------------------------------
    # Calculamos la fecha de hoy.
    # Importante: Usamos el formato 'dt=AAAA-MM-DD'.
    # Esto es un estándar en Big Data (Hive/Spark) para organizar datos por días.
    dt = datetime.now().strftime('%Y-%m-%d')
    particion = f"dt={dt}"

    # ---------------------------------------------------------
    # 3. LISTA DE RUTAS A PROCESAR
    # ---------------------------------------------------------
    # Lista maestra con todas las carpetas donde queremos crear el hueco para hoy.
    # Si quieres añadir un nuevo proceso en el futuro, solo agregas una línea aquí.
    rutas_base = [
        "/data/logs/raw",          # Datos crudos de Logs
        "/data/iot/raw",           # Datos crudos de sensores IoT
        "/audit/fsck",             # Auditorías de salud del sistema
        "/audit/inventory",        # Inventarios de archivos
        "/backup/logs/raw",        # Carpeta de respaldo para Logs
        "/backup/iot/raw"          # Carpeta de respaldo para IoT
    ]

    print(f"--> Iniciando proceso de creación para fecha: {dt}")

    # ---------------------------------------------------------
    # 4. LÓGICA DE CONEXIÓN Y CREACIÓN
    # ---------------------------------------------------------
    
    # BLOQUE DE SEGURIDAD 1 (GLOBAL): LA CONEXIÓN
    # Intentamos conectar con el servidor. Si falla aquí, nada funcionará.
    try:
        # Creamos el cliente (el "mando a distancia" para manejar HDFS).
        client = InsecureClient(url, user=user)
        print(f"--> Conexión establecida con HDFS en {url}")

        # BUCLE: Recorremos la lista de rutas una por una
        for base_path in rutas_base:
            
            # Construimos la ruta final uniendo la base + la fecha.
            # Ejemplo: /data/logs/raw/dt=2023-10-25
            full_path = f"{base_path}/{particion}"
            
            # BLOQUE DE SEGURIDAD 2 (ESPECÍFICO): LA CARPETA
            # Si falla una carpeta concreta, capturamos el error aquí dentro
            # para que el bucle continúe y cree las demás.
            try:
                # client.makedirs: Equivalente a 'mkdir -p'.
                # Crea la carpeta y, si no existen las superiores, las crea también.
                client.makedirs(full_path)
                print(f"[OK] Creado: {full_path}")
                
            except Exception as e_path:
                # Si falla solo esta carpeta, avisamos pero NO paramos el programa.
                print(f"[ERROR] Falló al crear {full_path}: {e_path}")

    # GESTIÓN DE ERROR FATAL
    # Aquí caemos si falló el BLOQUE 1 (ej: el servidor Hadoop está apagado).
    except Exception as e_conn:
        print(f"--> ERROR FATAL: No se pudo conectar con HDFS. Detalles: {e_conn}")

# ---------------------------------------------------------
# PUNTO DE ENTRADA
# ---------------------------------------------------------
# Asegura que este script se ejecute solo si lo llamamos directamente.
if __name__ == "__main__":
    crear_directorios_hdfs()