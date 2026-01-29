# 1. Importamos las librerías necesarias
from datetime import datetime
from hdfs import InsecureClient

# 2. Definimos la función principal a través de la cual vamos a crear los directorios
def crear_directorios_hdfs():
    # 3. Configuramos las variables de conexión con nuestro sistema de archivos HDFS
    host = "localhost"
    port = 9870
    user = "hdadmin"
    url = f'http://{host}:{port}'
    
    # 4. Definimos la partición de fecha dinámica (Formato Hive: dt=YYYY-MM-DD)
    dt = datetime.now().strftime('%Y-%m-%d')
    particion = f"dt={dt}"

    # 5. Definimos la lista de rutas base donde queremos crear las particiones
    # El script iterará sobre esta lista añadiendo la fecha al final de cada una
    rutas_base = [
        "/data/logs/raw",          # Datos originales de los Logs
        "/data/iot/raw",           # Datos originales de los sensores IoT
        "/audit/fsck",             # Informes de salud del sistema (fsck)
        "/audit/inventory",        # Listas para comprobar que no faltan archivos
        "/backup/logs/raw",        # Copias de seguridad de los Logs
        "/backup/iot/raw"          # Copias de seguridad del IoT
        
    ]

    # 6. Informamos al usuario por consola que el proceso ha iniciado
    print(f"--> Iniciando proceso para fecha: {dt}")

    # 7. Iniciamos un bloque de control de errores global (para la conexión)
    try:
        # 8. Establecemos la conexión con el cliente HDFS usando las variables definidas
        client = InsecureClient(url, user=user)
        print(f"--> Conectado a HDFS en {url}")

        # 9. Iniciamos un bucle para recorrer cada ruta de la lista definida en el paso 5
        for base_path in rutas_base:
            # 10. Construimos la ruta completa concatenando la carpeta base con la partición de fecha
            full_path = f"{base_path}/{particion}"
            
            # 11. Iniciamos un bloque de control de errores específico para la creación de carpetas
            try:
                # 12. Enviamos la orden de crear el directorio (makedirs crea también las carpetas intermedias)
                client.makedirs(full_path)
                print(f"Creado: {full_path}")
                
            # 13. Capturamos errores específicos de ruta (e_path) para que un fallo no detenga el bucle
            except Exception as e_path:
                print(f"Error creando {full_path}: {e_path}")

    # 14. Capturamos errores fatales de conexión (e_conn) que impiden ejecutar el programa
    except Exception as e_conn:
        print(f"Error fatal de conexión con HDFS: {e_conn}")

# 15. Punto de entrada del script: ejecutamos la función si el archivo se llama directamente
if __name__ == "__main__":
    crear_directorios_hdfs()
