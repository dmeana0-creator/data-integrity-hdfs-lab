# 1. Importamos las librerías necesarias
import os
import sys
import subprocess
from datetime import datetime

# 2. Configuración de Fecha
DT = datetime.now().strftime("%Y-%m-%d")

# 3. Rutas del Sistema Local (Windows)
# Calculamos la ruta absoluta de la carpeta donde están los datos generados por el paso anterior
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
LOCAL_DIR = os.path.join(BASE_DIR, "..", "..", "data_local", DT)

# 4. Mapeo de Destinos en HDFS
# Diccionario que dice: "Si el archivo contiene X texto, va a la carpeta Y en HDFS"
DESTINOS = {
    "logs_": f"/data/logs/raw/dt={DT}",
    "iot_":  f"/data/iot/raw/dt={DT}"
}

# 5. Función Auxiliar
# Esta función nos ahorra escribir "subprocess.run(...)" repetidamente.
# Ejecuta un comando en la terminal y captura el error (stderr) si falla.
def run(comando): 
    subprocess.run(
        comando, 
        shell=True,             # Permite usar comandos de shell complejos
        check=True,             # Lanza una excepción si el comando falla
        stdout=subprocess.DEVNULL, # Oculta la salida normal (para no ensuciar la pantalla)
        stderr=subprocess.PIPE     # Captura el mensaje de error para poder leerlo luego
    )

# 6. Función Principal de Ingesta
def ingestar():
    print(f"--> Iniciando Ingesta: {DT}")

    # 7. Verificación de Seguridad: ¿Docker está vivo?
    # Antes de empezar, comprobamos si Docker está respondiendo para evitar errores feos.
    try:
        run("docker ps")
    except subprocess.CalledProcessError:
        print("ERROR CRÍTICO: Docker no está ejecutándose o no está instalado.")
        return

    # 8. Verificación de Datos Locales
    if not os.path.exists(LOCAL_DIR): 
        print(f"No se encontraron datos locales en: {LOCAL_DIR}")
        return

    # 9. Bucle Principal: Recorremos los archivos de la carpeta local
    for archivo in os.listdir(LOCAL_DIR):
        
        # 10. Búsqueda Inteligente del Destino
        # Buscamos en el diccionario DESTINOS qué ruta coincide con el nombre del archivo
        # Si no hay coincidencia, devuelve None para evitar errores.
        ruta_hdfs = next((ruta for clave, ruta in DESTINOS.items() if clave in archivo), None)
        
        if ruta_hdfs:
            print(f"Subiendo {archivo}...", end=" ")
            
            # 11. El Pipeline de Ingesta
            # Como Windows no puede hablar directo con HDFS por problemas de red/puertos,
            # usamos el contenedor "namenode" como intermediario.
            try:
                
                # Paso A: Copiar archivo de Windows -> Contenedor Namenode (carpeta temporal)
                ruta_local_abs = os.path.join(LOCAL_DIR, archivo)
                run(f'docker cp "{ruta_local_abs}" namenode:/tmp/{archivo}')
                
                # Paso B: Mover archivo del Contenedor -> HDFS (Sistema Distribuido)
                # -f fuerza la sobreescritura si ya existe
                run(f"docker exec namenode hdfs dfs -put -f /tmp/{archivo} {ruta_hdfs}/{archivo}")
                
                # Paso D: Limpieza (Borrar el archivo temporal del contenedor)
                run(f"docker exec -u 0 namenode rm /tmp/{archivo}")
                
                print("OK")

            except subprocess.CalledProcessError as e:
                # Si algo falla, decodificamos el mensaje de error de Docker y lo mostramos
                mensaje_error = e.stderr.strip()
                print(f"FALLO -> {mensaje_error}")

    # 12. Generación de Evidencias
    # Consultamos a HDFS qué ha guardado realmente para verificar el éxito
    print("\n" + "="*40)
    print("EVIDENCIAS EN HDFS (Verificación)")
    print("="*40)

    for ruta in DESTINOS.values():
        try:
            # 13. Obtención de Datos Crudos desde Docker
            # 'check_output' ejecuta el comando y nos devuelve el texto de respuesta
            # -stat %n: Nos da solo los nombres de archivo
            raw_ls = subprocess.check_output(f"docker exec namenode hdfs dfs -stat %n {ruta}/*", shell=True, stderr=subprocess.DEVNULL)
            # -du -s: Nos da el peso total en bytes
            raw_du = subprocess.check_output(f"docker exec namenode hdfs dfs -du -s {ruta}", shell=True, stderr=subprocess.DEVNULL)
            
            # 14. Procesamiento y Formateo (Parsing)
            # Convertimos los bytes de texto a listas y números Python
            lista_archivos = raw_ls.decode('utf-8').strip().split('\n')
            bytes_total = int(raw_du.decode('utf-8').split()[0])
            megabytes = bytes_total / 1024 / 1024
            
            print(f"\nCarpeta: ({ruta})")
            print(f"Archivos: {lista_archivos}")
            print(f"Tamaño Total: {megabytes:.2f} MB")

        except subprocess.CalledProcessError:
            # Si el comando falla (generalmente porque la carpeta está vacía/no existe)
            print(f"\n{ruta}: Carpeta vacía o no encontrada.")
        except Exception as e:
            print(f"\n{ruta}: Error al calcular datos ({e})")

# 15. Punto de Entrada
if __name__ == "__main__": 
    ingestar()