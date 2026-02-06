# 1. Importamos las librerías necesarias
import subprocess
from datetime import datetime

# ---------------------------------------------------------
# 2. CONFIGURACION Y CONSTANTES
# ---------------------------------------------------------

# Calculamos la fecha de hoy
# Esto define qué carpeta del día vamos a auditar.
DT = datetime.now().strftime('%Y-%m-%d')

# Función auxiliar (lambda) para obtener la hora exacta del momento.
# Se usará en los 'print' para saber a qué hora ocurrió cada paso (Logs).
ahora = lambda: datetime.now().strftime('%Y-%m-%d %H:%M:%S')

# Familias de datos a revisar
FAMILIAS = ["logs", "iot"]

# Ruta donde guardaremos el informe final dentro del clúster HDFS
HDFS_DIR = f"/audit/inventory/dt={DT}"

# ---------------------------------------------------------
# 3. FUNCION AUXILIAR: LECTURA DE METADATOS
# ---------------------------------------------------------
def get_hdfs_files(ruta):
    """
    Conecta con HDFS y obtiene el inventario de una carpeta.
    Devuelve un diccionario: {'archivo.txt': 1024 bytes, 'otro.log': 500 bytes}
    """
    try:
        # Comando HDFS: 'hdfs dfs -stat'
        # %n = Nombre del archivo
        # %b = Tamaño en bytes
        cmd = f'docker exec namenode hdfs dfs -stat "%n %b" {ruta}/*'
        
        # subprocess.check_output: Ejecuta y captura el texto que responde la terminal.
        # text=True: Nos devuelve un string (texto), no bytes.
        out = subprocess.check_output(cmd, shell=True, text=True)
        
        # Convertimos la salida de texto plano en un diccionario Python.
        # splitlines(): Divide por líneas.
        # split(): Divide nombre y tamaño.
        return {line.split()[0]: int(line.split()[1]) for line in out.splitlines()}
        
    except subprocess.CalledProcessError:
        # Si la carpeta no existe (ej: fallo la carga ese día), devolvemos diccionario vacío.
        return {}

# ---------------------------------------------------------
# 4. FUNCION PRINCIPAL: EL INVENTARIO
# ---------------------------------------------------------
def inventory():
    
    print(f"[{ahora()}] [INFO]  --> INICIO AUDITORÍA DE INVENTARIO | FECHA={DT}")
    
    # Creamos una lista para ir guardando las líneas del informe en memoria
    reporte_lines = [f"INVENTARIO {DT}", "-"*20]

    # Bucle Principal: Analizamos cada familia de datos (Logs y IoT)
    for fam in FAMILIAS:
        print(f"[{ahora()}] [INFO]  Analizando familia: {fam.upper()}...")
        
        # Definimos las rutas a comparar
        path_src = f"/data/{fam}/raw/dt={DT}"   # Origen (Datos en crudo)
        path_dst = f"/backup/{fam}/raw/dt={DT}" # Destino (Copia de seguridad)
        
        # Obtenemos los inventarios (Diccionarios con nombre y peso)
        src = get_hdfs_files(path_src)
        dst = get_hdfs_files(path_dst)

        # Añadimos la evidencia cruda al reporte
        reporte_lines.append(f"\n--- EVIDENCIAS {fam.upper()} ---")
        
        for arc, tam in src.items():
            reporte_lines.append(f"ORIGEN:  {path_src}/{arc} ({tam} bytes)")
        for arc, tam in dst.items():
            reporte_lines.append(f"DESTINO: {path_dst}/{arc} ({tam} bytes)")
        
        # --- LOGICA DE COMPARACION ---
        
        # CASO 1: Archivos Perdidos (Missing Files)
        # Usamos Teoría de Conjuntos (Sets).
        # Restar conjuntos (A - B) nos da "lo que está en A pero falta en B".
        missing = set(src.keys()) - set(dst.keys())
        
        # CASO 2: Corrupción de Datos (Bad Size)
        # Si el archivo existe en ambos lados, pero pesan distinto, está corrupto.
        # Recorremos solo los archivos que existen en 'src' y 'dst'.
        bad_size = {f for f in src if f in dst and src[f] != dst[f]}

        # Veredicto
        if not missing and not bad_size:
            msg = f"{fam}: OK (Integridad verificada)"
            print(f"[{ahora()}] [OK]    {msg}")
            reporte_lines.append(msg)
        else:
            msg = f"{fam} ERROR -> Faltan: {missing}, Mal tamaño: {bad_size}"
            print(f"[{ahora()}] [ERROR] Discrepancia detectada en {fam}")
            print(f"                      -> Faltan: {missing}")
            print(f"                      -> Corruptos: {bad_size}")
            reporte_lines.append(msg)


    # --- GUARDAR REPORTE ---
    print(f"[{ahora()}] [INFO]  Generando reporte final...")
    
    # Unimos todas las líneas en un solo bloque de texto
    texto_final = "\n".join(reporte_lines)
    nombre_reporte = f"reporte_inventario_dt={DT}.txt"
    
    # Pasamos directamente el reporte a HDFS desde la memoria RAM.
    
    # -i: Modo interactivo (permite recibir datos).
    # -put - : El guion solo significa "lee de la entrada estándar (stdin)".
    cmd_put = f"docker exec -i namenode hdfs dfs -put -f - {HDFS_DIR}/{nombre_reporte}"
    
    try:
        # input=texto_final: Aquí inyectamos el texto al comando.
        subprocess.run(cmd_put, shell=True, check=True, input=texto_final, text=True)
        
        print(f"[{ahora()}] [OK]    Reporte guardado en HDFS: {HDFS_DIR}/{nombre_reporte}")
        
        print("\n" + "-"*60)
        print(f"[{ahora()}] [INFO]  VISTA PREVIA DEL REPORTE")
        print("-"*(60))
        print(texto_final)
        print("-"*(60))
        
    except subprocess.CalledProcessError:
        print(f"[{ahora()}] [FATAL] No se pudo guardar el reporte en HDFS.")
    
    print(f"[{ahora()}] [INFO]  --> FIN DEL PROCESO")

# ---------------------------------------------------------
# PUNTO DE ENTRADA
# ---------------------------------------------------------
if __name__ == "__main__":
    inventory()