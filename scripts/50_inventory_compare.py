# 1. Importamos las librerías necesarias
import subprocess
from datetime import datetime

# ---------------------------------------------------------
# 2. CONFIGURACION Y CONSTANTES
# ---------------------------------------------------------

# Calculamos la fecha de hoy (ej: "2026-02-05")
# Esto define qué carpeta del día vamos a auditar.
DT = datetime.now().strftime('%Y-%m-%d')

# Familias de datos a revisar
FAMILIAS = ["logs", "iot"]

# Ruta donde guardaremos el informe final dentro del clúster HDFS
# Notar que usamos una partición de fecha 'dt=...' para mantener el orden.
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
        # Es mucho más rápido que 'ls' para scripts porque podemos formatear la salida.
        # %n = Nombre del archivo
        # %b = Tamaño en bytes
        cmd = f'docker exec namenode hdfs dfs -stat "%n %b" {ruta}/*'
        
        # subprocess.check_output: Ejecuta y captura el texto que responde la terminal.
        # text=True: Nos devuelve un string (texto), no bytes.
        out = subprocess.check_output(cmd, shell=True, text=True)
        
        # Parsing (Procesamiento del texto):
        # Convertimos la salida de texto plano en un diccionario Python útil.
        # splitlines(): Divide por líneas.
        # split(): Divide nombre y tamaño.
        return {line.split()[0]: int(line.split()[1]) for line in out.splitlines()}
        
    except subprocess.CalledProcessError:
        # Si la carpeta no existe (ej: fallo la carga ese día), devolvemos diccionario vacío.
        return {}

# ---------------------------------------------------------
# 4. FUNCION PRINCIPAL: LA AUDITORIA
# ---------------------------------------------------------
def main():
    
    # Creamos una lista para ir guardando las líneas del informe en memoria
    reporte_lines = [f"INVENTARIO {DT}", "-"*20]

    # Bucle Principal: Analizamos cada familia de datos (Logs y IoT)
    for fam in FAMILIAS:
        print(f"Analizando: {fam}...")
        
        # Definimos las rutas a comparar
        path_src = f"/data/{fam}/raw/dt={DT}"   # Origen (Datos "calientes")
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
        
        # --- LOGICA DE COMPARACION (INTEGRIDAD) ---
        
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
            reporte_lines.append(f"{fam}: OK (Integridad verificada)")
        else:
            reporte_lines.append(f"{fam} ERROR -> Faltan: {missing}, Mal tamaño: {bad_size}")

    # --- GUARDAR REPORTE (TRUCO PRO) ---
    
    # Unimos todas las líneas en un solo bloque de texto
    texto_final = "\n".join(reporte_lines)
    
    # Truco de Streaming (Tuberías):
    # En lugar de guardar un archivo .txt en Windows y luego subirlo...
    # ...se lo pasamos directamente a HDFS desde la memoria RAM.
    
    # -i: Modo interactivo (permite recibir datos).
    # -put - : El guion solo significa "lee de la entrada estándar (stdin)".
    cmd_put = f"docker exec -i namenode hdfs dfs -put -f - {HDFS_DIR}/reporte.txt"
    
    try:
        # input=texto_final: Aquí inyectamos el texto al comando.
        subprocess.run(cmd_put, shell=True, check=True, input=texto_final, text=True)
        
        print(f"\n[EXITO] Reporte guardado en HDFS: {HDFS_DIR}/reporte.txt")
        print("--- Vista previa del reporte ---")
        print(texto_final)
        
    except subprocess.CalledProcessError:
        print("[ERROR CRITICO] No se pudo guardar el reporte en HDFS.")

# ---------------------------------------------------------
# PUNTO DE ENTRADA
# ---------------------------------------------------------
if __name__ == "__main__":
    main()