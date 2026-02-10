import subprocess
import time
import sys
from datetime import datetime

# Detectar comando de python según el sistema (python o python3)
PYTHON_CMD = sys.executable

SCRIPTS = [
    "00_bootstrap.py",
    "10_generate_data.py",
    "20_ingest_hdfs.py",
    "30_fsck_data_audit.py",
    "40_backup_copy.py",
    "50_inventory_compare.py",
    "60_fsck_backup_audit.py",
    "70_incident_simulation.py",
    "80_recovery_restore.py"
]

def main():
    print("--- ESPERANDO A QUE SE DESPLIEGUE CORRECTAMENTE EL SISTEMA HADOOP DOCKERIZADO ---")
    time.sleep(10)
    print("--- INICIANDO PIPELINE COMPLETO ---")
    
    for script in SCRIPTS:
        print(f"\n>>> EJECUTANDO: {script}")
        try:
            # check=True detiene todo si un script falla
            subprocess.run([PYTHON_CMD, f"./{script}"], check=True)
            time.sleep(1) # Pequeña pausa visual
        except subprocess.CalledProcessError:
            print(f"!!! ERROR CRÍTICO EN {script}. DETENIENDO PIPELINE.")
            break
            
    print("\n--- PIPELINE FINALIZADO ---")

if __name__ == "__main__":
    main()