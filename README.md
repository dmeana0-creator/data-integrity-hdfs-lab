# DataSecure Lab — Integridad de Datos en Big Data (HDFS)
**Versión**: `v1.0-entrega` **Contexto** Proyecto de ingenieria de datos para la empresa ficticia Datasecure.

Este repositorio contiene la implementación de un pipeline de ingesta y preservación digital para la gestión, auditoría, simulación de incidentes y recuperación de datos sensibles (Logs y Telemetría IoT) en un ecosistema Hadoop dockerizado.

El objetivo principal es garantizar la integridad de los datos y validar la resiliencia de la infraestructura frente a fallos críticos, demostrando capacidades de Auto-recuperación).

---
## Estructura del repositorio

```text
data-integrity-hdfs-lab/
├── docker/
│   └── clusterA/
│       ├── notebooks/                        # Directorio montado en NameNode (/media/notebooks)
│       │   └── 02_auditoria_integridad.ipynb # Notebook de análisis de auditorías y métricas en Jupyter dentro del Namenode
│       └── docker-compose.yml                # Definición de infraestructura (NameNode + YARN + DataNodes)
├── docs/                                     # Documentación (Enunciado, rúbrica, evidencias)
├── env/                                      # Entorno virtual de Python (venv)
├── img/                                      # Capturas de pantalla para la documentación
├── notebooks/                                # Notebook de análisis de auditorías y métricas en Jupyter
├── scripts/                                  # Pipeline de Automatización (Python)
│   ├── 00_bootstrap.py                       # Configuración inicial de directorios HDFS
│   ├── 10_generate_data.py                   # Generación de dataset sintético (Logs/IoT)
│   ├── 20_ingest_hdfs.py                     # Ingesta de datos a HDFS
│   ├── 30_fsck_data_audit.py                 # Auditoría de salud en capa Raw (/data)
│   ├── 40_backup_copy.py                     # Proceso de Backup/Replicación (/backup)
│   ├── 50_inventory_compare.py               # Validación de integridad (Inventario)
│   ├── 60_fsck_backup_audit.py               # Auditoría de salud en capa Backup (/backup)
│   ├── 70_incident_simulation.py             # Simulación de caída de nodos e impacto de la caída
│   ├── 80_recovery_restore.py                # Recuperación y comprobación de Self-Healing
│   └── 90_run_all.py                         # Orquestador para ejecutar todo el flujo
├── .gitignore                                # Exclusiones de Git
├── README.md                                 # Documentación principal del proyecto (Este archivo)
└── requirements.txt                          # Dependencias y librerías necesarias

```
---

## Arquitectura y Decisiones Técnicas

El diseño del sistema se ha concebido para garantizar la Integridad del Dato (Data Integrity) y la resiliencia operativa. La arquitectura se divide en tres capas lógicas: Infraestructura, Pipeline de Datos y Estrategia de Ingesta.

### 1. Arquitectura de Infraestructura (Microservicios)
El clúster se despliega mediante Docker Compose, simulando un entorno Hadoop 3.x distribuido. La arquitectura separa claramente las funciones de gestión (Maestros) de las de trabajo (Workers).

| Servicio / Contenedor | Rol (Función) | Descripción | Puertos (Accesos) |
| :--- | :--- | :--- | :--- |
| **`namenode`** | **Maestro de HDFS** | Gestiona el índice de archivos (metadatos) pero **no almacena datos** de usuario. Actúa como servidor principal para **Jupyter Notebook**, permitiendo persistir el trabajo en la carpeta `./notebooks`. | `9870` (HDFS WebUI)<br>`8889` (Jupyter Lab) |
| **`resourcemanager`** | **Maestro de YARN** | Administra los recursos del clúster (CPU y RAM). Su función es decidir qué nodo está libre para ejecutar las tareas que enviamos. | `8088` (YARN WebUI) |
| **`dnnm`** (x4) | **Nodos de Trabajo**<br>*(Storage + Compute)* | Son 4 contenedores escalados (`clustera-dnnm-1` a `4`) que realizan el trabajo pesado: **almacenan** los bloques de datos físicos y **ejecutan** el procesamiento. | *Internos* |

### 2. Pipeline de Datos: Ciclo de Vida y Resiliencia
Se ha implementado un flujo de trabajo que unifica la ingesta con protocolos estrictos de auditoría y recuperación. Cada fase es ejecutada por scripts especializados en Python.

**FASE A: Aprovisionamiento e Ingesta**
- **Inicialización (`00_bootstrap.py`):** Configura el "esqueleto", creando las estructuras de directorios (`/data`, `/audit`, `/backup`).

- **Generación Sintética (`10_generate_data.py`):** Simula una fuente de datos de alta velocidad utilizando Polars y Faker. Los datos se generan en una carpeta local (`data_local`) en formatos estándar: JSONL (para IoT) y LOG (para Logs), optimizando la memoria mediante escritura en lotes (batches).

- **Ingesta al Data Lake (`20_ingest_hdfs.py`):** Transfiere los datos desde local a la carpeta `/raw` de HDFS. Implementa un particionado estilo Hive (`/data/.../dt=YYYY-MM-DD/`) para optimizar el almacenamiento y el rendimiento de lectura futuro.


**FASE B: Gobierno y Aseguramiento**
Sistema de "Defensa en Profundidad" para garantizar la durabilidad del dato una vez almacenado.

- **Auditoría de Salud (`30_fsck_data_audit.py`):** Ejecuta diagnóstico (`hdfs fsck`) sobre el directorio `/data`. Detecta bloques corruptos (`CORRUPT`) o perdidos (`MISSING`) y genera evidencias tanto en local (parseadas por el notebook creado en `/notebook`) como en HDFS.

- **Replicación y Backup (`40_backup_copy.py`):**
**Nivel 1:** Replicación nativa de HDFS (Factor: 3).
**Nivel 2:** Script que realiza una copia de seguridad (Backup) de los datos hacia el directorio `/backup`, aislando los datos de producción.

- **Validación de Integridad (`50_inventory_compare.py`)**: Realiza una verificación cruzada entre `/data` (Origen) y `/backup` (Destino). Compara byte a byte el tamaño de los archivos para certificar matemáticamente que la copia es idéntica a la original.

- **Auditoría de Respaldo (`60_fsck_backup_audit.py`):** Verificación final de salud sobre el directorio `/backup` para asegurar la integridad del repositorio de recuperación.

**FASE C: Ingeniería del Caos y Recuperación**

Validación de la arquitectura mediante pruebas de estrés controladas.

- **Simulación de Incidente (`70_incident_simulation.py`):** Detiene intencionalmente contenedores DataNode críticos (`clustera-dnnm-1`, `clustera-dnnm-2`) durante la operación de ingesta de datos para probar la tolerancia a fallos del sistema y ver el impacto del accidente en el sistema.

- **Auto-Curación y Restauración (`80_recovery_restore.py`):** Reactiva los DataNodes y comprueba la Auto-Curación del sistema que pasará nuevamente a un estado completamente sano.

### 3. Justificación Técnica: Estrategia de Ingesta "Docker Bridge"

Durante el desarrollo del script `20_ingest_hdfs.py`, se identificó un desafío crítico en la comunicación entre el host (Windows) y el sistema distribuido (Docker).

Por ello, se tomó la decisión técnica de no utilizar la librería estándar `hdfs` (WebHDFS) y optar por una implementación nativa basada en `subprocess` y comandos Docker.

**Justificación del cambio**

- **El Problema:** Al usar la librería `hdfs` desde el host hacia el clúster Dockerizado, la conexión inicial con el NameNode funciona, pero la transmisión de datos falla. Esto ocurre porque el NameNode redirige al cliente a la IP/Hostname interno del contenedor (ej: `ccdbbce22765`), el cual no es resoluble desde el host sin una configuración de DNS compleja o manipulación del archivo `hosts`. Además, mapear los puertos de 4 DataNodes diferentes al mismo puerto local (`9864`) es inviable.

- **La Solución:** Se implementó una lógica de "puente" o bridge. Los scripts de Python mueven los datos locales al contenedor del NameNode (`docker cp`) y ejecutan las órdenes HDFS desde dentro del clúster (`docker exec`). Esto garantiza una comunicación interna fluida y elimina los errores de resolución de nombres (`NameResolutionError`).

---

## Configuración HDFS

### Ubicación de los ficheros de configuración

Las políticas de configuración del clúster se configuran mediante archivos XML aislados dentro de los contenedores Docker.

Debido a que utilizamos una imagen personalizada (`profesorbigdata`), la configuración no está en la ruta estándar de Linux (`/etc/hadoop/conf/`), sino en un directorio de instalación propio:

- **Directorio:**`/opt/bd/hadoop-3.3.6/etc/hadoop/`
- **Verificación :** Puedes comprobar la existencia de estos ficheros ejecutando el siguiente comando desde tu terminal:
```bash
docker exec namenode ls -l /opt/bd/hadoop-3.3.6/etc/hadoop/
```

### Archivo Clave y su Función
Existen varios archivos de configuración clave, pero nos vamos a centrar en el archivo con más impotancia en relación con la integridad, el archivo `hdfs-site.xml`:

| Archivo | Propósito en el Proyecto | Parámetros Críticos que define |
| :--- | :--- | :--- |
| **`hdfs-site.xml`** | **Reglas de Almacenamiento.** Define cómo interactúa el NameNode con los datos físicos. Es el archivo donde configuramos la integridad. | • **`dfs.replication`**: Define cuántas copias de cada bloque se guardan (Configurado a **3** por defecto).<br>• **`dfs.blocksize`**: Define el tamaño del "chunk" de datos (Configurado a **64MB** por defecto).<br>• **`dfs.namenode.name.dir`**: Ruta interna donde el NameNode guarda los metadatos. |

Para extraerlo desde el host y verificar la configuración actual, se utiliza el comando:

```bash
docker cp namenode:/opt/bd/hadoop-3.3.6/etc/hadoop/hdfs-site.xml {Ruta_local}
```
> Nota:  
> Para encontrar la ruta donde estaba alojado el archivo `hdfs-site.xml` he empleado el siguiente comando:
> ```bash
> docker exec namenode find / -name hdfs-site.xml
> ```

### Valores Configurados y Justificación (Integridad vs. Coste)

Los parámetros de configuración del clúster se encuentran definidos en el archivo `hdfs-site.xml`. Basado en las evidencias del entorno desplegado, los valores operativos son:

| Parámetro | Valor Configurado | Descripción |
| :--- | :--- | :--- |
| **`dfs.blocksize`** | `64 MB` | Tamaño del bloque lógico en el que HDFS divide los archivos. |
| **`dfs.replication`** | `3` | Número de copias que se mantienen de cada bloque en distintos nodos. |

**Justificación Técnica**

Para este entorno de laboratorio, se ha reducido el tamaño de bloque a 64 MB (frente al default de 128 MB) para optimizar el paralelismo. Dado que nuestros archivos de ingesta rondan los 250 MB, un bloque de 128 MB apenas generaría 2 bloques, infrautilizando el clúster. Con 64 MB, obtenemos ~4 bloques por archivo, permitiendo que los 4 DataNodes trabajen simultáneamente en la lectura/escritura, mejorando la distribución de carga sin saturar la memoria del NameNode. Respecto a la replicación, se mantiene el Factor 3 (RF=3) como estándar de integridad. Esto garantiza que, incluso si fallan dos nodos, el dato sigue disponible, ofreciendo la redundancia necesaria para un entorno que simula producción crítica sin incurrir en el coste excesivo de la replicación total (RF=4).

### Niveles de Integridad: CRC vs. Hashing End-to-End
Es fundamental distinguir entre la integridad que ofrece la infraestructura (HDFS) y la que requiere el negocio (Aplicación).

1. **¿Por qué el CRC (por bloque) es habitual en HDFS?** HDFS implementa nativamente **CRC-32C** (Cyclic Redundancy Check) por cada 512 bytes de datos. Su uso es el estándar en sistemas distribuidos por dos razones clave:

    - **Eficiencia Computacional**: El cálculo de CRC es extremadamente rápido y ligero para la CPU. Esto permite a HDFS verificar terabytes de datos en tiempo real mientras se leen o escriben sin ralentizar el sistema.

    - **Detección de "Bit Rot"**: Su función principal es detectar la degradación física del disco duro o errores de ruido en la transmisión de red. Si un bit cambia de 0 a 1 por un fallo de hardware, el CRC lo detecta y HDFS descarta ese bloque corrupto automáticamente.

2. **¿Qué aporta SHA/MD5 a nivel de aplicación?** Aunque el CRC garantiza que el bloque se lee igual que se escribió, no valida la lógica del archivo. Aquí es donde entran nuestros scripts con algoritmos de hash criptográfico (como SHA-256 o MD5) sobre el archivo completo:

    - **Integridad Lógica (End-to-End):** El CRC no detecta si un proceso de ingesta se cortó a la mitad o si un archivo fue truncado por un error de software (el disco guardó bien "medio archivo", por lo que el CRC es válido, pero el dato es inútil).

    - **Autenticidad:** El hash actúa como una **huella digital única**. Nos asegura matemáticamente que el archivo que reside en `/backup` es idéntico al original generado, protegiéndonos contra errores humanos, manipulaciones o fallos lógicos que el sistema de archivos no puede ver.

---

## Servicios y UIs
- NameNode UI: http://localhost:9870
- ResourceManager UI: http://localhost:8088
- Jupyter (NameNode): http://localhost:8889

---

## Guía de reproducción
Esta sección detalla el procedimiento paso a paso para desplegar el entorno y ejecutar el pipeline de datos de forma controlada. Si prefieres una ejecución automática y desatendida, consulta la sección Quickstart más abajo.

### Requisitos Previo

Asegúrate de tener instalado y configurado lo siguiente en tu máquina local:

- Docker Desktop
- Python 3.11.9
- Git

### Preparación del Entorno Python

El proyecto utiliza un entorno virtual para aislar las dependencias y la herramienta `uv` para una instalación rápida de paquetes.

1. **Creamos la carpeta donde clonaremos el repo**

2. **Clonamos el repo en nuestra carpeta de trabajo**
```bash
# Abrimos la carpeta donde vamos a querer almacenar este proyecto con Visual Studio
# Abrimos una terminal posicionandonos en dicha carpeta y ejecutamos lo siguiente
git clone https://github.com/dmeana0-creator/data-integrity-hdfs-lab.git
```

3. **Creamos el entorno virtual:**
- Para Windows
```bash
# En la raíz del proyecto (data-integrity-hdfs-lab)
python -m venv env
```
- Para Linux
```bash
# En la raíz del proyecto (data-integrity-hdfs-lab)
python3 -m venv env
```

4. **Activamos el entorno:**
- Windows (PowerShell): `.\env\Scripts\activate`
- Linux/Mac: `source env/bin/activate`

5. **Instalamos dependencias:**
```bash
pip install uv
uv pip install -r requirements.txt
```

### Despliegue de la Infraestructura (Hadoop Cluster)

Levantamos el clúster HDFS escalando los nodos de datos (DataNodes) a 4 instancias

1. **Navegamos al directorio de orquestación:**
```bash
cd docker/clusterA
```

2. **Limpiamos por si hay contenedores anteriores desplegados:**
```bash
docker compose down
```

3. **Iniciamos los contenedores**
```bash
docker compose up -d --scale dnnm=4
```

### Ejecución del Pipeline

>**¡IMPORTANTE!**: Evita ejecutar el los scripts cerca de las 23:00 - 00:00 de la noche, ya que todos los scripts deben de ejecutarse en la misma fecha. Y asegurate de clonar el repositorio en una carpeta de trabajo a ser posible específica para >este proyecto, ya que se crean carpetas fuera de la carpeta raiz del repositorio `data-integrity-hdfs-lab`.

Tenemos dos opciones para ejecutar la lógica del proyecto:
- **Opción A: Ejecución Maestra (Recomendada)** Ejecutamos el orquestador que lanza todos los scripts en el orden correcto y gestiona los tiempos de espera.
```bash
cd ../../scripts
python 90_run_all.py
```
> Nota: en el caso de ejecutarlos en Linux el comando sería así:
> ```bash
>cd ../../scripts
>python3 90_run_all.py
>```

- **Opción B: Ejecución Manual Paso a Paso** Si deseamos ver el resultado de cada fase individualmente, ejecutamos los scripts en este orden desde la carpeta `scripts/` :

1. `python 00_bootstrap.py` (Prepara directorios HDFS).
2. `python 10_generate_data.py` (Genera logs y datos IoT en local).
3. `python 20_ingest_hdfs.py` (Sube los datos al clúster).
4. `python 30_fsck_data_audit.py` (Verifica la salud de los datos originales).
5. `python 40_backup_copy.py` (Realiza la copia de los archivos en /backup).
6. `python 50_inventory_compare.py` (Valida integridad entre /data y /backup byte a byte).
7. `python 60_fsck_backup_audit.py` (Audita el backup).
8. `python 70_incident_simulation.py` (Simula la caída de nodos y muestra su impacto).
9. `python 80_recovery_restore.py` (Restaura el servicio y verifica el self-healing).

> Nota: en el caso de ejecutarlos en Linux el comando sería así:
> ```bash
>cd ../../scripts
>python3 {nombre_script}.py
>```

> **¡IMPORTANTE!**: Cada vez que se ejecuta un script que realiza una auditoría (30, 60, 80 y 70) se reescribe la anterior.

### Ejecución notebook en Jupyter

Como hemos indicado anteriormente, hay un servidor Jupyter Notebook alojado directamente en el contenedor del NameNode. Este entorno nos permite analizar de forma visual y estructurada las auditorías generadas por los scripts de Python.

Para acceder y ejecutar el análisis, sigue estos pasos:

1. **Requisitos previos:** Asegúrate de haber ejecutado el docker compose previamente y alguno de los scripts que generen auditorias: `30_fsck_data_audit.py`, `60_fsck_backup_audit.py`, `70_incident_simulation.py` o `80_recovery_restore.py` incluyendo los que les preceden de forma individual o en su defecto ejecutar el pipeline completo `90_run_all.py`.

> **¡IMPORTANTE!**: Para ir generando los resúmenes de auditorías uno a uno se recomienda ejecutar el notebook cada vez que se genera un informe fsck. Siendo preferible así la opción de ejecutar los scripts secuencialmente de forma manual.

2. **Acceso a la interfaz:** Abre tu navegador web e ingresa a http://localhost:8889. Si esto no funciona ve a Docker Desktop y clica sobre el nombre del contenedor `namenode` y busca en los logs algo así http://127.0.0.1:8889.

3. **Localiza el Notebook:** La carpeta local `docker/clusterA/notebooks/` está mapeada dentro del contenedor. En la interfaz de Jupyter, navega hasta el archivo principal llamado `02_auditoria_integridad.ipynb` y ábrelo.

4. **Ejecución del análisis:** Ejecuta las celdas de forma secuencial.

El notebook leerá los reportes de auditorías generadas en la carpeta `raw_audits` generada los scripts de auditoría, procesará los datos y te mostrará un resumen claro sobre el estado de salud del directorio auditado (`/data` o `/backup`) y lo guardará en el sistema de archivos HDFS en `/audits` y en local en la carpeta `/resumen_audits`que se creará en la misma carpeta del notebook al ejecutar sus celdas.

Además, en el notebook podemos visualizar el análisis de diferentes métricas, en relación al uso de diferentes factores de replicación, y unas conclusiones y recomendaciones de dicho análisis.

---

## Quickstart (para corrección)

> **¡IMPORTANTE!**: Evita ejecutar el quickstart cerca de las 23:00 - 00:00 de la noche, ya que todos los scripts deben de ejecutarse en la misma fecha. Y asegurate de clonar el repositorio en una carpeta de trabajo a ser posible específica para este
> proyecto, ya que se crean carpetas fuera de la carpeta raíz del repositorio `data-integrity-hdfs-lab`.

### Versión Linux
```bash
cd data-integrity-hdfs-lab && python3 -m venv env && source env/bin/activate && pip install uv && uv pip install -r requirements.txt && cd docker/clusterA && docker compose down && docker compose up -d --scale dnnm=4 && cd ../../scripts && python ./90_run_all.py
```
### Versión Windows
```bash
cd data-integrity-hdfs-lab ; python -m venv env ; .\env\Scripts\activate ; pip install uv ; uv pip install -r requirements.txt ; cd docker/clusterA ; docker compose down ; docker compose up -d --scale dnnm=4 ; cd ../../scripts ; python ./90_run_all.py
```

> **¡IMPORTANTE!**: Cada vez que se ejecuta un script que realiza una auditoría en un mismo directorio el mismo día (`30_fsck_data_audit.py`, `70_incident_simulation.py`, `80_recovery_restore.py`) se reescribe la anterior. 