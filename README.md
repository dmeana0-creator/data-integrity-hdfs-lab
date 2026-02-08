# DataSecure Lab — Integridad de Datos en Big Data (HDFS)
**Versión**: `v1.0-entrega` **Contexto** Proyecto de ingenieria de datos para la empresa ficticia Datasecure.

Este repositorio contiene la implementación de un pipeline de ingesta y preservación digital para la gestión, auditoría, simulación de incidentes y recuperación de datos sensibles (Logs y Telemetría IoT) en un ecosistema Hadoop dockerizado.

El objetivo principal es garantizar la Integridad del Dato (Data Integrity) y validar la resiliencia de la infraestructura frente a fallos críticos, demostrando capacidades de Self-Healing (auto-recuperación).

---

## Arquitectura y Decisiones Técnicas

El diseño del sistema se ha concebido para garantizar la Integridad del Dato (Data Integrity) y la resiliencia operativa. La arquitectura se divide en tres capas lógicas: Infraestructura, Pipeline de Datos y Estrategia de Ingesta.

### 1. Arquitectura de Infraestructura (Microservicios)
El clúster está orquestado mediante Docker Compose (v3.8) y simula un entorno Hadoop 3.x (YARN + HDFS) utilizando una red interna en modo Bridge.

| Rol | Servicio (Container) | Descripción Técnica | Puertos Expuestos |
| :--- | :--- | :--- | :--- |
| **Maestro (Master)** | `namenode` | **Cerebro del sistema.** Gestiona el Namespace (metadatos) y la tabla de bloques. Aloja el servidor de **Jupyter Notebook** para análisis local. Persiste el trabajo mediante volúmenes en `./notebooks`. | `9870` (HDFS UI)<br>`8889` (Jupyter) |
| **Gestión (Mgmt)** | `resourcemanager` | **Orquestador.** Gestiona la asignación de recursos computacionales (CPU/RAM) en el clúster YARN. | `8088` (YARN UI) |
| **Trabajadores (Slaves)** | `dnnm` (x4) | **Almacenamiento y Cómputo.** 4 instancias escaladas (`clustera-dnnm-1` a `4`) que ejecutan los daemons **DataNode** y **NodeManager**. | Internos |

### 2. Pipeline de Datos: Ciclo de Vida y Resiliencia (End-to-End)
Se ha implementado un flujo de trabajo que unifica la ingesta masiva con protocolos estrictos de auditoría y recuperación. Cada fase es ejecutada por scripts especializados en Python.

**FASE A: Aprovisionamiento e Ingesta**
- **Inicialización (`00_bootstrap.py`):** Configura el "esqueleto" del Data Lake, creando las estructuras de directorios (`/data`, `/audit`, `/backup`) y definiendo permisos en HDFS antes de cualquier operación.

- **Generación Sintética (`10_generate_data.py`):** Simula una fuente de datos de alta velocidad utilizando Polars y Faker. Los datos se generan en una zona de Staging Local (`data_local`) en formatos estándar: JSONL (para IoT) y LOG (para Logs), optimizando la memoria mediante escritura en lotes (batches).

- **Ingesta al Data Lake (`20_ingest_hdfs.py`):** Transfiere los datos desde Staging a la capa Raw de HDFS. Implementa un particionado estilo Hive (`/data/.../dt=YYYY-MM-DD/`) para optimizar el almacenamiento y el rendimiento de lectura futuro.


**FASE B: Gobierno y Aseguramiento**
Sistema de "Defensa en Profundidad" para garantizar la durabilidad del dato una vez almacenado.

- **Auditoría de Salud (`30_fsck_data_audit.py`):** Ejecuta diagnóstico (`hdfs fsck`) sobre el directorio `/data`. Detecta bloques corruptos (`CORRUPT`) o perdidos (`MISSING`) y genera evidencias tanto en local (parseadas por el notebook creado en `/notebook`) como en HDFS.

- **Replicación y Backup (`40_backup_copy.py`):**
**Nivel 1:** Replicación nativa de HDFS (Factor: 3).
**Nivel 2:** Script que realiza una copia de seguridad (Backup) de los datos hacia el directorio `/backup`, aislando los datos de producción.

- **Validación de Integridad (`50_inventory_compare.py`)**: Realiza una verificación cruzada entre `/data` (Origen) y `/backup` (Destino). Compara byte a byte el tamaño de los archivos para certificar matemáticamente que la copia es idéntica al original.

- **Auditoría de Respaldo (`60_fsck_backup_audit.py`):** Verificación final de salud sobre el directorio `/backup` para asegurar la integridad del repositorio de recuperación.

**FASE C: Ingeniería del Caos y Recuperación**

Validación empírica de la arquitectura mediante pruebas de estrés controladas.

- **Simulación de Incidente (`70_incident_simulation.py`):** Detiene intencionalmente contenedores DataNode críticos (`clustera-dnnm-1`, `clustera-dnnm-2`) durante operaciones de escritura activas para probar la tolerancia a fallos del sistema y ver el impacto del accidente en el sistema.

- **Auto-Curación y Restauración (`80_recovery_restore.py`):** Reactiva la infraestructura y monitorea el proceso de Self-Healing de Hadoop (re-replicación de bloques y balanceo) hasta que el clúster reporta nuevamente un estado completamente sano.

### 3. Justificación Técnica: Estrategia de Ingesta "Docker Bridge"

Durante el desarrollo del script `20_ingest_hdfs.py`, se identificó un desafío crítico en la comunicación entre el host (Windows) y el sistema distribuido (Docker).

Por ello, se tomó la decisión técnica de no utilizar la librería estándar `hdfs` (WebHDFS) y optar por una implementación nativa basada en `subprocess` y comandos Docker.

**Justificación del cambio**

- **El Problema:** Al usar la librería `hdfs` desde el host (Windows/Linux) hacia el clúster Dockerizado, la conexión inicial con el NameNode funciona, pero la transmisión de datos falla. Esto ocurre porque el NameNode redirige al cliente a la IP/Hostname interno del contenedor (ej: `ccdbbce22765`), el cual no es resoluble desde el host sin una configuración de DNS compleja o manipulación del archivo `hosts`. Además, mapear los puertos de 4 DataNodes diferentes al mismo puerto local (`9864`) es inviable.

- **La Solución:** Se implementó una lógica de "puente" o bridge. Los scripts de Python mueven los datos locales al contenedor del NameNode (`docker cp`) y ejecutan las órdenes HDFS desde dentro del clúster (`docker exec`). Esto garantiza una comunicación interna fluida y elimina los errores de resolución de nombres (`NameResolutionError`).

---

## Servicios y UIs
- NameNode UI: http://localhost:9870
- ResourceManager UI: http://localhost:8088
- Jupyter (NameNode): http://localhost:8889

---

## Estructura del repositorio
- `docker/clusterA/`: docker-compose del aula (Cluster A)
- `scripts/`: pipeline (generación → ingesta → auditoría → backup → incidente → recuperación)
- `notebooks/`: análisis en Jupyter (tabla de auditorías y métricas)
- `docs/`: documentación (enunciado, rúbrica, pistas, entrega, evidencias)

---

## Configuración HDFS

**Ubicación de los ficheros de configuración**

Las políticas de configuración del clúster se configuran mediante archivos XML aislados dentro de los contenedores Docker.

Debido a que utilizamos una imagen personalizada (`profesorbigdata`), la configuración no está en la ruta estándar de Linux (`/etc/hadoop/conf/`), sino en un directorio de instalación propio:

- **Directorio:**`/opt/bd/hadoop-3.3.6/etc/hadoop/`
- **Verificación :** Puedes comprobar la existencia de estos ficheros ejecutando el siguiente comando desde tu terminal:
```bash
docker exec namenode ls -l /opt/bd/hadoop-3.3.6/etc/hadoop/
```

**Archivo Clave y su Función**
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

**Valores Configurados y Justificación (Integridad vs. Coste)**


---


