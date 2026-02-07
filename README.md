# DataSecure Lab — Integridad de Datos en Big Data (HDFS)
**Versión**: `v1.0-entrega` **Contexto** Proyecto de ingenieria de datos para la empresa ficticia Datasecure.

Este repositorio contiene la implementación de un pipeline de ingesta y preservación digital para la gestión, auditoría, simulación de incidentes y recuperación de datos sensibles (Logs y Telemetría IoT) en un ecosistema Hadoop dockerizado.

El objetivo principal es garantizar la Integridad del Dato (Data Integrity) y validar la resiliencia de la infraestructura frente a fallos críticos, demostrando capacidades de Self-Healing (auto-recuperación).

---

## Arquitectura y Decisiones Técnicas

El diseño del sistema se ha concebido para garantizar la Integridad del Dato (Data Integrity) y la resiliencia operativa. La arquitectura se divide en tres capas lógicas: Infraestructura, Pipeline de Datos y Estrategia de Ingesta.

### 1. Arquitectura de Infraestructura (Microservicios)
El clúster está orquestado mediante Docker Compose (v3.8) y simula un entorno Hadoop 3.x (YARN + HDFS) utilizando una red interna en modo Bridge.

| Rol | Servicio (Container) | Descripción Técnica |
| :--- | :--- | :--- |
| **Maestro (NameNode)** | `namenode` | **Cerebro.** Gestiona los metadatos. Aloja el servidor **Jupyter** en el puerto `8889` con acceso a los logs de auditoría compartidos. |
| **Gestión (ResourceManager)** | `resourcemanager` | **Orquestador.** Gestiona los recursos de cómputo (YARN) del clúster. |
| **Trabajadores (DataNodes)** | `dnnm` (x4) | **Almacenamiento.** 4 instancias escaladas que garantizan la redundancia física. |

### 2. Pipeline de Datos: Ciclo de Vida y Resiliencia (End-to-End)
Se ha implementado un flujo de trabajo que unifica la ingesta masiva con protocolos estrictos de auditoría y recuperación. Cada fase es ejecutada por scripts especializados en Python.

**FASE A: Aprovisionamiento e Ingesta**
- **Inicialización (`00_bootstrap.py`):** Configura el "esqueleto" del Data Lake, creando las estructuras de directorios (/data, /audit, /backup) y definiendo permisos en HDFS antes de cualquier operación.

- **Generación Sintética (`10_generate_data.py`):** Simula una fuente de datos de alta velocidad utilizando Polars y Faker. Los datos se generan en una zona de Staging Local (data_local) en formatos estándar: JSONL (para IoT) y LOG (para Logs), optimizando la memoria mediante escritura en lotes (batches).

- **Ingesta al Data Lake (`20_ingest_hdfs.py`):** Transfiere los datos desde Staging a la capa Raw de HDFS. Implementa un particionado estilo Hive (`/data/.../dt=YYYY-MM-DD/`) para optimizar el almacenamiento y el rendimiento de lectura futuro.


**FASE B: Gobierno y Aseguramiento**
Sistema de "Defensa en Profundidad" para garantizar la durabilidad del dato una vez almacenado.

- **Auditoría de Salud (`30_fsck_data_audit.py`):** Ejecuta diagnóstico (hdfs fsck) sobre el directorio /data. Detecta bloques corruptos (CORRUPT) o perdidos (MISSING) y genera evidencias tanto en local (parseadas por el notebook creado en `/notebook`) como en HDFS.

- **Replicación y Backup (`40_backup_copy.py`):**
**Nivel 1:** Replicación nativa de HDFS (Factor: 3).
**Nivel 2:** Script que realiza una copia de seguridad (Backup) de los datos hacia el directorio `/backup`, aislando los datos de producción.

- **Validación de Integridad (`50_inventory_compare.py`)**: Realiza una verificación cruzada entre /data (Origen) y /backup (Destino). Compara byte a byte el tamaño de los archivos para certificar matemáticamente que la copia es idéntica al original.

- **Auditoría de Respaldo (`60_fsck_backup_audit.py`):** Verificación final de salud sobre el entorno de backup para asegurar la integridad del repositorio de recuperación.

**FASE C: Ingeniería del Caos y Recuperación**

Validación empírica de la arquitectura mediante pruebas de estrés controladas.

- **Simulación de Incidente (`70_incident_simulation.py`):** Detiene intencionalmente contenedores DataNode críticos (clustera-dnnm-1, clustera-dnnm-2) durante operaciones de escritura activas para probar la tolerancia a fallos del sistema y ver el impacto del accidente en el sistema.

- **Auto-Curación y Restauración (`80_recovery_restore.py`):** Reactiva la infraestructura y monitorea el proceso de Self-Healing de Hadoop (re-replicación de bloques y balanceo) hasta que el clúster reporta nuevamente un estado HEALTHY.

### 3. Justificación Técnica: Estrategia de Ingesta "Docker Bridge"

Durante el desarrollo del script `20_ingest_hdfs.py`, se identificó un desafío crítico en la comunicación entre el host (Windows) y el sistema distribuido (Docker).


El despliegue se realiza sobre una red interna aislada (hadoop-net) configurada en modo Bridge.
- **

```bash
cd docker/clusterA && docker compose up -d
bash scripts/00_bootstrap.sh && bash scripts/10_generate_data.sh && bash scripts/20_ingest_hdfs.sh
bash scripts/30_fsck_audit.sh && bash scripts/40_backup_copy.sh && bash scripts/50_inventory_compare.sh
bash scripts/70_incident_simulation.sh && bash scripts/80_recovery_restore.sh
```

> Si algún script necesita variables:  
> `DT=YYYY-MM-DD` (fecha) y `NN_CONTAINER=namenode` (nombre del contenedor NameNode).

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

## Normas de entrega (individual)
Consulta `docs/entrega.md`.  
**Obligatorio:** tag final `v1.0-entrega`.

---

## Nota
Este repositorio es un “starter kit”: algunos scripts contienen **TODOs** para completar el proyecto.
