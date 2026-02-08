# Evidencias (plantilla)

Incluye aquí (capturas o logs) con fecha:

## 1) NameNode UI (9870)

En esta captura podemos visualizar el número de datanodes que administra nuestro NameNode, en mi caso 4 datanodes:
![Captura del NameNode con 4 DataNodes](../img/numero_datanodes_usados.png)

A continuación se nos muestra los datanodes vivos y sus características en relación con su almacenamiento:
![Detalle de DataNodes vivos y capacidad](../img/datanodes_vivos_y_su_almacenamiento.png)

**Observación:** En la captura se confirma que los 4 nodos están en estado **"In Operation"** (vivos). También se observa la columna *Configured Capacity* (aprox. 1006 GB por nodo) y *DFS Used*, lo que valida la capacidad disponible del clúster para iniciar la ingesta.


## 2) Auditoría fsck

## 2.1) Auditoría fsck sobre /data
- Capturas Auditoría fsck sobre /data:
![Captura 1 Auditoría fsck sobre /data](../img/fsck_data_captura_1.png)
![Captura 2 Auditoría fsck sobre /data](../img/fsck_data_captura_2.png)

- Captura resumen de la Auditoría fsck sobre /data:
![Captura resumen de la Auditoría fsck sobre /data](../img/resumen_csv_data.png)

- Captura resumen de la Auditoría fsck sobre /data en HDFS (9870):
![Captura resumen de la Auditoría fsck sobre /data en HDFS (9870)](../img/resumen_data_9870.png)

## 2.2) Auditoría fsck sobre /backup
- Capturas Auditoría fsck sobre /backup:
![Captura 1 Auditoría fsck sobre /backup](../img/fsck_backup_captura_1.png)
![Captura 2 Auditoría fsck sobre /backup](../img/fsck_backup_captura_2.png)

- Captura resumen de la Auditoría fsck sobre /backup:
![Captura resumen de la Auditoría fsck sobre /backup](../img/resumen_csv_backup.png)

- Captura resumen de la Auditoría fsck sobre /backup en HDFS (9870):
![Captura resumen de la Auditoría fsck sobre /backup en HDFS (9870)](../img/resumen_backup_9870.png)

## 3) Backup + validación
- Captura Inventario origen vs destino:
![Captura Inventario origen vs destino](../img/inventario_origen_vs_destino.png)

-  Capturas Evidencias de consistencia (tamaños/rutas) en HDFS (9870)
- /data:
![Captura 1 Evidencias de consistencia (tamaños/rutas) en HDFS (9870): logs en /data](../img/data_9870_logs.png)
![Captura 2 Evidencias de consistencia (tamaños/rutas) en HDFS (9870): IoT en /data](../img/data_9870_iot.png)

- /backup:
![Captura 2 Evidencias de consistencia (tamaños/rutas) en HDFS (9870): logs en /backup](../img/backup_9870_logs.png)
![Captura 3 Evidencias de consistencia (tamaños/rutas) en HDFS (9870): IoT en /backup](../img/backup_9870_iot.png)

## 4) Incidente + recuperación
## 4.1) Incidente
Para el incidente lo que hice, ya que tenía 4 datanodes levantados y en ese momento el factor de replicación que tenia configurado era de 3, decidí apagar 2 datanodes mientra se realizaba la ingestión de los datos que cree en local por parte de HDFS.

De este modo forcé que el sistema de archivos perdiese o no pudiese repartir parte de la información de dichos archivos.

Esto se ve reflejado ya que al realizar una auditoría fsck, después de 10 minutos de espera, sobre ese directorio se ven cambios porque el sistema detectó que algunos bloques no tenian el número de copias esperado (Missing replicas):

- Capturas de la Auditoría fsck sobre /data después del incidente:
![Captura 1 de la Auditoría fsck sobre /data después del incidente](../img/fsck_data_accidente_1.png)
![Captura 2 de la Auditoría fsck sobre /data después del incidente](../img/fsck_data_accidente_2.png)

- Captura resumen de la Auditoría fsck sobre /data después del incidente:
![Captura resumen de la Auditoría fsck sobre /data después del incidente](../img/resumen_csv_data_accidente.png)

## 4.2) Recuperación
Para la recuperación, una vez finalizado de forma exitosa el accidente simulado, procedí a levantar los mismos 2 nodos que apagué durante la ingesta. Tras esto esperé un total de 10 minutos, para que le de tiempo al sistema HDFS a auto-recuperarse, y así, poder ver esa recuperación reflejada en una auditoria que hago posteriormenete del directorio /data.

Tras realizar la recuperación y esperar la actualización del reporte fsck, se pudo observar que el sistema ya habia solucionado el problema de las réplicas perdidas:

- Capturas de la Auditoría fsck sobre /data después de la recuperación:
![Captura 1 de la Auditoría fsck sobre /data después de la recuperación](../img/fsck_data_recuperacion_1.png)
![Captura 2 de la Auditoría fsck sobre /data después de la recuperación](../img/fsck_data_recuperacion_2.png)

- Captura resumen de la Auditoría fsck sobre /data después de la recuperación:
![Captura resumen de la Auditoría fsck sobre /data después de la recuperación](../img/resumen_csv_data_recuperacion.png)


## 5) Métricas
## 5.1) Docker stats
## 5.1.1) Docker stats durante la subida a HDFS
En estas capturas se puede observar los docker stats durante el proceso de ingesta de los datos creados por el script 10 por parte del sistema HDFS a través del script 20, todo ello con el sistema de archivos configurado con un factor de replicación de diferente valor, en este caso desde el valor 1 al 4:

- Subida a HDFS (Ingesta) con Factor de Replicación = 1 :
![Captura Subida a HDFS (Ingesta) con Factor de Replicación = 1](../img/stats_ingestion_fr1.png)

- Subida a HDFS (Ingesta) con Factor de Replicación = 2 :
![Captura Subida a HDFS (Ingesta) con Factor de Replicación = 2]()

- Subida a HDFS (Ingesta) con Factor de Replicación = 3 :
![Captura Subida a HDFS (Ingesta) con Factor de Replicación = 3]()

- Subida a HDFS (Ingesta) con Factor de Replicación = 4 :
![Captura Subida a HDFS (Ingesta) con Factor de Replicación = 4]()


## 5.1.2) Docker stats durante la replicación/copia en HDFS
En estas capturas se puede observar los docker stats durante el proceso de backup (replicación/copia) realizado por el script 40, de los datos previamente ingestados por el script 20, todo ello con el sistema de archivos configurado con un factor de replicación de diferente valor, en este caso desde el valor 1 al 4:

- Replicación/copia (Backup) en HDFS con Factor de Replicación = 1 :
![Captura Replicación/copia (Backup) en HDFS con Factor de Replicación = 1](../img/stats_backup_fr1.png)

- Replicación/copia (Backup) en HDFS con Factor de Replicación = 2 :
![Captura Replicación/copia (Backup) en HDFS con Factor de Replicación = 2]()

- Replicación/copia (Backup) en HDFS con Factor de Replicación = 3 :
![Captura Replicación/copia (Backup) en HDFS con Factor de Replicación = 3]()

- Replicación/copia (Backup) en HDFS con Factor de Replicación = 4 :
![Captura Replicación/copia (Backup) en HDFS con Factor de Replicación = 4]()

## 5.2) Tabla de tiempos
![Captura Tabla de tiempos]()

## 5.3) Impacto de replicación
