# Taller Apache Kafka - ARSW

**Escuela Colombiana de Ingeniería Julio Garavito**  
**Asignatura:** Arquitecturas de Software (ARSW)  
**Duración:** 2 a 3 horas | **Modalidad:** 2 estudiantes | **Nivel:** Intermedio

---

## Capítulo 1 – Evolución hacia arquitecturas orientadas por eventos

### Actividad 1 – Análisis de comunicación

**Enunciado:** Para una tienda en línea, clasifique qué procesos deberían ser síncronos, asíncronos o híbridos: consultar productos, crear pedido, validar pago, enviar notificación, actualizar analítica y registrar auditoría.

| Proceso | Tipo | Justificación |
|---|---|---|
| Consultar productos | **Síncrono (REST)** | El usuario necesita la respuesta de inmediato para navegar el catálogo. La consulta es simple y no desencadena acciones en otros servicios. |
| Crear pedido | **Híbrido** | La creación necesita una respuesta inmediata (ID del pedido), pero los procesos posteriores (pago, inventario, notificación) se desencadenan de forma asíncrona mediante eventos. |
| Validar pago | **Híbrido** | La validación inicial (formato, saldo) puede ser síncrona para rechazar rápidamente. El procesamiento definitivo y la notificación del resultado son asíncronos. |
| Enviar notificación | **Asíncrono (Kafka)** | Las notificaciones no deben bloquear el flujo principal. El usuario no espera la notificación para que el pedido exista; puede llegar segundos después sin afectar la experiencia. |
| Actualizar analítica | **Asíncrono (Kafka)** | La analítica es un proceso de lectura diferida. No requiere respuesta inmediata y puede tolerar pequeños retrasos. Kafka permite que analytics-service lea los mismos eventos sin impactar los servicios de negocio. |
| Registrar auditoría | **Asíncrono (Kafka)** | La auditoría es un registro de hechos ya ocurridos. No debe afectar la latencia del flujo principal. Kafka garantiza retención para reprocesar si el servicio de auditoría falla. |

**Conclusión:** Los procesos que generan valor inmediato para el usuario (consultas, validaciones críticas) son síncronos. Los procesos derivados o de soporte (notificaciones, analítica, auditoría) son asíncronos mediante eventos, lo que reduce el acoplamiento temporal y mejora la resiliencia.

---

## Capítulo 2 – Apache Kafka: fundamentos y arquitectura interna

### Actividad 2 – Decisiones de configuración

**Configuración analizada:**
- Topic: `orders` | Particiones: 1 | Replicación: 1 | Sin clave | Retención: 24 horas

### Riesgos identificados

| Elemento | Riesgo |
|---|---|
| **1 partición** | Sin paralelismo. Todos los consumidores dentro del mismo Consumer Group procesan secuencialmente. Si el volumen de pedidos crece, el lag aumenta sin posibilidad de escalar horizontalmente añadiendo más instancias del consumidor. |
| **Replicación 1** | Si el único broker falla, el topic y todos sus eventos se pierden permanentemente. No hay tolerancia a fallos. Inaceptable en producción para datos de negocio críticos. |
| **Sin clave de particionamiento** | Los eventos del mismo pedido pueden distribuirse en distintas particiones (cuando haya más de una). Kafka no garantiza orden inter-partición, por lo que `order-created` y `order-cancelled` del mismo pedido podrían procesarse fuera de orden. |
| **Retención 24 horas** | Muy corta para reprocesamiento y auditoría. Si un consumidor falla más de 24 horas, pierde eventos irrecuperables. En un sistema con trazabilidad regulatoria, 24 horas no cumple los requerimientos mínimos. |

### Mejoras propuestas para producción

| Elemento | Configuración sugerida | Motivo |
|---|---|---|
| Particiones | 3 o más (según volumen esperado) | Permite paralelismo: hasta 3 consumidores en el mismo grupo procesan simultáneamente. |
| Replicación | Factor 3 (mínimo 2 en entornos pequeños) | Tolera la caída de hasta 2 brokers sin perder datos. |
| Clave | `orderId` | Garantiza que todos los eventos del mismo pedido vayan a la misma partición y se procesen en orden causal. |
| Retención | 7 días o más (según SLA de auditoría) | Permite reprocesamiento ante fallos prolongados y cumple requerimientos regulatorios básicos. |
| ISR mínimo | 2 | Garantiza que al menos 2 réplicas estén sincronizadas antes de confirmar una escritura (`min.insync.replicas=2`). |

---

## Capítulo 3 – Preparación del entorno de laboratorio

### Actividad 3 – Entorno Docker, topics y eventos

#### Levantar el entorno

```bash
docker compose up -d
docker ps
```

- **Kafka UI** disponible en: http://localhost:8080
- **Broker Kafka** en: localhost:9092

#### Crear los tres topics

```bash
docker exec -it arsw-kafka bash

/opt/kafka/bin/kafka-topics.sh --create --topic orders \
  --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1

/opt/kafka/bin/kafka-topics.sh --create --topic payments \
  --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1

/opt/kafka/bin/kafka-topics.sh --create --topic inventory \
  --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1

/opt/kafka/bin/kafka-topics.sh --list --bootstrap-server localhost:9092
```

#### Publicar cinco eventos JSON

```bash
/opt/kafka/bin/kafka-console-producer.sh \
  --topic orders --bootstrap-server localhost:9092 \
  --property "parse.key=true" --property "key.separator=:"
```

Eventos publicados (clave:valor):

```
ORD-1001:{"orderId":"ORD-1001","customerId":"CUS-01","total":120000,"status":"CREATED","occurredAt":"2026-06-30T10:00:00Z"}
ORD-1002:{"orderId":"ORD-1002","customerId":"CUS-02","total":85000,"status":"CREATED","occurredAt":"2026-06-30T10:01:00Z"}
ORD-1003:{"orderId":"ORD-1003","customerId":"CUS-03","total":310000,"status":"CREATED","occurredAt":"2026-06-30T10:02:00Z"}
ORD-1004:{"orderId":"ORD-1004","customerId":"CUS-01","total":45000,"status":"CREATED","occurredAt":"2026-06-30T10:03:00Z"}
ORD-1005:{"orderId":"ORD-1005","customerId":"CUS-04","total":260000,"status":"CREATED","occurredAt":"2026-06-30T10:04:00Z"}
```

#### Verificación en Kafka UI (http://localhost:8080)

| Evento | Clave | Partición | Offset | Contenido |
|---|---|---|---|---|
| order-created | ORD-1001 | 0 | 0 | JSON completo |
| order-created | ORD-1002 | 1 | 0 | JSON completo |
| order-created | ORD-1003 | 2 | 0 | JSON completo |
| order-created | ORD-1004 | 0 | 1 | JSON completo |
| order-created | ORD-1005 | 1 | 1 | JSON completo |

> La distribución de particiones depende del hash de la clave (`orderId`): `partition = hash(key) % numPartitions`. Kafka UI permite inspeccionar topic, partición, offset, clave y contenido de cada mensaje.
