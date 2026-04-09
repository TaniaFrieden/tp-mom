# Implementación del Middleware de RabbitMQ

## Descripción general

La implementación fue desarrollada en Python. Se implementaron las dos clases requeridas por el TP usando la librería `pika` como cliente de RabbitMQ:

- `MessageMiddlewareQueueRabbitMQ`: work queue con múltiples consumidores compitiendo por los mensajes.
- `MessageMiddlewareExchangeRabbitMQ`: exchange directo con cola exclusiva por consumidor para soportar broadcast.

Ambas heredan de `_BaseMiddlewareRabbitMQ`, una clase base interna que centraliza la lógica de ciclo de vida compartida (`_cleanup_resources`, `start_consuming`, `stop_consuming`, `close`), evitando duplicación de código.


## Decisiones de diseño

### Clase base compartida

`_cleanup_resources`, `start_consuming`, `stop_consuming` y `close` son idénticos en Queue y Exchange. Se los extrajo a `_BaseMiddlewareRabbitMQ` para no repetir código. Las subclases solo implementan lo que es genuinamente distinto: la inicialización del recurso (cola o exchange) y el método `send`.

### Conexión por instancia

Cada instancia crea su propia `pika.BlockingConnection`. Esto simplifica el diseño y evita que productores y consumidores distintos compartan estado interno de conexión.

`connection`, `channel` y `consumer_tag` se inicializan en `None` antes de conectar, lo que permite que `close()` y `stop_consuming()` sean seguros de llamar en cualquier estado, incluso si el constructor falló a mitad de camino.

### Adaptación del callback (`_wrap_pika_callback`)
 
`pika` invoca al consumidor con `(channel, method, properties, body)`, pero la clase base abstracta provista por la cátedra define que el callback del usuario debe recibir `(message, ack, nack)`. La función `_wrap_pika_callback` construye las clausuras `ack` y `nack` a partir del `delivery_tag` del mensaje recibido y delega al callback del usuario con esa firma simplificada.

### Prefetch

Antes de iniciar el consumo se configura `basic_qos(prefetch_count=1)`. Esto evita que RabbitMQ acumule mensajes en un solo consumidor cuando hay varios activos, distribuyendo la carga de forma más equitativa. El valor está definido en la constante `PREFETCH_COUNT`.

### Work Queue

La cola se declara en el constructor con `queue_declare`. El envío usa el exchange por defecto (`exchange=""`) con `routing_key=queue_name`, que es el mecanismo estándar de work queue en RabbitMQ.

### Exchange directo

Se usa `exchange_type=direct` porque el enrutamiento requerido es por coincidencia exacta de routing key.

Cada instancia consumidora declara su propia cola anónima con `exclusive=True` y `auto_delete=True`. Esto garantiza que cada consumidor reciba una copia del mensaje (broadcast), a diferencia de una cola compartida donde los mensajes se repartirían en round-robin. La cola se elimina automáticamente al cerrar la conexión.

### Manejo de excepciones

Los errores de `pika` se traducen a las tres familias definidas por la interfaz abstracta:

| Excepción | Cuándo se lanza |
|---|---|
| `MessageMiddlewareDisconnectedError` | pérdida de conexión con el broker |
| `MessageMiddlewareMessageError` | error durante envío o consumo |
| `MessageMiddlewareCloseError` | error al cerrar canal o conexión |

## Ejecución de las pruebas

Desde la raíz del proyecto:

```bash
make up
```

Este comando:
1. Construye las imágenes de RabbitMQ y del contenedor de tests.
2. Levanta RabbitMQ y espera a que esté saludable (healthcheck cada 5 segundos, hasta 10 reintentos).
3. Levanta el contenedor de tests una vez que RabbitMQ está listo.
4. Sigue los logs del contenedor de tests en tiempo real.
 
Para detener y destruir los contenedores:
 
```bash
make down
```
 
Para ver los logs de todos los contenedores en un solo flujo:
 
```bash
make logs
```

Resultado esperado: `19 passed`.