# Simulador de estrés de bots

Herramienta local para someter un servidor Citadel a tráfico de movimiento
reproducible y muy intenso. El directorio se divide deliberadamente en las dos
piezas que se ejecutan:

- `client/`: código fuente Rust multiplataforma del simulador de consola.
- `server/`: servidor listo para ejecutar en la plataforma actual, su
  configuración y el gameplay Lua que define el mapa y sus colisiones.

El servidor de esta copia es para Windows y se llama `citadel.exe`. No se
versiona porque es un artefacto de compilación; se genera con
`powershell -ExecutionPolicy Bypass -File .\stage-server.ps1` desde este
directorio. La tarea prepara una copia ya compilada en `server/citadel.exe`.

## Ejecutar una prueba

En una terminal, inicia el servidor:

```powershell
cd server
.\citadel.exe serve --yes
```

En otra terminal, compila y ejecuta el cliente:

```powershell
cd client
cargo run --release
```

El programa pregunta la cantidad de bots (de 1 a 1000), los minutos de la
simulación, el transporte (QUIC o WebSocket), su endpoint, la rampa entre
conexiones y si se desea imprimir cada paquete. QUIC es el modo predeterminado:
cada bot abre su propia conexión QUIC y envía posiciones como datagramas. El
servidor confirma cada movimiento y difunde el estado en snapshots por bloques
de hasta 32 jugadores, para no convertir cada posición en cientos de paquetes
pequeños. La rampa predeterminada de 25 ms evita confundir una ráfaga de handshakes con
capacidad de juego; después hay dos segundos de calentamiento antes del primer
movimiento. WebSocket sigue disponible para comparar el perfil fiable/ordenado.
Para una carga grande, deja el modo
detallado apagado: la consola sigue mostrando un resumen con color cada segundo
y **el archivo JSONL conserva cada evento**. En modo detallado, los errores,
desconexiones y mensajes malformados son rojos; los movimientos confirmados son
verdes; los huecos de secuencia y clamps son amarillos; el resto se muestra en
cyan.

Los resultados quedan en `client/logs/bot-stress-<unix-ns>-<pid>.jsonl.gz`.
Es JSONL compacto comprimido con gzip, no el texto detallado de la consola: los campos no
existentes se omiten, los nombres son cortos y los eventos se guardan como
códigos numéricos. Así se conserva la evidencia atómica sin repetir millones
de veces nombres largos y valores `null`.

| Campo | Significado |
| --- | --- |
| `m` | Tiempo monotónico en nanosegundos. `t` sólo aparece en el metadato inicial, que ancla la ejecución al tiempo de pared. |
| `e`, `s`, `b` | Código del evento, scope (`1` es externo; local se omite) y ordinal del bot. |
| `p`, `q`, `r` | ID de jugador asignado (sólo al asignarse), secuencia y peer. |
| `x`, `z`, `l`, `g`, `d` | Posición de eventos locales, latencia/edad en ns, hueco inferido y detalle de error. Las coordenadas externas quedan en la consola detallada, no se repiten en JSONL. |

Los códigos `e` son: `1` inicio de conexión, `2` conectado, `3` error de
conexión, `4` desconectado, `5` posición enviada, `6` error de envío, `7` fin
de simulación, `8` error de cierre, `9` ACK, `10` movimiento bloqueado, `11`
clamp, `12` ACK malformado, `13` posición externa, `14` hueco de secuencia,
`15` peer malformado, `16` ID de jugador, `17` ID malformado y `18` error de
recepción; `19` es un mensaje de protocolo no manejado y `20` contiene los
metadatos de la ejecución. El analizador traduce
estos códigos de vuelta a nombres legibles y también acepta los JSONL detallados
sin comprimir que se generaron antes de este formato compacto.

## Analizar un log sin LLM

Desde `client/`, el analizador escoge por defecto el último archivo JSONL,
imprime un resumen con colores y escribe un informe JSON nuevo en `reports/`:

```powershell
cargo run --release --bin log-analyzer
```

Para seleccionar una ejecución concreta o ajustar los umbrales:

```powershell
cargo run --release --bin log-analyzer -- `
  --input .\logs\bot-stress-<...>.jsonl.gz `
  --ack-warn-ms 250 `
  --peer-warn-ms 1000 `
  --loss-warn-percent 1
```

El analizador trabaja en streaming y descomprime gzip al vuelo —no carga el log completo en memoria— y
marca errores de conexión, bots sin ID/finalización, desequilibrio envío/ACK,
errores de protocolo, movimientos bloqueados, huecos de secuencia y latencias,
edades o intervalos de actualización anómalos. El reporte JSON contiene los
conteos, los umbrales, los detalles de conexión y las anormalidades para
automatizar un dashboard o una comparación entre ejecuciones.

Para QUIC, los movimientos externos se agrupan en datagramas de snapshot y
usan latest-wins por bloque: si un receptor se queda atrás, conserva la versión
más nueva de cada bloque, no una cola FIFO obsoleta. Por ello los saltos de
secuencia son información de coalescencia; una edad de peer o un intervalo entre
actualizaciones de ese peer superior a su umbral, no el salto en sí, indica
stuttering. Los ACK usan 250 ms como umbral predeterminado porque coincide con
el intervalo de movimiento.

## Validación y limpieza

Con el servidor iniciado, run-validation.ps1 ejecuta 200 bots por 15 segundos
reales con una rampa de 25 ms. clean-artifacts.ps1 mueve los JSONL y reportes
generados a la Papelera de reciclaje.

El servidor aplica el mapa de forma autoritativa: un bot puede escoger una
ruta localmente válida, pero el Lua vuelve a comprobar los límites, el segmento
completo de movimiento y cada obstáculo antes de confirmar la posición. La
confirmación (`ack`) vuelve al emisor y la posición aceptada se publica a los
demás bots.

Para una comprobación automatizada corta,
`CITADEL_STRESS_DURATION_SECONDS=3` reemplaza temporalmente la duración
elegida. La aplicación sigue pidiendo los minutos durante el uso normal; ese
override sólo evita ejecutar un minuto entero en un smoke test.
`CITADEL_STRESS_FORCE_BLOCKED=1` fuerza el primer movimiento a un obstáculo
conocido, para comprobar que el Lua devuelve `move_rejected`; no se usa en
pruebas de carga normales.

## Compilar el cliente para otra plataforma

El cliente es un crate Rust independiente y no contiene código específico de
Windows. Instala el target y compílalo, por ejemplo:

```powershell
cd client
rustup target add x86_64-pc-windows-msvc
cargo build --release --target x86_64-pc-windows-msvc
```

En un host Linux o macOS se usan los targets correspondientes. Si se hace
cross-compilación, también se necesita el linker/SDK de esa plataforma. La
copia de `server/citadel.exe` es intencionalmente sólo para Windows; el binario
del servidor para Linux o macOS se compila en esos hosts o desde la matriz de
release del repositorio.

## Consideración de capacidad

Con 1000 bots y el intervalo predeterminado de 250 ms, se procesan cerca de
cuatro millones de estados de peer por segundo antes de contar acknowledgements
y logs. Los snapshots reducen de forma drástica la cantidad de datagramas, pero
el JSONL conserva cada estado recibido para el análisis; gzip evita que esa
evidencia ocupe varios GB por minuto.
Usa una unidad rápida y prueba primero con 10--50 bots. El modo detallado
muestra `jugador-local=<bot>` y `jugador-externo=<id>` con colores por acción;
mantenerlo apagado a gran escala evita que la terminal sea el cuello de botella.
