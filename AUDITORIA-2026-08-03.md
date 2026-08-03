# Auditoría técnica de Citadel — 3 de agosto de 2026

**Commit auditado:** `77e6862` (rama `develop`, versión `0.9.14` en `Cargo.toml`, último tag `v0.9.13`)
**Alcance:** arquitectura y estructura de carpetas, seguridad, calidad de código Rust, developer experience, CI/CD, testing, documentación y proceso de release.
**Método:** cuatro análisis independientes en paralelo sobre el árbol completo, con verificación manual de los hallazgos críticos.

---

## Índice

1. [Resumen ejecutivo](#1-resumen-ejecutivo)
2. [Métricas del proyecto](#2-métricas-del-proyecto)
3. [Lo que ya está bien hecho](#3-lo-que-ya-está-bien-hecho)
4. [Seguridad](#4-seguridad)
5. [Arquitectura y estructura de carpetas](#5-arquitectura-y-estructura-de-carpetas)
6. [Calidad de código Rust](#6-calidad-de-código-rust)
7. [CI/CD y gates de verificación](#7-cicd-y-gates-de-verificación)
8. [Testing](#8-testing)
9. [Documentación](#9-documentación)
10. [Developer experience y velocidad del ciclo de desarrollo](#10-developer-experience-y-velocidad-del-ciclo-de-desarrollo)
11. [Proceso de release y distribución](#11-proceso-de-release-y-distribución)
12. [SDKs de cliente](#12-sdks-de-cliente)
13. [Plan de acción priorizado](#13-plan-de-acción-priorizado)

---

## 1. Resumen ejecutivo

Citadel es un proyecto **técnicamente muy sólido** con una disciplina de ingeniería que está por encima de la media: cero `.unwrap()` en código de producción, cero `TODO`/`FIXME`, cobertura del 100 % de documentación a nivel de módulo, `unsafe_code = "forbid"` en todo el workspace excepto la capa FFI (justificada y auditada), inyección de dependencias con `Arc<dyn Trait>` de forma consistente, y 1.400 tests.

Los problemas no están en el código del *data path* del juego. Se concentran en tres áreas muy concretas:

**a) La superficie de operador/admin es el eslabón débil de seguridad.** Mientras que la autenticación de jugadores está implementada correctamente (Argon2id con parámetros OWASP, tokens de 256 bits desde el CSPRNG del sistema, rate limiting por origen y por email), la consola de administración tiene credenciales por defecto `admin`/`password` que arrancan sin bloqueo, genera sus bearer tokens con SipHash en lugar de un CSPRNG, y no tiene ningún rate limiting en el login. Los tres se combinan en un único vector: fuerza bruta a velocidad de red contra una contraseña conocida.

**b) El gate de verificación no es el que se ejecuta en CI.** El trigger de `ci.yml` apunta a `main`/`master`, ramas que **no existen** en este repositorio (la rama por defecto es `develop`). El propio `AGENTS.md` autoriza pushes directos a `develop`. El resultado es que el flujo de trabajo sancionado no ejecuta CI en absoluto. Además, de los 11 gates de `scripts/check.sh`, CI solo ejecuta 5, y omite `--workspace` tanto en clippy como en test, dejando 17.634 líneas y 221 tests de `crates/*` sin cubrir.

**c) El árbol `docs/architecture/` y `docs/features/` no existe, pero 54 referencias apuntan a él.** Incluyen `Cargo.toml`, la plantilla de pull request (que pide marcar casillas sobre directorios inexistentes), rustdoc de ~25 módulos, `citadel.toml`, un script Lua que **se distribuye dentro del ZIP de release al usuario final**, y cuatro páginas publicadas de la web.

Ninguno de los tres es un problema de diseño de fondo. Los tres son mecánicamente reparables sin tocar el comportamiento del sistema.

**Valoración global:** *un código disciplinado con un layout de archivos indisciplinado y una red de seguridad de CI que no está conectada.*

---

## 2. Métricas del proyecto

| Ámbito | Archivos `.rs` | Líneas |
|---|---|---|
| `src/` (servidor: lib + bin) | 160 | 112.137 |
| `crates/` (9 crates) | 41 | 17.634 |
| `tests/` (integración) | 58 | 17.368 |
| `tools/bot-stress-simulator/` (fuera del workspace) | 3 | 2.817 |
| **Total Rust** | **263** | **~149.900** |

De las 112.137 líneas de `src/`, unas **31.677 son código de test inline** (≈28 %), dejando ~80.500 líneas de producción.

**Tests:** 1.400 funciones de test en total (987 inline en `src/`, 192 en `tests/`, 221 en `crates/`).

**Código no-Rust:** Unreal C++ 5.858 LOC, Unity C# 1.812, JS SDK 1.760, Godot GDScript 1.441, más `src/http/assets/console.html` con 2.289 líneas embebidas vía `include_str!`.

**Orquestación de build:** `Makefile` 699 líneas + `make.ps1` 1.500 líneas + `make.bat` 9 líneas = dos implementaciones paralelas del mismo task runner.

---

## 3. Lo que ya está bien hecho

Esta sección existe porque es el estándar contra el que hay que medir el resto. Nada de lo siguiente debería tocarse:

**Seguridad de la ruta de jugador:**
- Hashing de contraseñas correcto: `src/services/memory.rs:64-81` usa Argon2id, `Version::V0x13`, m=19 MiB / t=2 / p=1 / salida de 32 bytes — cumple exactamente el mínimo OWASP — con salt de 16 bytes fresco desde `getrandom` por contraseña y parámetros codificados en PHC para permitir upgrades deliberados.
- Tokens de sesión fuertes: `src/services/token.rs:100-125` genera 256 bits de entropía del SO por token de acceso y de refresco, más un `token_ref` independiente de 128 bits.
- Sin oráculo de credenciales: `src/services/memory.rs:57-60` colapsa usuario desconocido, deshabilitado, tombstoned y ausente en un único `authentication_failed()`.
- Rate limiting con claves que preservan privacidad (digests SHA-256, nunca email o IP en claro — `src/services/auth_rate_limit.rs:58-70`) y que **ignoran deliberadamente `X-Forwarded-For`** con una justificación explícita en `src/http/auth.rs:340-346`. Es la decisión correcta en ausencia de un proxy autenticado.

**Defensa SSRF de primer nivel** — `src/runtime/outbound_http.rs`. El DNS se resuelve una vez y se **fija** vía `resolve_to_addrs` antes de conectar (`:400-408`), lo que cierra estructuralmente el TOCTOU de DNS rebinding que derrota a la mayoría de implementaciones ingenuas. Añade `Policy::none()` (sin seguir redirects), `.no_proxy()`, rechazo de IP literales y de credenciales en URL, allowlists de host y puerto, límites de tamaño de respuesta, rate limiting por minuto y semáforo de concurrencia. La comprobación de rangos privados (`:595-640`) cubre loopback, RFC1918, link-local, CGNAT 100.64/10, TEST-NET, benchmark, 240/4 y —en IPv6— ULA, site-local, Teredo, 6to4, documentación **y direcciones IPv4-mapped de forma recursiva**.

**Cero inyección SQL.** Todo `src/repository/` usa `sqlx::query(...).bind(...)`. El explorador de base de datos resuelve cada tabla contra una allowlist de metadatos fresca *antes* de interpolar (`src/database_explorer.rs:1194-1200`), restringe columnas de filtro/orden/PK a las presentes y no sensibles, entrecomilla identificadores y bindea todos los valores.

**Tipos secretos irrevelables por construcción** — `SessionTokenSecret`, `Password`, `PasswordVerifier`, `EmailAddress` tienen `Debug` redactado a mano, sin `Display` ni `Serialize`. `SessionTokenSecret` incluso renuncia a derivar `Hash` para no abrir un segundo canal de observación (`src/session/token.rs:34-36`).

**Plano de control mTLS bien hecho** — `src/matchmaker_transport.rs:454-476` combina `WebPkiClientVerifier` contra una CA suministrada por el operador **más** pinning de huella del certificado hoja por peer. Sin fallback a self-signed; la configuración falla en duro si faltan CA/cert/key con clustering habilitado.

**Manejo de errores ejemplar.** `anyhow` está confinado a `src/main.rs` (4 apariciones, cero en el resto de `src/` y `crates/`). Cero `.unwrap()`, cero `panic!`, cero `todo!`/`unimplemented!` en producción. Las 41 llamadas a `.expect()` de producción documentan invariantes reales, no son placeholders.

**El sistema de paridad de SDKs es la mejor pieza de diseño del repo.** `crates/citadel-wire/contract.json` (`abi_version: 3`) es la tabla canónica de constantes, mantenida honesta por un test de staleness; cada SDK declara su `sdk.manifest.json`; `check_sdk_parity.py` los diferencia con **descubrimiento por glob**, de modo que añadir un motor no requiere editar el script. Encima hay un manifiesto de completitud de features y una matriz de capacidades que genera la tabla del README. La arquitectura es correcta; lo único que le falta es que CI la ejecute.

**Empaquetado Docker sólido:** usuario no-root `citadel` (uid 10001, `/usr/sbin/nologin`), `tini` como PID 1, imagen runtime `debian-slim`, y un `.dockerignore` deny-by-default (`**` seguido de allowlist explícita) que excluye estructuralmente `.env`, proyectos de juego y bases de datos del contexto de build.

**`citadel.toml` y `.gitignore`** explican el *porqué* de cada entrada. Ese estándar de justificación inline está presente en todo el código y es lo que hizo esta auditoría viable.

---

## 4. Seguridad

### 4.1 Hallazgos ALTOS

#### H1 — Credenciales de administración por defecto (`admin` / `password`) que arrancan habilitadas

`src/config/mod.rs:1415-1424` define `ConsoleConfig::default()` con `username: "admin"`, `password: "password"`. El `citadel.toml` versionado **no tiene sección `[console]`**, así que estos valores aplican a cualquier despliegue de tipo "descarga y ejecuta".

La única mitigación es informativa: `src/startup.rs:539-545` imprime un `WARNING`. `ConsoleConfig::validate()` (`src/config/mod.rs:1437-1455`) solo rechaza contraseñas *vacías*, nunca la de defecto.

Esas credenciales dan lectura y escritura completa sobre `/console/v1/*`: cuentas (incluyendo ban, borrado y exportación), ajuste de wallet, chat, grupos, el explorador de base de datos y RPC del runtime. `HttpConfig` liga por defecto a `127.0.0.1:7350`, pero `citadel.toml:21` invita explícitamente a usar `0.0.0.0:7350`.

**Corrección:** negarse a arrancar cuando `uses_default_credentials()` sea cierto *y* `http.bind` no sea loopback; o generar una contraseña aleatoria en el primer arranque e imprimirla una sola vez.

#### H2 — Los bearer tokens de consola no se generan con un CSPRNG

`src/services/console.rs:200-218`:

```rust
fn random_token() -> String {
    let nanos = SystemTime::now()...as_nanos();
    for lane in 0u64..4 {
        let word = RandomState::new().hash_one((nanos, std::process::id(), lane));
        token.push_str(&format!("{word:016x}"));
    }
}
```

Tres problemas que se componen:

1. **Primitiva no criptográfica.** `RandomState` es SipHash-1-3, que la propia biblioteca estándar documenta como *no* criptográficamente segura.
2. **Las cuatro "lanes" no son independientes.** El comentario de la función afirma que son "cuatro estados independientes" con "entropía fresca del SO por construcción". No es así como funciona `std`: `RandomState::new()` lee un `(k0, k1)` thread-local sembrado **una vez por hilo** y después incrementa `k0` en 1 por llamada. Las cuatro lanes son por tanto SipHash bajo *claves relacionadas* `(k0..k0+3, k1)`, y cada token posterior en ese hilo continúa la misma secuencia.
3. **Entradas públicas de baja entropía.** `nanos` es aproximadamente conocido por el solicitante y `process::id()` suele ser adivinable. Lo único secreto es la semilla de 128 bits del hilo, reutilizada en cada token que ese hilo emite.

Consecuencia práctica: quien obtenga un token legítimo (por ejemplo su propio token de `viewer`) observa cuatro salidas de SipHash bajo claves relacionadas conocidas y preimágenes conocidas, lo que ayuda materialmente a recuperar o predecir un token de `admin` emitido cerca en el mismo hilo.

La justificación del comentario ("no se introduce una dependencia `rand` para este único call site") no aplica: **`getrandom` ya es dependencia directa** y se usa correctamente en el resto del código, incluido `src/services/token.rs:100-106`.

**Corrección:** sustituir el cuerpo por `getrandom::fill(&mut [0u8; 32])`, igual que `RandomTokenIssuer::random_b64`. Es una corrección de una línea y la de mayor ratio severidad/esfuerzo de todo el informe.

#### H3 — El login de consola no tiene rate limiting ni bloqueo

`src/http/console_api/mod.rs:343-388` llama a `verify_login` directamente. Los fallos se registran en auditoría (`:361-368`) pero nunca se limitan.

La asimetría es llamativa: la superficie de autenticación *de jugador* sí está protegida (`src/http/auth.rs:348-386` → `AuthenticationRateLimitPolicy` con ventanas fijas por origen y por email). La superficie de *administración* — con credenciales más débiles (H1) y sin KDF (M4) — no tiene ninguna.

Combinado con H1, un atacante no autenticado puede hacer fuerza bruta contra la contraseña de operador a velocidad de red. El comentario en `:359-360` ("para que los operadores vean la presión de fuerza bruta en el rastro") reconoce el ataque, pero se apoya en un ring en memoria de 1.024 entradas (M6) para exponerlo.

**Corrección:** enrutar el login de consola por el helper `admit(...)` existente con clave `peer_source`, más un backoff por usuario.

#### H4 — Certificados autofirmados de desarrollo sirven tráfico de producción en silencio

`src/transport/mod.rs:730-746` (QUIC) y `:769-784` (WebTransport):

```rust
let cert = if cfg.tls.is_configured() {
    quic::SelfSignedCert::from_pem(...)?
} else {
    quic::SelfSignedCert::generate(&["localhost".to_string()])?
};
```

`TransportTlsConfig` tiene `None`/`None` por defecto y su `validate()` trata ese par como válido. La única señal es un `tracing::info!(... tls = "development self-signed" ...)` en `:750` — nivel info, no warning, y no falla el arranque.

El certificado generado tiene SAN solo para `localhost`, así que ningún cliente real puede validarlo. Eso presiona a los integradores a desactivar la verificación en el cliente, convirtiendo un hueco de configuración en una exposición MITM de toda la flota.

**Corrección:** fallar en duro cuando un listener de transporte liga a una dirección no-loopback sin PEM configurado; como mínimo, escalar a `warn!` y exponerlo en `/status`.

#### H5 — Toda la superficie HTTP, incluida la consola de administración, es HTTP en claro sin opción de TLS

`src/config/mod.rs:1363-1374` — `HttpConfig` tiene exactamente un campo, `bind`. No existe ajuste de certificado, clave ni `https` en ninguna parte del stack HTTP. `src/http/mod.rs:100-118` liga un `TcpListener` pelado.

La SPA de administración se sirve en `/dashboard` y su login hace `POST` de la contraseña de operador a `/console/v1/login`, recibiendo un bearer token que guarda en `sessionStorage`. En cualquier despliegue no-loopback sin un terminador TLS externo, **la contraseña de administración y todos los bearer tokens subsiguientes viajan por la red en texto plano**, junto con todos los tokens de sesión de jugador emitidos por `/v1/*`.

El `EXPOSE 7350/tcp` del Dockerfile y su healthcheck confirman que HTTP plano es el puerto de servicio previsto.

**Corrección:** o bien añadir terminación rustls nativa a `HttpConfig`, o bien negarse a servir `/dashboard` y `/console/v1/*` en un bind no-loopback salvo que el operador ponga un `behind_tls_proxy = true` explícito.

### 4.2 Hallazgos MEDIOS

| ID | Hallazgo | Detalle |
|---|---|---|
| **M1** | Credenciales de terceros reales en el árbol de trabajo | `.env:1-3` contiene host, email y contraseña reales de PocketBase. **Verificado:** está en `.gitignore`, **no** está trackeado, y **nunca** apareció en el historial. Riesgo residual: disco local y sincronización de backups. **Rotar esa credencial.** |
| **M2** | El transporte WebSocket de fallback no tiene TLS ni comprobación de `Origin` | `src/transport/websocket/mod.rs:236` usa `accept_async` sobre TCP crudo. Los tokens de sesión se presentan en el primer frame (`:248-252`), así que todo cliente de navegador en fallback transmite su bearer en claro. Además hereda los defaults de tungstenite: **64 MiB por mensaje / 16 MiB por frame**, por conexión, sin ningún tope de `max_connections` en la capa de transporte. |
| **M3** | Sin cabeceras HTTP de seguridad ni CSP en la SPA de consola | Cero coincidencias en todo `src/` para `CorsLayer`, `Content-Security-Policy`, `X-Frame-Options`, `Strict-Transport`, `nosniff`. El router se compone sin ningún middleware: sin `TimeoutLayer`, sin `DefaultBodyLimit` global. Se devuelve una SPA de 114 KB con JavaScript inline sustancial sin CSP ni protección de clickjacking sobre mutaciones de administración. |
| **M4** | Las contraseñas de operador se almacenan y comparan en texto plano | `src/config/mod.rs:1387-1397` las guarda como `String` desde TOML o entorno. La comparación en sí está bien hecha (`constant_time_eq` en `:94-99`), pero sin KDF cualquiera que lea `citadel.toml` o el entorno del proceso obtiene la credencial. Contrasta con la ruta de jugador, que sí lo hace correctamente. |
| **M5** | El rol `viewer` de consola puede leer todas las tablas, incluida PII | En `src/http/console_api/database.rs`, `require_admin()` **no aparece en ningún sitio**. La redacción de columnas es solo una heurística por nombre (`src/database_explorer.rs:1553-1567`, busca `password`/`secret`/`token`/`apikey`/`credential`), así que `email`, `mfa_seed`, `pin`, `recovery_hash`, `ip_address` o `dob` se devuelven completos. |
| **M6** | El log de auditoría es en memoria, tope de 1.024 entradas, y se pierde al reiniciar | `src/services/audit.rs:8-11`. Como los logins fallidos de consola son la *única* señal de fuerza bruta (H3), un atacante que genere ~1.024 peticiones hace rotar todo el rastro, incluida la evidencia de su propio login exitoso. |
| **M7** | Sin escaneo de vulnerabilidades de dependencias | No existe `deny.toml` ni `audit.toml`, y no hay referencia a `cargo-audit`/`cargo-deny`/RUSTSEC en workflows, `Makefile`, `make.ps1` ni `scripts/`. Además, la integración de **gitleaks** vive solo en `.githooks/pre-push`, que requiere opt-in manual — un colaborador que nunca instale el hook se salta el escaneo de secretos por completo. |
| **M8** | El deadline del runtime Python es evadible, y CPython no es sandboxeable | `src/runtime/python.rs:595-607` implementa el deadline con `sys.settrace`. El código de script puede llamar a `sys.settrace(None)` o reasignar el global `_deadline_at` para quitarse su propio deadline. Es inherente a CPython embebido y está mitigado porque `runtime-python` no es feature por defecto, pero debe documentarse como nivel *solo para código de confianza*. Contrasta con el runtime JS, que sí está bien acotado (límite de heap de 64 MiB y de pila de 512 KiB en `src/runtime/js.rs:2546-2547`). |

### 4.3 Hallazgos BAJOS

- **L1** — El modo `Trusted` de Lua concede `StdLib::ALL_SAFE`, es decir `io`, `os`, `package` (y por tanto `os.execute`/`io.popen`). Correctamente opt-in: el defecto es `Sandboxed`, y el modo sandbox omite deliberadamente `coroutine` y `debug` para que el hook de deadline no pueda evadirse, con test de regresión en `src/runtime/lua.rs:4602`.
- **L2** — Comentario obsoleto en `src/transport/quic/tls.rs:8-10`: documenta un `insecure_client_config` que ya no existe en el archivo. Inofensivo pero describe mal la postura de seguridad.
- **L3** — Comentario incorrecto en `src/services/console.rs:202-206` (ver H2).
- **L4** — `validate_identifier` (`src/database_explorer.rs:2535-2547`) acepta *cualquier* carácter no de control, confiando enteramente en el doblado de comillas. El entrecomillado es correcto y los identificadores se resuelven contra metadatos, así que no es explotable, pero una allowlist positiva de charset sería más barata de auditar.
- **L5** — `crates/citadel-client-ffi` está exento de `unsafe_code = "forbid"`, documentado y justificado. **Revisado el ABI C completo: no se encontró ningún patrón unsound.** Cada punto de entrada comprueba nulls antes de `from_raw_parts`, envuelve el cuerpo en `guard()` → `catch_unwind` y lleva documentación `# Safety`.
- **L6** — `docker-compose.crdb.yml` usa `start-single-node --insecure` y publica la UI de admin de CockroachDB en el 8080. Está claramente etiquetado como fixture desechable, pero liga a todas las interfaces.
- **L7** — Los defaults de realtime son `require_auth = false, allow_guests = true`. Deliberado para el relay de demo, y la ruta de token inválido falla cerrada en lugar de degradar a invitado. Producción debe invertirlo.
- **L8** — `OutboundHttpClient::execute_blocking` usa `block_in_place`, dejando que un script ocupe un worker del runtime durante el timeout de la petición. Acotado por el semáforo de concurrencia y el rate limit.
- **L9** — La validación de token de consola es un `HashMap::get` sobre el string crudo, no de tiempo constante. Irrelevante una vez corregido H2.

---

## 5. Arquitectura y estructura de carpetas

### 5.1 Layout del workspace

El `Cargo.toml` raíz declara `members = ["crates/*"]` con el **paquete raíz `citadel` haciendo de servidor**.

| Crate | LOC | Rol | Dependencias |
|---|---|---|---|
| `citadel-wire` | 7.593 | Formato de wire: envelope, codec, bit-packing, replicación netpeer | ninguna (hoja limpia) |
| `citadel-map` | 697 | Formato `.cmap` de colisión/nivel | ninguna (hoja limpia) |
| `citadel-nav` | 380 | Bake/validate/pathfind de navmesh | `citadel-map` |
| `citadel-physics` | 2.010 | Character controller, BVH, raycast | `citadel-map` |
| `citadel-tmx` | 300 | Importador Tiled TMX → `citadel-map` | `citadel-map` |
| `citadel-process-env` | 19 | Shim edición 2021 para mutar `std::env` | ninguna |
| `citadel-client` | 1.900 | SDK cliente en Rust | `citadel-wire` + dev-dep en raíz |
| `citadel-client-ffi` | 4.281 | ABI C para Unity/Unreal/Godot | `citadel-client`, `citadel-wire`, **dep normal en la raíz** |
| `demo-client` | 454 | Demo de terminal | `citadel-wire`, `citadel-client` |

El clúster `wire / map / nav / physics / tmx` es un DAG genuinamente bien factorizado, sin ciclos ni aristas de vuelta hacia el servidor.

### 5.2 El problema: `citadel-client-ffi` depende del servidor completo

```toml
# crates/citadel-client-ffi/Cargo.toml
citadel = { path = "../.." }   # dependencia NORMAL, no dev
```

Su único uso son dos tipos, en `crates/citadel-client-ffi/src/transform_ffi.rs:10`:

```rust
use citadel::realtime::transform::{RemoteWorldView, TransformState};
```

Esto significa que **compilar el `citadel_client_ffi.dll` que se distribuye dentro de Unity, Unreal y Godot compila el servidor de juego entero**: sqlx (Postgres + SQLite bundleado), mongodb, axum, quinn, web-transport, **mlua con Lua 5.4 vendored compilado desde fuentes**, sysinfo, argon2, sentry, reqwest. Es un impuesto de tiempo de build y de tamaño de binario en cada release de SDK, por dos structs. Y es, en efecto, *un SDK de cliente dependiendo del servidor*.

**Que el servidor sea el paquete raíz es un problema por tres vías concretas:**

1. Fuerza el `path = "../.."` de arriba; no hay forma de depender solo de "la reconstrucción de transform" sin arrastrar el servidor completo.
2. Impide una tabla `[workspace.dependencies]`: `tokio`, `quinn`, `rustls`, `bytes`, `tokio-tungstenite`, `futures-util`, `reqwest` y `serde` se declaran **dos veces** (raíz + `citadel-client`) con conjuntos de features independientes, lo que es riesgo real de deriva.
3. `cargo build` en la raíz compila el servidor por defecto, así que cualquier flujo que solo toque `crates/*` paga igualmente la resolución de features del manifiesto raíz.

**Corrección:** extraer `RemoteWorldView`/`TransformState` a un nuevo crate hoja `crates/citadel-transform` del que dependan tanto el servidor como el FFI. Después, mover el servidor a `crates/citadel-server/` y convertir la raíz en manifiesto virtual.

### 5.3 Los módulos-dios

| Archivo | Líneas totales | Líneas de producción | Veredicto |
|---|---|---|---|
| `src/realtime/gateway.rs` | **8.487** | 5.336 | **El peor con diferencia** |
| `src/repository/mongodb.rs` | 5.976 | 5.833 | Backend en un solo archivo |
| `src/runtime/lua.rs` | 5.500 | 3.543 | |
| `src/runtime/js.rs` | 4.579 | 3.579 | |
| `src/runtime/python.rs` | 4.216 | 3.194 | |
| `src/database_explorer.rs` | 3.687 | 2.564 | Archivo plano, debería ser carpeta |
| `src/config/mod.rs` | **2.950** | 2.108 | **42 tipos de config en un archivo** |
| `src/transport/mod.rs` | 1.495 | 1.405 | **Raíz de composición encubierta** |

**`src/realtime/gateway.rs` es un objeto-dios de manual.** `pub struct Gateway` (línea 2086) tiene **20 campos, de los cuales 8 son `Option<...>`** (`runtime`, `transform`, `rep`, `domain`, `durable_parties`, `cluster_matchmaker`, `live_matchmaker`…), lo que significa que la mayoría de métodos empiezan con una rama `None`. Un solo bloque `impl Gateway` abarca las líneas **2158–5266 (~3.100 líneas, 85 métodos)**, y un `impl DomainRpcServices` abarca **281–2028 (~1.750 líneas, 55 métodos)** — una tabla de despacho RPC escrita a mano que duplica la superficie de la capa de servicios.

**`src/transport/mod.rs` tiene un desajuste entre su documentación y la realidad.** Su doc de módulo dice ser una *"abstracción de transporte agnóstica del wire… intencionadamente mínima"*. En realidad es **la raíz de composición del sistema**: `start_enabled` va de la línea 328 a la 807 —una única función de ~480 líneas— y construye los runtimes Lua/JS/Python, el transform hub, la autoridad netpeer, el matchmaker (local, cluster y live), el clúster de presencia de chat, el renovador de leases y todos los listeners.

**`src/startup.rs` (1.038 líneas) está mal nombrado:** contiene solo el asistente interactivo de primer arranque, el scaffolding de scripts y el banner ASCII. Ningún bootstrap de servidor vive ahí. La raíz de composición real está repartida entre cuatro archivos (`app.rs`, `http/mod.rs`, `transport/mod.rs`, `startup.rs`) sin un punto de entrada evidente.

### 5.4 Inconsistencia sistemática de granularidad

- Features de **≤ 400 líneas** tienen carpeta (`identity/` con 3 archivos, `session/` con 5, `maps/mod.rs` solo en su carpeta).
- Features de **1.000–8.500 líneas** son archivos planos (`matchmaker_transport.rs` 1.731, `chat_cluster.rs` 1.304, `deferred_storage.rs` 1.341, `database_explorer.rs` 3.687).
- `src/config/` es una carpeta que contiene exactamente un archivo de 2.950 líneas — la carpeta no aporta nada. Igual con `src/storage/` y `src/maps/`.
- La familia `matchmaker*` son **4 archivos planos hermanos que suman 4.600 líneas** y claramente quieren ser `src/matchmaker/{queue,cluster,live,transport}.rs`.

**Regla recomendada:** una feature de ≥ ~800 líneas es una carpeta; una carpeta con un solo archivo vuelve a ser un archivo plano.

### 5.5 Inyección de dependencias: la parte más fuerte del diseño

`Arc<dyn Trait>` se usa de forma penetrante y deliberada — el `Cargo.toml` raíz incluso documenta *por qué* `async-trait` es necesario para la dyn-compatibilidad. Hay 282 usos de `Arc<dyn …>`, con inyección de reloj (`Arc<dyn Clock>`) y constructores `App::with_backend` / `App::with_auth_clock` que hacen todo el sistema testeable. Es la razón por la que existen 1.400 tests.

**Tres olores, aun así:**

1. **`Backend` es un service locator.** `src/repository/backend.rs:143` expone ~14 accesores `fn *_repository()`. Cada feature nueva ensancha un trait implementado 4 veces. Tiene forma de DI pero comportamiento de localización de servicios: los consumidores alcanzan `App → Backend → repository` en lugar de recibir el repositorio que necesitan.

2. **Cuatro inversiones de capas concretas:**

| Violación | Evidencia |
|---|---|
| repository → feature de admin | `src/repository/backend.rs:34`, `pg/mod.rs:44`, `sqlite/mod.rs:55`, `mongodb.rs:26` importan `crate::database_explorer` — la capa de persistencia depende de una feature de consola de operador |
| services → objeto-dios de realtime | `src/services/session_revocation.rs:6`: `use crate::realtime::gateway::Gateway;` |
| services → matchmaker/party | `src/services/matchmaker_directory.rs:16-17`, `party_directory.rs:10` |
| config → runtime/storage/deferred | `src/config/mod.rs:22-23`, `:488`, `:882-883` — config debería ser hoja y aquí es un hub |

3. **Nada refuerza el layering.** `lib.rs` declara los 25 módulos como `pub mod` planos, con `mod validate;` como único privado. En todo `src/**` hay 1.658 items `pub` contra solo 62 `pub(crate)`.

Midiendo el consumo externo real (grep de `citadel::<mod>` en `tests/`, `crates/` y `examples/`), **siete módulos tienen cero consumidores externos y tampoco los usa `main.rs`**: `matchmaker_transport` (1.256 LOC), `matchmaker_live` (1.077), `matchmaker` (842), `matchmaker_cluster` (727), `deferred_storage` (~790), `party` (~300), `host_telemetry` (~300). Son **~5.300 líneas de API pública comprometida con semver sin ningún consumidor**.

### 5.6 Otros directorios

- **`migrations/` (15) vs `migrations-crdb/` (15) vs `migrations-sqlite/` (14)** — 44 archivos SQL. Comparando pg contra crdb con comentarios y espacios eliminados: **14 de 15 tienen SQL genuinamente distinto** (divergencia real de dialecto: `text COLLATE "C"` omitido, `GENERATED ALWAYS AS IDENTITY` → `unique_rowid()`, bien documentado). Solo **1 de 15** es duplicación pura. Así que la triplicación está mayormente justificada; la recomendación es estrecha: compartir los archivos neutros de dialecto y añadir un test que afirme que los tres directorios describen el mismo esquema lógico.
- **`manifests/`** (antes `docs/`, **renombrado el 2026-08-03**) contiene exactamente dos archivos JSON. No es documentación, es un directorio de datos — y **ambos archivos son de carga estructural**: `manifests/capability-matrix.json` lo consumen `scripts/generate_readme_capability_matrix.py` (que genera la tabla de features del README), `scripts/check-reference-contract.py` y el agente `documentation-author`; `manifests/client-feature-manifest.json` lo consume `scripts/check_client_feature_completion.py`. Además, `scripts/check-docs.sh` lo trata como ubicación válida de documentación interna en su regla 2. La documentación en prosa vive en `website/src/content/docs/` (82 páginas, Astro + Starlight, bien organizada). El nombre anterior prometía prosa y solo contenía datos, lo que alimentaba la confusión con las 54 referencias rotas de §9.1; el renombrado elimina esa ambigüedad.
- **`maps/`** está vacío y sin trackear.
- **`tools/bot-stress-simulator/client/`** declara un `[workspace]` vacío y su propio `Cargo.lock`: **2.817 líneas de Rust que nunca se compilan ni se lintean** por `cargo clippy --workspace`.
- **`clients/unity-quic-demo/`** tiene **39 archivos trackeados y cero archivos `.cs`**. Todos los `.cs.meta` están commiteados pero sus hermanos `.cs` no existen ni en git ni en disco. Peor: su `.gitignore` afirma explícitamente que *"citadel_client_ffi.dll SE commitea a propósito para que la demo funcione tras un clon limpio"* — pero solo el `.meta` está trackeado, el DLL no. **Esta demo no puede abrirse en Unity desde un clon limpio.** Es un duplicado de `clients/unity/` de todos modos.
- **`packaging/server/`** contiene una *tercera* copia de scripts de gameplay de arranque, junto a `game/main.lua` y el scaffolder del asistente en `src/startup.rs:485`.

---

## 6. Calidad de código Rust

### 6.1 `party_block_on` — el hallazgo de mayor impacto en runtime

`src/realtime/gateway.rs:95-108`:

```rust
fn party_block_on<T: Send + 'static>(
    future: impl Future<Output = crate::error::AppResult<T>> + Send + 'static,
) -> crate::error::AppResult<T> {
    std::thread::spawn(move || {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|error| crate::error::AppError::internal(error.to_string()))?
            .block_on(future)
    })
    .join()
    .map_err(|_| crate::error::AppError::internal("party directory worker panicked"))?
}
```

Esto **lanza un hilo del SO nuevo y construye un runtime Tokio completo en cada llamada**, y luego bloquea en `join()`. Se invoca desde 14 puntos de producción, todos alcanzables desde la ruta síncrona de despacho de RPC entrante (`Gateway::handle_inbound`): `:3258`, `:3276`, `:3347`, `:3355`, `:3439`, `:3475`, `:3504`, `:3528`, `:3631`, `:3642`, `:3706`, `:3771`, `:4011`, `:2365`.

El coste es aproximadamente el de crear un hilo más construir un runtime (~50–200 µs, más el setup de reactor y timers) **por cada RPC de party**, encima de la latencia real de almacenamiento. También impide la reutilización de conexiones y el batching de timers, y cada runtime creado muere de inmediato.

Lo notable es que **el patrón correcto ya existe dos módulos más allá**: `src/runtime/host_services.rs:364` y `src/runtime/outbound_http.rs:583` usan `tokio::task::block_in_place(|| Handle::current().block_on(fut))`, con una justificación detallada en la documentación del módulo. La ruta del gateway debería capturar un `tokio::runtime::Handle` en construcción y llamar a `handle.block_on(...)`, que es válido desde un hilo del blocking pool, que es exactamente donde corre el tick del gateway (vía `spawn_blocking` en `src/realtime/tick.rs:396`).

### 6.2 El resto del código async está limpio

- **Ningún lock bloqueante retenido a través de un `.await`.** Un escaneo heurístico produjo 20 candidatos y **los 20 son falsos positivos** verificados manualmente: los guards están correctamente acotados y liberados.
- **La elección de mutex es correcta y deliberada:** 57 locks de `std::sync` (todos en máquinas de estado síncronas) contra 16 de `tokio::sync`, y estos últimos aparecen exactamente donde un guard *debe* cruzar un await (sesiones de transacción de BD).
- **`spawn_blocking` se usa correctamente** para el trabajo genuinamente bloqueante: ticks de la VM de Lua, hot-reload de scripts, recolección de sysinfo.
- **El apagado ordenado está bien diseñado:** `CancellationToken` sobre `watch` y un `Supervisor` que cancela y luego hace join de todas las tareas, exponiendo el primer error y tratando un join con panic como `Internal`.

**Hueco:** `Supervisor::shutdown` (`src/lifecycle.rs:177-210`) hace join de cada `JoinHandle` **sin timeout**. Un servicio que ignore su `CancellationToken` cuelga el apagado del proceso indefinidamente.

**Menor:** el listener de control del matchmaker duerme 5 ms fijos en `WouldBlock` *y en cualquier error* (`src/matchmaker_transport.rs:672-674`). El `Err(_) => sleep(5ms)` incondicional gira a 200 Hz contra un listener que falle de forma persistente.

### 6.3 Clonado y asignaciones: no es un problema

817 `.clone()` de producción en ~98k LOC. Las rutas calientes son *más ligeras* de lo sospechado: 52 clones en 5.337 líneas de gateway es densidad baja. `Arc::clone(&x)` en forma explícita se usa 333 veces (la forma que prefiere clippy, distingue incrementos de refcount baratos de copias profundas). **No se recomienda ninguna acción aquí** — está muy por debajo de `party_block_on` en prioridad.

### 6.4 Duplicación

**La división del matchmaker es descomposición legítima, no copy-paste.** Cada módulo tiene una responsabilidad distinta y documentada, y sus superficies públicas no comparten ningún símbolo.

**Duplicación real #1: el helper de validez de lease está escrito seis veces**, con semántica idéntica y solo el orden de operandos cambiando:

- `src/chat_cluster.rs:51` → `self.expires_at > now`
- `src/matchmaker_cluster.rs:52` → `now < self.expires_at`
- `src/repository/chat.rs:184` → `self.expires_at > now`
- `src/runtime/cache_lease.rs:34` → `now < self.expires_at`
- `src/services/party_directory.rs:44` → `self.expires_at > now`
- `src/session/ownership.rs:51` → `now < self.expires_at`

Dado que la corrección del fencing es crítica aquí, un único `trait Lease` con un `is_current_at` por defecto haría la condición de frontera (estricta vs. inclusiva) uniforme de forma demostrable.

**Duplicación real #2:** el SPA de consola son 2.289 líneas de HTML embebidas vía `include_str!`, sin herramientas de lint, formato ni test, con solo dos aserciones de humo.

### 6.5 Lints y dependencias

El `Cargo.toml` raíz define `unsafe_code = "forbid"` más `panic`/`todo`/`unimplemented`/`unwrap_used = "warn"`, y CI aplica `-D warnings`.

**Pero no hay `[workspace.lints]`.** El mismo bloque de 4 líneas está copiado en 7 manifiestos de crate, y **`citadel-tmx` y `citadel-process-env` no tienen `[lints]` en absoluto**, así que ahí `unwrap_used`/`panic` no se aplican.

Hay 91 `#[allow]`, con distribución sana. El grupo más significativo son los **32 `clippy::too_many_arguments`**, agrupados densamente en la capa de persistencia y chat: `src/services/chat.rs` tiene 8 y `src/repository/chat.rs` otros 8, con el patrón repitiéndose idéntico en `pg/` y `sqlite/`. Es la señal más clara del código de que unos cuantos paquetes de parámetros quieren ser structs.

**Dependencias duplicadas en `Cargo.lock`** (42 crates con 2+ versiones):

| Crate | Versiones | Nota |
|---|---|---|
| **`reqwest`** | 0.12.28 + 0.13.4 | `sentry 0.48` arrastra la 0.13.4 — **dos stacks HTTP completos enlazados** |
| `rand` | 0.8.7, 0.9.5, 0.10.2 | tres majors |
| `getrandom` | 0.2.17, 0.3.4, 0.4.3 | tres majors |
| `sha2` | 0.10.9 + 0.11.0 | |
| `webpki-roots` | 0.26.11 + 1.0.9 | |
| `windows-sys` | 0.48, 0.52, 0.59, 0.61 | cuatro versiones |

**La buena noticia, y es claramente intencionada:** `rustls` y `ring` aparecen cada uno en **una sola versión**. Los comentarios del `Cargo.toml` muestran que se diseñó así a propósito (`web-transport-quinn` fijado para casar con "nuestro quinn 0.11 / rustls 0.23 exactos sin rotación de versiones", `sqlx` con `tls-rustls-ring` explícitamente "sin rotación de aws-lc-rs", `sentry` con `rustls-no-provider`). El stack criptográfico está unificado; el split de `reqwest` es la única fuga de esa postura por lo demás cuidadosa.

### 6.6 Señales uniformemente positivas

- **Cero `TODO` / `FIXME` / `XXX` / `HACK`** en todo `src/` y `crates/`. Verificado dos veces.
- **Cero código comentado.**
- **100 % de cobertura de documentación de módulo** — los 160 archivos `.rs` de `src/` tienen cabecera `//!` en sus primeras 3 líneas, y explican *razones*, no solo *qué*.
- **560 constantes con nombre** en `src/`, de las cuales 147 son límites, timeouts o capacidades. Prácticamente todo literal `Duration` es o una constante nombrada o un `Default` documentado.

---

## 7. CI/CD y gates de verificación

### 7.1 CRÍTICO: el trigger de push está muerto

```yaml
on:
  pull_request:
  push:
    branches: [main, master]
```

`origin/HEAD -> origin/develop`. **Ni `main` ni `master` existen** (ramas remotas verificadas: `develop`, `release`, dos `codex/*`). Mientras tanto, `AGENTS.md` sanciona explícitamente que *"los cambios de implementación pueden por lo demás commitearse y pushearse directamente a `develop`"*. **El flujo de trabajo autorizado se salta CI por completo.**

**Corrección:** cambiar a `[develop, release]`.

### 7.2 CRÍTICO: `check.sh` ≠ CI — 6 de 11 gates son solo locales

`scripts/check.sh` se describe en el README y en la plantilla de PR como "el comando canónico de verificación local". CI ejecuta solo 5 de sus 11 gates:

| Gate | ¿En CI? |
|---|---|
| `check-runtime-ingress-media.py` | sí |
| `check-docs.sh` | sí |
| `check-sdk-parity.sh` | sí |
| `check-godot-web-sdk.py` | sí |
| `cargo fmt` / `clippy` / `test` | sí, pero con flags más débiles |
| `check-capability-matrix.sh` | **no** |
| `check-client-doc-tabs.sh` | **no** |
| `check-networkpeer-cross-engine.py` | **no** |
| `check-client-feature-completion.sh` | **no** |
| `check-server-release-packages.py` | **no** |
| `check-runtime-parity.sh` | **no** |
| `check-container-assets.sh` | **no** |

Irónicamente, `check-server-release-packages.py` protege exactamente el invariante que el commit de HEAD dice haber arreglado ("mantener los archives de servidor libres de cliente") y no se aplica en CI.

**Corrección de una línea:** sustituir la secuencia de pasos fmt/clippy/test/check-* del job `rust` por `bash scripts/check.sh`. Esto colapsa toda la clase de deriva de golpe.

### 7.3 CI no cubre los crates del workspace

```yaml
- run: cargo clippy --all-targets --all-features -- -D warnings
- run: cargo test  --all-targets --all-features
```

Ambos **omiten `--workspace`**. Como el workspace tiene un paquete raíz y no define `default-members`, Cargo se limita al paquete raíz, así que `crates/*` **no se lintean ni se testean en CI**. Eso excluye silenciosamente **17.634 LOC y 221 funciones de test**, incluyendo `crates/citadel-client-ffi/tests/contract_manifest.rs` — que `scripts/check.sh` documenta explícitamente como *el* guardián de staleness de `crates/citadel-wire/contract.json`. **Ese guardián no corre en CI.**

Además hay tres definiciones distintas de "correr los tests": `Makefile:123` usa `cargo test --workspace` (sin `--all-targets` ni `--all-features`), `make.ps1` usa los tres flags, y CI usa dos de tres. Un desarrollador en Linux que ejecute `make test` se salta en silencio todos los tests de integración y ambos runtimes opcionales.

### 7.4 Lo que falta en CI

- **Sin `cargo-audit`/`cargo-deny`.** Cero escaneo de cadena de suministro para un servidor expuesto a red con quinn, rustls, sqlx, mongodb, pyo3, rquickjs y mlua.
- **Sin escaneo de secretos en CI.** El propio `AGENTS.md` reconoce que *"los hooks locales son una salvaguarda del desarrollador, no un mecanismo de aplicación remota; usa escaneo de secretos en CI como comprobación requerida adicional cuando esté disponible"*. No está.
- **Sin cobertura de código.** Ni `cargo-llvm-cov` ni tarpaulin ni codecov.
- **Sin MSRV.** No hay `rust-version` en *ningún* `Cargo.toml`. El `Dockerfile` fija `RUST_VERSION=1.92.0` mientras `rust-toolchain.toml` dice `channel = "stable"` — la imagen de release y los builds de desarrollo pueden compilar con versiones distintas de Rust sin nada que afirme compatibilidad.
- **Sin `cargo doc` en CI.** Los enlaces intra-doc rotos nunca se detectan.
- **Sin testeo de combinaciones de features.** Solo corre `--all-features`. El **build por defecto (sin features), que es el que se distribuye en el ZIP de release, nunca se compila ni se testea en CI.**
- **Sin job del SDK de JS en `ci.yml`.** Los 6 archivos de test de `clients/js/test/` corren **solo en `release.yml`** — una regresión del SDK se descubre el día del release.
- **Sin build de `website/` en CI.** `check-docs.sh` *obliga* a editar la web para cambios de cara al cliente, pero su build nunca se valida en un PR.
- **Sin `concurrency: cancel-in-progress` ni `timeout-minutes`** en ningún workflow.

### 7.5 Duración de los jobs

El job `rust` está **deliberadamente configurado para ser lento**:

```yaml
- uses: Swatinem/rust-cache@v2
  with:
    cache-targets: false     # los artefactos de target/ NO se cachean
```

Cada PR recompila desde cero: Lua 5.4 vendored, RecastNavigation (C++/CMake), libsqlite3 bundleado, quinn/rustls, sqlx, driver de mongodb, **más** PyO3 y QuickJS por `--all-features`. Después enlaza **~64 binarios de test separados**. Estimación realista: **35–55 min** para `rust`, **12–20 min** para `godot-web-sdk`, y **20–30 min** por pata de `linux-release-packages` (que instala `cross` desde git en cada una, ~5 min antes de compilar nada del proyecto).

Los comentarios sobre presión de disco en `ci.yml:29-34` muestran que el equipo ya chocó con los límites del runner y cambió velocidad por disco. La solución real no es esa: es dividir el job en uno de features por defecto y otro de `--all-features`.

### 7.6 Git hooks

`.githooks/pre-push` está bien diseñado y falla cerrado: ejecuta `check-runtime-ingress-media.py` y luego `gitleaks git --redact --log-opts='--all' .`. Si falta el binario de gitleaks, el push se bloquea. Postura correcta.

Se instala vía `scripts/install-git-hooks.sh` → `git config core.hooksPath .githooks`, pero **no es automático** y el README nunca lo menciona. Un clon fresco no tiene hooks hasta que alguien encuentre y ejecute ese script.

**Inconsistencia:** `AGENTS.md` instruye verificar que exista **`.git/hooks/pre-push`**. Con `core.hooksPath=.githooks`, esa ruta nunca se puebla, así que la verificación documentada comprueba el archivo equivocado y siempre reportará el gate como ausente.

---

## 8. Testing

### 8.1 Lo que funciona

- **Tests de contrato de repositorio.** Un patrón genuinamente bueno: `{chat,friends,groups,leaderboards,notifications,storage,wallet}_repository_contract.rs` ejecutan un mismo cuerpo de contrato contra in-memory, SQLite, Postgres, CockroachDB y MongoDB.
- **Paridad cross-engine/ABI:** guardián de staleness del manifiesto de contrato, `codec_ffi_parity.rs`, `rep_ffi_parity.rs`, `wire_vectors.rs`.
- **Paridad de runtimes:** `runtime_physics_parity.rs`, `runtime_static_data_parity.rs`, `host_api_manifest.rs` con scripts de humo equivalentes en Lua, Python y JS.
- **Los `#[allow(clippy::unwrap_used)]` están acotados como atributos internos dentro de `mod tests`**, nunca a nivel de crate.

### 8.2 Huecos

**`cargo test` no es señal de lo que se ejecutó.** `#[ignore]` se usa **cero veces**. En su lugar el proyecto usa saltos por variable de entorno que pasan en silencio: 20 archivos de test emiten `eprintln!("skipping …")` y devuelven verde. En una ejecución por defecto sin variables de BD, **~70 puntos de salto** producen una suite en verde que no ejerció casi nada de la capa de persistencia.

**Solo MongoDB corre en CI.** Los tests de contrato de Postgres y CockroachDB —pese a que `docker-compose.yml` existe y GitHub Actions ofrece contenedores `services:` gratis— **nunca corren en CI**. Postgres se describe como "el backend duradero multinodo al que apunta Citadel" y solo se valida cuando un desarrollador recuerda levantar un contenedor a mano.

**`src/matchmaker_live.rs`: 1.077 líneas de producción, cero tests.** Sin módulo `#[cfg(test)]`, y ningún test de integración referencia `matchmaker_live` ni `LiveMatchmakerNode`. Su único ejercicio es incidental vía tests del gateway. Es el módulo que posee el fencing duradero y la redención de handoff remoto: **la superficie sin testear de mayor consecuencia del árbol.**

**`src/repository/mongodb.rs`: 5.834 líneas de producción, 7 tests inline (2,4 %).** Parcialmente mitigado por `tests/mongodb_foundation.rs`, pero esa suite depende de un replica set vivo.

**Sin benchmarks.** Cero `criterion`, cero directorios `benches/`. Para un servidor de juego en tiempo real con transform sync, física y un tick loop, no hay detección de regresiones de latencia ni throughput.

**Sin property testing ni fuzzing.** Cero `proptest`/`quickcheck`/`arbitrary`, sin `fuzz/`, sin `cargo-fuzz`. Este es el hueco de testing más serio: `citadel-wire` decodifica **bytes de red no confiables** desde QUIC/WebSocket/WebTransport, y `citadel-tmx` parsea archivos de mapa. `unsafe_code = "forbid"` limita el daño a panics y DoS en vez de corrupción de memoria, pero un panic del decodificador en el gateway sigue siendo un bug remoto de disponibilidad.

**Infraestructura de test compartida delgada:** solo 253 líneas de helpers sirven a 56 archivos de test de integración, y cubren únicamente handshakes de transporte (los usan 10 de 56 archivos). Mientras tanto `Config::default` aparece 89 veces en 35 archivos y `DatabaseConfig {` 47 veces en 16. Falta un constructor de fixtures de app/config/BD.

**El `database_explorer.rs` invierte la premisa:** de sus 3.687 líneas solo 395 son producción, con 35 tests más un test de contrato. Es el módulo grande **mejor** testeado.

---

## 9. Documentación

### 9.1 CRÍTICO: `docs/architecture/` y `docs/features/` no existen en ninguna parte

`Cargo.toml:10` referencia `docs/release-process.md`, que falta. Es solo la punta. Resolviendo cada nombre referenciado contra el árbol completo, **confirmado ausentes — no existe ningún archivo con ese nombre en ningún directorio**:

`release-process`, `ai-collaboration`, `technical-debt`, `database-abstraction`, `client-sdk-layout`, `runtime-contract`, `service-boundaries`, `testing`, `embedded-lua-runtime`, `observability-and-errors`, `cli-and-config`, `node-ownership-and-routing`, `network-peer-property-replication`, `social-graph-friends`, `script-runtime-parity`, `cmap-terrain-export`, `godot-sdk-skeleton`, `unreal-sdk-skeleton`.

**Verificado independientemente: 54 referencias** a estas rutas en el árbol. Son de carga estructural:

| Ubicación | Referencia rota |
|---|---|
| `Cargo.toml:10` | `docs/release-process.md` |
| `.github/workflows/release.yml:95` | `docs/release-process.md` (el procedimiento de credenciales de Apple) |
| `.github/pull_request_template.md` | `docs/features/`, `docs/architecture/` — **se pide a cada colaborador marcar casillas sobre directorios inexistentes** |
| `src/error.rs:4,20` | `docs/architecture/observability-and-errors.md` |
| `src/config/mod.rs:4,49,1113` | `docs/architecture/cli-and-config.md` |
| `src/storage/mod.rs:5,227`, `src/repository/mod.rs:12`, `pg/mod.rs:5` | `docs/architecture/database-abstraction.md` |
| `src/runtime/mod.rs:7,38` | `docs/features/embedded-lua-runtime.md` |
| `src/services/mod.rs:7` | `docs/architecture/service-boundaries.md` |
| `src/realtime/netpeer/mod.rs:4` | `docs/architecture/network-peer-property-replication.md` |
| `citadel.toml:75`, `packaging/server/scripts/main.lua:6` | `docs/features/embedded-lua-runtime.md` — **se distribuye al usuario final dentro del ZIP de release** |
| `website/src/content/docs/reference/admin-api/*.md` (4 páginas publicadas) | `docs/architecture/technical-debt.md` — **enlaces rotos en la web en producción** |
| 6 archivos `.codex/agents/*.toml` | `docs/ai-collaboration.md`, `docs/testing.md` |

**Verificado en el historial completo de git (`--all --diff-filter=A`): estos archivos nunca existieron.** Lo único que ha vivido jamás bajo `docs/` son los dos JSON actuales. No se trata, por tanto, de un árbol que se borró o se migró a `website/` sin actualizar los referenciadores — es documentación que **nunca llegó a escribirse**, mientras el código y las herramientas se escribían citándola.

Esto cambia la naturaleza de la corrección: no hay nada que restaurar. Hay que decidir, para cada una de las 54 referencias, si el documento debe escribirse o si el puntero debe reescribirse hacia `website/src/content/docs/`. El efecto actual es que un colaborador nuevo que siga cualquier puntero interno no encuentra nada, y la salida de `cargo doc` contiene decenas de referencias muertas.

### 9.2 Otros huecos de documentación

- **`CONTRIBUTING.md` no existe**, aunque el README afirma: *"`CONTRIBUTING.md` describirá el flujo público de contribución cuando se añada"*.
- **No hay archivo `LICENSE`** en la raíz, pese a que `clients/js/package.json` declara `"license": "MIT"`. Ambigüedad legal para un repo público.
- **Ningún `#![warn(missing_docs)]` en ninguna parte.** Cero coincidencias de `missing_docs`, `#![warn`, `#![deny`, `#![forbid` en `src/` y `crates/*/src/`. La cobertura de documentación de API pública no está forzada, incluyendo `citadel-client` y `citadel-client-ffi`, que **se distribuyen a consumidores de SDK**.
- `make docs-build` copia la salida de `cargo doc` a `website/public/rustdoc/`, así que el rustdoc se publica — pero se construye solo manualmente y nunca se valida.

---

## 10. Developer experience y velocidad del ciclo de desarrollo

### 10.1 Deriva real en la orquestación de build

`Makefile` (699 líneas, 47 targets) y `make.ps1` (1.500 líneas, 43 targets) son dos implementaciones paralelas. La deriva no es hipotética:

- **Tres definiciones distintas de "correr los tests"** (§7.3).
- **`Invoke-DocsCheck` (`make.ps1:282-346`, ~65 líneas) es código muerto**: está definido pero nunca se invoca desde la tabla de despacho. Además está doblemente obsoleto — su `$DocsBase` por defecto es `"origin/main"`, una rama que no existe, y le falta la vía de escape `Docs-Exempt` y el escalonado de documentación que sí tiene la versión en shell.
- **`fmt` difiere por diseño pero en silencio:** el `Makefile` ejecuta `cargo fmt`; `make.ps1` ejecuta `cargo fmt -- --config newline_style=Auto` mientras `rustfmt.toml` fija `newline_style = "Unix"`. Justificado en Windows, pero sin documentar como override intencional.
- **Targets asimétricos por plataforma:** `package-linux`, `package-macos` y los `package-client-*-macos` existen solo en el `Makefile`; `setup` (bootstrap de rustup) existe solo en `make.ps1`.

**Recomendación:** `cargo xtask` es el mejor encaje aquí. La lógica de empaquetado ya es consciente de la versión (ambos runners reparsean `Cargo.toml` con `grep`/`sed`), hace ramificación por plataforma y manipula árboles de directorios preparados — eso es territorio de programa Rust, no de shell. Un xtask obtiene la versión de `env!("CARGO_PKG_VERSION")` gratis, elimina tanto `Get-CargoVersion` como el hack `VERSION := $(shell grep -m1 ...)`, y quita la dependencia dura de Git Bash en una ruta fija en Windows. Fase 1 pragmática: definir los targets de verificación **una sola vez** para que `test`/`clippy`/`fmt` no puedan volver a divergir por tres caminos.

### 10.2 El ciclo de desarrollo es lento por razones evitables

- **`RUST_TEST_THREADS=1` es el valor por defecto del check canónico.** `scripts/python-runtime-env.sh` (que `check.sh` carga primero) termina con `export RUST_TEST_THREADS="${RUST_TEST_THREADS:-1}"`. La razón declarada es real pero estrecha: *"la inicialización de CPython es global al proceso… la ejecución paralela puede competir por su setup de intérprete **en Windows**"*. La consecuencia es global: **todo desarrollador en toda plataforma ejecuta los 1.400 tests en serie** en cada `scripts/check.sh`. Es muy probablemente el mayor coste de DX local del repositorio, impuesto para proteger un puñado de tests del runtime de Python. **`cargo-nextest` resuelve esto correctamente** con aislamiento de proceso por test.
- **Sin sccache, sin mold/lld, sin split-debuginfo.** Cero coincidencias en `.cargo/config.toml`, `Makefile`, `make.ps1` y ambos workflows. Con este grafo de dependencias los tiempos de enlazado dominan, y un linker más rápido es prácticamente gratis.
- **Sin ninguna sección `[profile.*]` en ningún `Cargo.toml`.** En particular, sin `[profile.dev.package."*"] opt-level = 3` — física, navmesh y codec corren **sin optimizar en cada test de debug**, lo que agrava directamente la suite en serie de arriba.
- **~64 binarios de test.** Cada archivo de integración es un crate separado que enlaza el servidor completo.

### 10.3 Onboarding

El README hace bien en priorizar la ruta de *usuario* (descargar ZIP, ejecutar, abrir `/dashboard`, editar `scripts/main.lua`, ver hot reload) sin necesidad de toolchain. Buena decisión de producto.

La ruta de *colaborador* es delgada: lista como prerrequisitos *"Git, un toolchain reciente de Rust estable, Python 3 y Make"*. Lo realmente necesario, descubierto en `.cargo/config.toml`, `Dockerfile` y `make.ps1`:

- **CMake + toolchain C/C++** (RecastNavigation se vendoriza a la fuerza vía `RECAST_VENDOR=true`), **MSVC/VS 2022 específicamente en Windows**
- **`libclang-dev`**
- **Node 24** para `website/` y `clients/js/`
- **Docker** para cualquier test con BD
- **Git Bash en `C:\Program Files\Git`** para `.\make.ps1 check` (requisito duro)
- **gitleaks en el PATH** o cada push se bloquea
- **`scripts/install-git-hooks.sh`** ejecutado a mano, nunca mencionado en el README

Nada de esto está en el README, y no hay un `make doctor` de preflight del lado de Make.

### 10.4 Configuración

| Archivo | Estado |
|---|---|
| `rust-toolchain.toml` | `channel = "stable"`, con `rustfmt` y `clippy`. **Sin fijar** — una release nueva de stable puede romper el build sin sincronía. Sin lista de `targets` pese a los cross-builds musl. |
| `rustfmt.toml` | 3 líneas, correcto |
| `.editorconfig` | consistente con rustfmt |
| `clippy.toml` | **No existe.** Sin `msrv`, sin `disallowed-methods`, sin umbral de complejidad cognitiva |
| `deny.toml` | **No existe** |
| `citadel.toml` | 191 líneas, excelente — cada clave documentada inline con su defecto y su razón |
| `.env` | Correctamente ignorado y sin trackear. **No existe `.env.example`**, aunque `.gitignore` ya lo permite explícitamente con `!.env.example` |

---

## 11. Proceso de release y distribución

`release.yml` es el workflow más fuerte del repo: valida la versión por regex semver y **falla en duro si `v$version` ya existe en el remoto**, empaqueta en una matriz de 3 (Windows + 2 musl vía `cross`, con aserción de enlazado estático y humo bajo qemu para ARM64), construye el SDK Web de Godot, corre los tests del SDK de JS, y publica con `fail_on_unmatched_files: true`.

**Pero hay dos fallos de higiene confirmados:**

**A) El CHANGELOG se saltó una versión publicada entera.** Tags presentes: `v0.9.9`, `v0.9.10`, `v0.9.11`, `v0.9.12`, `v0.9.13`. Cabeceras del CHANGELOG: `Unreleased`, `[0.9.14]`, `[0.9.12]`, `[0.9.11]`, `[0.9.10]`, `[0.9.9]`. **No hay sección `[0.9.13]`**, pese a que `v0.9.13` está tageada y `4751adb release: prepare v0.9.13` está en el historial. El extractor `awk` de `release.yml:244-251` cayó por tanto a su fallback y publicó `v0.9.13` con el cuerpo *"Citadel v0.9.13"* — **sin ninguna nota de release para una versión distribuida.**

**B) El bump de versión es manual y sin guardián.** `release.yml` valida *solo* que el tag sea nuevo; nunca comprueba que exista una sección de CHANGELOG para esa versión, que es exactamente por donde se coló la 0.9.13. La versión se reparsea con `sed`/`grep` en **cuatro** lugares.

**C) Sin `cargo publish`** a crates.io para `citadel-wire`/`citadel-client`, ni `npm publish` para el SDK de JS — la distribución son solo ZIPs de GitHub Releases.

macOS está deliberadamente deshabilitado a la espera de credenciales de Apple, documentado en comentarios — la decisión correcta, aunque el procedimiento referenciado (`docs/release-process.md`) no existe.

---

## 12. SDKs de cliente

| SDK | Build | Tests | Versionado |
|---|---|---|---|
| **js** | `esbuild` | `node --test test/*.test.js` — 6 archivos | `package.json` = **`0.1.0`** |
| **godot** | GDExtension vía SCons + godot-cpp; addon Web en GDScript puro | El mejor cubierto: test de contrato headless, test de integración con WS mockeado y **E2E real de navegador en CI** | versión del servidor |
| **unity** | ABI C → `Plugins/x86_64` | **Sin tests automatizados** | versión del servidor |
| **unreal** | `ue-plugin-build.sh`, `bundle-ffi.sh` | hook de paridad Tier-B + 3 tests Python | versión del servidor |

**La lógica de protocolo está reimplementada 5 veces.** El protocolo canónico vive en `crates/citadel-wire` (7.593 LOC) y se reimplementa de forma independiente en JS (553 LOC), Godot (431), Unity (545) y **Unreal (~2.500 líneas de C++**: `CitadelNetworkPeer.cpp` 1.064 + `CitadelTransformSync.cpp` 820 + `CitadelTransformWire.h` 481). Unreal es la anomalía: tiene binding FFI *y además* una reimplementación nativa completa de replicación netpeer y transform sync.

**El mitigante es real pero limitado.** El gate de paridad existe y está bien diseñado, pero su propio docstring admite que es Tier-A: *"nunca compila los SDKs ni ejecuta un runtime de lenguaje, solo parsea con regex los literales de constantes declaradas."* La deriva de constantes se detecta; **la deriva de comportamiento en 2.500 líneas de bit-packing C++ escritas a mano, no.**

**Desincronización de versiones:** `clients/js/package.json` dice `0.1.0` mientras el artefacto empaquetado es `citadel-client-js-v0.9.14.zip`. Nada reconcilia `package.json`, `sdk.manifest.json`, el `abi_version` de `contract.json` y la versión de Cargo. Unity, Unreal y Godot no tienen campo de versión propio.

---

## 13. Plan de acción priorizado

### P0 — Antes de cualquier despliegue público (todos son cambios pequeños)

| # | Acción | Referencia |
|---|---|---|
| 1 | Sustituir `random_token()` por `getrandom::fill` | H2 — `src/services/console.rs:200` |
| 2 | Fallar el arranque si las credenciales de consola son las de defecto y `http.bind` no es loopback (o generar contraseña aleatoria e imprimirla una vez) | H1 — `src/config/mod.rs:1415` |
| 3 | Aplicar `AuthenticationRateLimitPolicy` a `/console/v1/login` más backoff por usuario | H3 — `src/http/console_api/mod.rs:343` |
| 4 | Rotar la credencial de PocketBase de `.env` | M1 |
| 5 | Corregir el trigger de CI: `push: branches: [develop, release]` | §7.1 |
| 6 | Hacer que CI ejecute `bash scripts/check.sh` en lugar de la secuencia copiada a mano | §7.2 — arregla de un golpe el `--workspace` faltante y los 6 gates locales |

### P1 — Alto impacto, bien acotado

| # | Acción | Referencia |
|---|---|---|
| 7 | Sustituir `party_block_on` por un `Handle` cacheado + `block_in_place` (el patrón correcto ya existe en `src/runtime/host_services.rs:364`) | §6.1 — 14 call sites en la ruta de RPC entrante |
| 8 | Fallar o avisar en `warn!` cuando QUIC/WebTransport ligan a no-loopback sin PEM; considerar gatear `SelfSignedCert::generate` tras una feature `dev-certs` | H4 |
| 9 | Añadir terminación rustls nativa a `HttpConfig`, o exigir `behind_tls_proxy` explícito antes de servir `/dashboard` fuera de loopback | H5 |
| 10 | Añadir `deny.toml` + job de `cargo-deny check advisories bans licenses sources` | M7 |
| 11 | Promover gitleaks del hook local a job requerido de CI | M7 |
| 12 | Romper la dependencia de `citadel-client-ffi` sobre el crate raíz extrayendo `crates/citadel-transform` | §5.2 |
| 13 | Añadir sección `[0.9.13]` al CHANGELOG y un paso en `release.yml` que verifique que existe sección para la versión *antes* de empaquetar | §11 |
| 14 | Añadir tests a `src/matchmaker_live.rs` (1.077 líneas de fencing duradero sin cobertura) | §8.2 |

### P2 — Reparación de documentación y contribución

| # | Acción |
|---|---|
| 15 | Resolver las 54 referencias rotas a `docs/architecture/` + `docs/features/`. **No hay nada que restaurar: esos archivos nunca existieron en el historial.** Para cada referencia hay que decidir entre escribir el documento o reescribir el puntero hacia `website/src/content/docs/`. Empezar por las tres que bloquean a colaboradores: `.github/pull_request_template.md`, `Cargo.toml:10` y `release.yml:95`. *(El directorio `docs/` se renombró a `manifests/` el 2026-08-03 para eliminar la ambigüedad de nombre — ver §5.6.)* |
| 16 | Añadir `CONTRIBUTING.md` con la lista real de prerrequisitos (CMake/MSVC, libclang, Node 24, Docker, gitleaks, Git Bash) y `scripts/install-git-hooks.sh` como primer paso obligatorio |
| 17 | Añadir archivo `LICENSE` en la raíz |
| 18 | Commitear `.env.example` (el `.gitignore` ya lo contempla) |
| 19 | Corregir la instrucción de `AGENTS.md` que comprueba `.git/hooks/pre-push` — con `core.hooksPath` esa ruta nunca se puebla |
| 20 | Añadir `SECURITY.md` con dirección de divulgación y checklist de endurecimiento para producción |
| 21 | Corregir los comentarios obsoletos/incorrectos en `src/transport/quic/tls.rs:8-10` y `src/services/console.rs:202-206` |
| 22 | Documentar explícitamente Lua `Trusted` y el runtime Python como niveles *solo para código de confianza*, y que el deadline de Python es orientativo |

### P3 — Endurecimiento adicional y velocidad del ciclo

| # | Acción | Referencia |
|---|---|---|
| 23 | Stack de cabeceras de seguridad: CSP, `X-Frame-Options: DENY`, `nosniff`, `Referrer-Policy`; más `TimeoutLayer` y `DefaultBodyLimit` globales | M3 |
| 24 | `wss://` en el fallback WebSocket y `WebSocketConfig` explícito muy por debajo del defecto de 64 MiB; topes de `max_connections` por listener | M2 |
| 25 | Hashear contraseñas de operador con el helper Argon2 ya presente | M4 |
| 26 | Gatear las *lecturas* del explorador de BD tras `require_admin()` o un rol `db_reader`; complementar la redacción por nombre con configuración explícita de columnas sensibles por tabla | M5 |
| 27 | Hacer duradero el log de auditoría (JSONL append-only junto a `citadel-errors.jsonl`) | M6 |
| 28 | Acotar `RUST_TEST_THREADS=1` a donde hace falta, o adoptar `cargo-nextest` | §10.2 |
| 29 | Añadir `[profile.dev.package."*"] opt-level = 3` | §10.2 |
| 30 | Habilitar un linker rápido (lld/mold) en `.cargo/config.toml` | §10.2 |
| 31 | Dividir el job `rust` de CI para no necesitar `cache-targets: false`, y cubrir el build de features por defecto (el que se distribuye) | §7.5 |
| 32 | Añadir `concurrency: cancel-in-progress` y `timeout-minutes` a ambos workflows; cachear `cross` | §7.4 |
| 33 | Añadir Postgres y CockroachDB a CI vía `services:` | §8.2 |
| 34 | Acotar los joins de `Supervisor::shutdown` con timeout | §6.2 |

### P4 — Estructura a medio plazo

| # | Acción | Referencia |
|---|---|---|
| 35 | Mover el servidor a `crates/citadel-server/`, raíz como manifiesto virtual con `[workspace.dependencies]` y `[workspace.lints]` | §5.2, §6.5 |
| 36 | Dividir `src/realtime/gateway.rs` (8.487 líneas): extraer `impl DomainRpcServices` a `gateway/domain_rpc/{friends,groups,party,matchmaker}.rs` y descomponer el `impl Gateway` de 85 métodos por familia de frame. Los 8 campos `Option<…>` son la costura: cada subsistema opcional debería ser un handler registrado, no un campo nullable comprobado en cada método | §5.3 |
| 37 | Nombrar la raíz de composición: renombrar `startup.rs` → `wizard.rs`, crear `src/bootstrap/` con `start_enabled` descompuesto, y reducir `transport/mod.rs` a la abstracción de wire que su propio doc-comment describe | §5.3 |
| 38 | Adoptar la regla de granularidad (≥800 líneas → carpeta) empezando por `matchmaker*` (4.600 líneas), `database_explorer.rs`, `config/mod.rs` y `repository/mongodb.rs` | §5.4 |
| 39 | Reparar las cuatro inversiones de capas | §5.5 |
| 40 | Degradar a `pub(crate)` los 7 módulos con cero consumidores (~5.300 líneas de API pública innecesaria) | §5.5 |
| 41 | Extraer un `trait Lease` que unifique las 6 implementaciones duplicadas de `is_current_at` | §6.4 |
| 42 | Agrupar parámetros en structs en los 32 sitios de `too_many_arguments`, empezando por `services/chat.rs` y `repository/chat.rs` | §6.5 |
| 43 | Resolver el split de `reqwest` 0.12/0.13 que arrastra `sentry 0.48` | §6.5 |
| 44 | Añadir fuzzing a `citadel-wire` y `citadel-tmx` (decodifican bytes no confiables) | §8.2 |
| 45 | Añadir benchmarks criterion al codec, el tick loop y transform sync | §8.2 |
| 46 | Añadir property tests a los round-trips de encode/decode de envelope y a los parsers TMX/cmap | §8.2 |
| 47 | Generar el código de protocolo de los SDKs desde `contract.json` en lugar de escribirlo 5 veces; objetivo prioritario: las 2.500 líneas de C++ de Unreal | §12 |
| 48 | Consolidar el task runner en `cargo xtask` (fase 1: definir los targets de verificación una sola vez) | §10.1 |
| 49 | Declarar MSRV (`rust-version`) casando con `RUST_VERSION=1.92.0` del Dockerfile, añadir job de CI en ese toolchain y `msrv` en un `clippy.toml` nuevo | §7.4 |
| 50 | Reconciliar el versionado de SDKs derivándolo todo de la versión de Cargo en el paso de empaquetado | §12 |
| 51 | Meter `tools/bot-stress-simulator/client` en el workspace (2.817 líneas nunca linteadas) | §5.6 |
| 52 | Arreglar o borrar `clients/unity-quic-demo/` (39 archivos trackeados, 0 `.cs`, DLL ausente) | §5.6 |
| 53 | Extraer `src/http/assets/console.html` (2.289 líneas) a un subproyecto con su propio build, lint y tests | §6.4 |
| 54 | Correr los tests del SDK de JS en `ci.yml`, no solo en `release.yml`; construir `website/` en CI | §7.4 |
| 55 | Añadir `cargo doc --no-deps --workspace -D warnings` a CI y `#![warn(missing_docs)]` a `citadel-client` y `citadel-client-ffi` | §9.2 |
| 56 | Compartir las migraciones neutras de dialecto y añadir un test que afirme que los tres directorios convergen en el mismo esquema lógico | §5.6 |

---

## Nota de cierre

Dos cosas merecen preservarse explícitamente frente a cualquier refactor:

**El sistema de paridad de SDKs dirigido por manifiestos** (`contract.json` → descubrimiento por glob de `sdk.manifest.json` → Tier A/B → manifiesto de completitud de features → matriz de capacidades → README generado). El diseño es excelente; solo le falta que CI lo ejecute.

**El estándar de justificación inline** en `citadel.toml`, `.gitignore` y los comentarios de dependencias del `Cargo.toml`. Explicar el *porqué* de cada entrada es lo que hizo posible auditar 150.000 líneas con confianza en un solo pase.
