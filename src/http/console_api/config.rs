//! Console Configuration section (`GET /console/v1/config`, ).
//!
//! Renders the node's **effective** resolved configuration as grouped
//! key/value pairs for the console's Configuration browser. Redaction is
//! explicit and tested, never best-effort: the values of [`SECRET_KEYS`] are
//! replaced with `<redacted>` (`database.url` keeps its shape minus
//! credentials via [`redact_url_credentials`], so an operator can still see
//! which host/database the node points at).
//!
//! The view is produced by serializing [`Config`] through `toml::Value` and
//! flattening each section into dotted keys — new config fields therefore
//! appear automatically. Any new secret field MUST be added to
//! [`SECRET_KEYS`] in the same change (the redaction unit tests are the
//! reminder: they assert every known secret never reaches the response).

use axum::Json;
use axum::extract::State;
use serde::Serialize;

use crate::app::App;
use crate::config::Config;
use crate::error::{AppError, redact_url_credentials};
use crate::services::ConsolePrincipal;

use crate::http::error::ApiError;

/// The Configuration section route.
pub const CONFIG_PATH: &str = "/console/v1/config";

/// Fully-redacted dotted keys: the value is replaced with `<redacted>`.
const SECRET_KEYS: &[&str] = &["console.password", "console.viewer_password"];

/// Keys holding URLs that may embed credentials: the value is kept but its
/// `user:password@` part is stripped.
const CREDENTIAL_URL_KEYS: &[&str] = &["database.url"];

/// Replacement for fully-redacted values.
const REDACTED: &str = "<redacted>";

/// The JSON response for [`CONFIG_PATH`].
#[derive(Debug, Clone, Serialize)]
pub struct ConfigResponse {
    /// Node identity, for the browser header.
    pub node_id: String,
    /// Server version.
    pub version: &'static str,
    /// Selected persistence backend (`in-memory`, `sqlite`, `postgres`).
    pub backend: &'static str,
    /// One group per top-level config section, in section order.
    pub groups: Vec<ConfigGroup>,
}

/// One config section rendered as flat dotted key/value strings.
#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct ConfigGroup {
    /// Section name (`server`, `http`, `transport`, ...).
    pub name: String,
    /// Dotted keys within the section, with display-ready values.
    pub entries: Vec<ConfigEntry>,
}

/// One rendered configuration key.
#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct ConfigEntry {
    /// Dotted key relative to the section (e.g. `quic.bind`).
    pub key: String,
    /// Display value (TOML-style scalar rendering), post-redaction.
    pub value: String,
}

/// `GET /console/v1/config`: the redacted effective configuration.
pub(super) async fn get_handler(
    State(app): State<App>,
    _operator: ConsolePrincipal,
) -> Result<Json<ConfigResponse>, ApiError> {
    app.metrics().record_http_request();
    let groups = config_groups(app.config()).map_err(ApiError::from)?;
    Ok(Json(ConfigResponse {
        node_id: app.node_id().to_string(),
        version: app.version(),
        backend: app.backend_kind().as_str(),
        groups,
    }))
}

/// Render `config` into redacted, grouped dotted key/value pairs.
///
/// # Errors
/// Returns an internal error if the config fails to serialize (not expected
/// for a validated [`Config`]).
pub fn config_groups(config: &Config) -> Result<Vec<ConfigGroup>, AppError> {
    let value = toml::Value::try_from(config).map_err(|e| {
        AppError::internal("failed to serialize configuration for the console")
            .with_detail(e.to_string())
    })?;
    let toml::Value::Table(sections) = value else {
        return Err(AppError::internal(
            "configuration did not serialize to a table",
        ));
    };
    let mut groups = Vec::with_capacity(sections.len());
    for (section, section_value) in sections {
        let mut entries = Vec::new();
        flatten(&section, "", &section_value, &mut entries);
        groups.push(ConfigGroup {
            name: section,
            entries,
        });
    }
    Ok(groups)
}

/// Recursively flatten a TOML value into dotted keys, applying redaction.
fn flatten(section: &str, prefix: &str, value: &toml::Value, out: &mut Vec<ConfigEntry>) {
    match value {
        toml::Value::Table(table) => {
            for (key, nested) in table {
                let child = if prefix.is_empty() {
                    key.clone()
                } else {
                    format!("{prefix}.{key}")
                };
                flatten(section, &child, nested, out);
            }
        }
        scalar => {
            let key = if prefix.is_empty() {
                // A bare scalar section (none today, but total by construction).
                section.to_string()
            } else {
                prefix.to_string()
            };
            out.push(ConfigEntry {
                value: render(section, &key, scalar),
                key,
            });
        }
    }
}

/// Render one scalar with redaction by full dotted path.
fn render(section: &str, key: &str, value: &toml::Value) -> String {
    let path = format!("{section}.{key}");
    if SECRET_KEYS.contains(&path.as_str()) {
        return REDACTED.to_string();
    }
    let rendered = match value {
        toml::Value::String(s) => s.clone(),
        other => other.to_string(),
    };
    if CREDENTIAL_URL_KEYS.contains(&path.as_str()) {
        return redact_url_credentials(&rendered);
    }
    rendered
}

#[cfg(test)]
mod tests {
    use super::*;

    fn config_with_secrets() -> Config {
        let mut config = Config::default();
        config.console.password = "operator-secret".to_string();
        config.console.viewer_password = Some("viewer-secret".to_string());
        config.database.url = Some("postgres://user:dbpass@localhost:5432/citadel".to_string());
        config
    }

    fn rendered(config: &Config) -> String {
        serde_json::to_string(&config_groups(config).expect("groups")).expect("serialize")
    }

    #[test]
    fn every_known_secret_is_redacted() {
        let text = rendered(&config_with_secrets());
        assert!(!text.contains("operator-secret"), "console.password leaked");
        assert!(
            !text.contains("viewer-secret"),
            "console.viewer_password leaked"
        );
        assert!(!text.contains("dbpass"), "database.url credentials leaked");
        assert!(text.contains(REDACTED));
    }

    #[test]
    fn database_url_keeps_its_shape_without_credentials() {
        let groups = config_groups(&config_with_secrets()).expect("groups");
        let database = groups
            .iter()
            .find(|g| g.name == "database")
            .expect("database group");
        let url = database
            .entries
            .iter()
            .find(|e| e.key == "url")
            .expect("url entry");
        assert!(
            url.value.contains("localhost:5432/citadel"),
            "host/db visible: {}",
            url.value
        );
        assert!(!url.value.contains("dbpass"));
    }

    #[test]
    fn sections_and_nested_keys_are_flattened_with_dots() {
        let groups = config_groups(&Config::default()).expect("groups");
        let names: Vec<&str> = groups.iter().map(|g| g.name.as_str()).collect();
        for expected in [
            "server",
            "http",
            "logging",
            "transport",
            "runtime",
            "console",
        ] {
            assert!(names.contains(&expected), "missing section {expected}");
        }
        let transport = groups
            .iter()
            .find(|g| g.name == "transport")
            .expect("transport group");
        assert!(
            transport.entries.iter().any(|e| e.key == "quic.bind"),
            "nested keys use dotted paths"
        );
    }

    #[test]
    fn default_config_renders_real_values() {
        let groups = config_groups(&Config::default()).expect("groups");
        let server = groups
            .iter()
            .find(|g| g.name == "server")
            .expect("server group");
        let node_id = server
            .entries
            .iter()
            .find(|e| e.key == "node_id")
            .expect("node_id");
        assert_eq!(node_id.value, "dev-1");
    }
}
