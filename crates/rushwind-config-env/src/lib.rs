//! Environment-variable engine for the Rust configuration contract,
//! ported from `go-wind-plugins/config/env`.
//!
//! The carrier is the process environment: a lookup of the key — or the
//! configured default key when the caller passes an empty one — with an
//! optional prefix prepended (`APP_` turns `DATABASE_URL` into
//! `APP_DATABASE_URL`).
//!
//! An unset variable is `Ok(None)`, the contract's "absent" answer —
//! exactly what a [`FallbackSource`](rushwind_config::FallbackSource)
//! falls through on, which is the composition this engine exists for:
//! file first, environment overrides last.
//!
//! Environment variables are process-wide and mutable from anywhere;
//! within one process they serve as a read-only source in practice. No
//! watch capability: the environment has no change notifications — the
//! default `NotWatchable` stands.

#![forbid(unsafe_code)]
#![deny(missing_docs)]

use rushwind_config::{BoxFuture, ConfigError, Source};

/// Builder for [`EnvSource`].
#[derive(Default)]
pub struct EnvOptions {
    prefix: Option<String>,
    key: Option<String>,
}

impl EnvOptions {
    /// Options with no prefix and no default key.
    pub fn new() -> Self {
        Self::default()
    }

    /// Sets a prefix prepended to every looked-up variable name.
    pub fn with_prefix(mut self, prefix: &str) -> Self {
        self.prefix = Some(prefix.to_string());
        self
    }

    /// Sets the default variable name used when a load passes an empty
    /// key.
    pub fn with_key(mut self, key: &str) -> Self {
        self.key = Some(key.to_string());
        self
    }
}

/// The environment-variable configuration source.
pub struct EnvSource {
    options: EnvOptions,
}

impl EnvSource {
    /// Builds the engine from its options.
    pub fn new(options: EnvOptions) -> Self {
        Self { options }
    }
}

impl Source for EnvSource {
    fn load<'a>(&'a self, key: &'a str) -> BoxFuture<'a, Result<Option<Vec<u8>>, ConfigError>> {
        Box::pin(async move {
            // The Go resolveKey: the caller's key wins over the
            // configured default; the prefix applies when both it and
            // the key are non-empty.
            let mut name = if key.is_empty() {
                self.options.key.clone().unwrap_or_default()
            } else {
                key.to_string()
            };
            if let Some(prefix) = &self.options.prefix {
                if !name.is_empty() {
                    name = format!("{prefix}{name}");
                }
            }
            if name.is_empty() {
                return Err(ConfigError::Failed("env: no key specified".to_string()));
            }
            match std::env::var(&name) {
                Ok(value) => Ok(Some(value.into_bytes())),
                Err(std::env::VarError::NotPresent) => Ok(None),
                Err(std::env::VarError::NotUnicode(_)) => Err(ConfigError::Failed(format!(
                    "env: variable {name} is not valid unicode"
                ))),
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn source() -> EnvSource {
        EnvSource::new(EnvOptions::new())
    }

    #[tokio::test]
    async fn an_empty_resolved_name_is_an_error() {
        // No default key, empty lookup key: the Go "no key specified".
        assert!(matches!(
            source().load("").await.unwrap_err(),
            ConfigError::Failed(_)
        ));
    }

    #[tokio::test]
    async fn the_default_key_serves_empty_lookups() {
        let name = "RUSHWIND_CONFIG_ENV_TEST_DEFAULT";
        // Test fns run in parallel against one process environment; the
        // unique variable name makes interference a non-issue.
        std::env::set_var(name, "from-default");
        let env = EnvSource::new(EnvOptions::new().with_key(name));
        assert_eq!(env.load("").await.unwrap(), Some(b"from-default".to_vec()));
        std::env::remove_var(name);
    }

    #[tokio::test]
    async fn an_explicit_key_wins_over_the_default() {
        let explicit = "RUSHWIND_CONFIG_ENV_TEST_EXPLICIT";
        std::env::set_var(explicit, "explicit-value");
        let env = EnvSource::new(EnvOptions::new().with_key("RUSHWIND_NEVER_SET_KEY"));
        assert_eq!(
            env.load(explicit).await.unwrap(),
            Some(b"explicit-value".to_vec())
        );
        std::env::remove_var(explicit);
    }

    #[tokio::test]
    async fn a_missing_variable_is_absent_not_an_error() {
        // The Go (nil, nil): the fallback composition's fall-through.
        assert_eq!(source().load("RUSHWIND_NEVER_SET_KEY").await.unwrap(), None);
    }

    #[tokio::test]
    async fn the_prefix_is_prepended() {
        let name = "RUSHWIND_TEST_PREFIXED_VALUE";
        std::env::set_var(name, "prefixed");
        let env = EnvSource::new(EnvOptions::new().with_prefix("RUSHWIND_TEST_"));
        assert_eq!(
            env.load("PREFIXED_VALUE").await.unwrap(),
            Some(b"prefixed".to_vec())
        );
        // And a prefixed lookup of an unset variable stays absent.
        assert_eq!(env.load("TOTALLY_UNSET_SUFFIX").await.unwrap(), None);
        std::env::remove_var(name);
    }

    #[tokio::test]
    async fn an_empty_key_under_a_prefix_stays_an_error() {
        // The Go resolveKey: an empty key never gets the prefix; the
        // no-key error stands.
        let env = EnvSource::new(EnvOptions::new().with_prefix("RUSHWIND_TEST_"));
        assert!(matches!(
            env.load("").await.unwrap_err(),
            ConfigError::Failed(_)
        ));
    }

    #[tokio::test]
    async fn watching_is_not_offered() {
        assert_eq!(
            source()
                .watch_value("k")
                .await
                .err()
                .expect("watch_value outcome"),
            ConfigError::NotWatchable
        );
        assert_eq!(
            source().watch("k").await.err().expect("watch outcome"),
            ConfigError::NotWatchable
        );
    }
}
