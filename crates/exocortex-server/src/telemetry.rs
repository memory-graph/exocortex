//! D6 (core PRD §19 R-O3): opt-in OpenTelemetry metric export.
//!
//! The node's metrics already flow through the `metrics` facade to the
//! authenticated Prometheus surface (`/metrics`, R-Sec7). This module
//! adds the OTLP path: when the binary is built with the `otlp`
//! feature AND `OTEL_EXPORTER_OTLP_ENDPOINT` is set, the process-wide
//! recorder becomes a FANOUT — every instrument writes to BOTH the
//! Prometheus recorder (unchanged, still authenticated, still the
//! default when OTLP is off or unconfigured) and an OpenTelemetry
//! meter whose PeriodicReader pushes to the endpoint over OTLP/gRPC
//! (tonic). One `metrics` recorder slot, two sinks, no instrument
//! changes anywhere else in the workspace.
//!
//! Fail-closed configuration: an endpoint set on a binary built
//! WITHOUT the feature is a startup ERROR (the operator asked for
//! export the build cannot do — silently ignoring it would be a lie),
//! and a malformed endpoint fails startup rather than exporting to a
//! guess. Scope: METRICS export; trace/log export is recorded as
//! out-of-scope for this row (the repo's observability contract is
//! metrics-first — R-W14/R-O2).
//!
//! Rule-9 record (PUBLISHING.md): metrics 0.23→0.24 +
//! metrics-exporter-prometheus 0.15→0.18 (the line pairing with the
//! bridge), metrics-exporter-opentelemetry 0.2 (rust-version 1.85,
//! the exact floor), the opentelemetry 0.31 line, and — behind the
//! feature only — opentelemetry-otlp 0.31's tonic transport, which
//! pins tonic 0.14.5 / tonic-prost 0.14.5 / ordered-float 5.0.0 in
//! Cargo.lock (0.14.6 needs rustc 1.88; the feature-gated second
//! gRPC stack is the recorded cost of OTLP/gRPC, isolated from the
//! workspace's tonic 0.12 server surface).

use std::sync::OnceLock;

/// Install the process-wide metrics recorders once: Prometheus
/// always; the OTLP fanout leg when the feature is compiled and the
/// endpoint is configured. Returns the Prometheus render handle the
/// HTTP surface already serves. Unchanged behavior when OTLP is off.
pub fn install() -> metrics_exporter_prometheus::PrometheusHandle {
    static HANDLE: OnceLock<metrics_exporter_prometheus::PrometheusHandle> = OnceLock::new();
    HANDLE
        .get_or_init(|| {
            #[cfg(feature = "otlp")]
            {
                match ot_endpoint() {
                    Ok(Some(endpoint)) => {
                        return install_with_otlp(endpoint)
                            .expect("installing the OTLP metric fanout");
                    }
                    Err(error) => {
                        panic!("OTLP metric export is configured but invalid: {error}");
                    }
                    Ok(None) => {}
                }
            }
            #[cfg(not(feature = "otlp"))]
            {
                if std::env::var("OTEL_EXPORTER_OTLP_ENDPOINT")
                    .ok()
                    .is_some_and(|value| !value.trim().is_empty())
                {
                    panic!(
                        "OTEL_EXPORTER_OTLP_ENDPOINT is set but this binary was built without \
                         the `otlp` feature — refusing to silently skip the configured export; \
                         rebuild with --features otlp or unset the variable"
                    );
                }
            }
            metrics_exporter_prometheus::PrometheusBuilder::new()
                .install_recorder()
                .expect("install prometheus recorder")
        })
        .clone()
}

/// The OTLP endpoint: `OTEL_EXPORTER_OTLP_ENDPOINT`, with the
/// signal-specific `OTEL_EXPORTER_OTLP_METRICS_ENDPOINT` override (the
/// standard OpenTelemetry environment precedence).
#[cfg(feature = "otlp")]
fn ot_endpoint() -> anyhow::Result<Option<String>> {
    let endpoint = std::env::var("OTEL_EXPORTER_OTLP_METRICS_ENDPOINT")
        .or_else(|_| std::env::var("OTEL_EXPORTER_OTLP_ENDPOINT"))
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty());
    match endpoint {
        Some(endpoint) if !endpoint.contains("://") => Err(anyhow::anyhow!(
            "the OTLP endpoint must carry its scheme (http:// or https://): {endpoint}"
        )),
        other => Ok(other),
    }
}

/// Build the fanout: Prometheus + OpenTelemetry under one global
/// `metrics` recorder slot.
#[cfg(feature = "otlp")]
fn install_with_otlp(
    endpoint: String,
) -> anyhow::Result<metrics_exporter_prometheus::PrometheusHandle> {
    use opentelemetry_otlp::WithExportConfig;
    let exporter = opentelemetry_otlp::MetricExporter::builder()
        .with_tonic()
        .with_endpoint(endpoint)
        .build()
        .map_err(|error| anyhow::anyhow!("building the OTLP metric exporter: {error:?}"))?;
    let reader = opentelemetry_sdk::metrics::PeriodicReader::builder(exporter).build();
    let (_provider, otel_recorder) =
        metrics_exporter_opentelemetry::Recorder::builder("exocortex-node")
            .with_meter_provider(|builder| builder.with_reader(reader))
            .build();
    let prometheus = metrics_exporter_prometheus::PrometheusBuilder::new().build_recorder();
    let handle = prometheus.handle();
    let fanout = metrics_util::layers::FanoutBuilder::default()
        .add_recorder(prometheus)
        .add_recorder(otel_recorder)
        .build();
    metrics::set_global_recorder(fanout)
        .map_err(|_| anyhow::anyhow!("a global metrics recorder is already installed"))?;
    Ok(handle)
}

#[cfg(all(test, feature = "otlp"))]
mod tests {
    use opentelemetry_sdk::metrics::data::ResourceMetrics;
    use opentelemetry_sdk::metrics::exporter::PushMetricExporter;
    use opentelemetry_sdk::metrics::PeriodicReader;

    /// A capturing exporter: the collector stand-in. Proves the
    /// fanout delivers the SAME instruments the Prometheus surface
    /// renders — no network, no collector.
    #[derive(Clone, Default)]
    struct CapturingExporter {
        seen: std::sync::Arc<std::sync::Mutex<Vec<String>>>,
    }

    impl PushMetricExporter for CapturingExporter {
        fn export(
            &self,
            metrics: &ResourceMetrics,
        ) -> impl std::future::Future<Output = opentelemetry_sdk::error::OTelSdkResult> + Send
        {
            let mut names = Vec::new();
            for scope in metrics.scope_metrics() {
                for metric in scope.metrics() {
                    names.push(metric.name().to_string());
                }
            }
            self.seen.lock().unwrap().extend(names);
            std::future::ready(Ok(()))
        }

        fn force_flush(&self) -> opentelemetry_sdk::error::OTelSdkResult {
            Ok(())
        }

        fn shutdown_with_timeout(
            &self,
            _timeout: std::time::Duration,
        ) -> opentelemetry_sdk::error::OTelSdkResult {
            Ok(())
        }

        fn temporality(&self) -> opentelemetry_sdk::metrics::Temporality {
            opentelemetry_sdk::metrics::Temporality::Cumulative
        }
    }

    #[test]
    fn the_fanout_delivers_instruments_to_the_otel_leg() {
        let capturing = CapturingExporter::default();
        let reader = PeriodicReader::builder(capturing.clone()).build();
        let (provider, otel_recorder) =
            metrics_exporter_opentelemetry::Recorder::builder("exocortex-test")
                .with_meter_provider(|builder| builder.with_reader(reader))
                .build();
        let fanout = metrics_util::layers::FanoutBuilder::default()
            .add_recorder(otel_recorder)
            .build();
        // A LOCAL recorder slot for this test (not the global one,
        // which the test binary's other suites may already use).
        metrics::with_local_recorder(&fanout, || {
            metrics::counter!("exocortex_test_otlp_fanout_total").increment(7);
        });
        provider.force_flush().expect("flush");
        assert!(
            capturing
                .seen
                .lock()
                .unwrap()
                .iter()
                .any(|name| name.contains("exocortex_test_otlp_fanout")),
            "the counter reached the OTel exporter leg: {:?}",
            capturing.seen.lock().unwrap()
        );
    }
}
