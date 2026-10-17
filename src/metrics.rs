use std::{
    collections::{BTreeSet, HashMap},
    convert::Infallible,
    env,
    sync::{Arc, Mutex},
    time::Instant,
};

use hyper::service::Service;
use prometheus::{
    Encoder, HistogramOpts, HistogramVec, IntCounterVec, Opts, Registry, TextEncoder,
};
use tower_service::Service as TowerService;
use warp::{Filter, Reply, http::StatusCode};

#[derive(Clone)]
struct RequestLabel(Arc<Mutex<Option<&'static str>>>);

fn mark(path: Option<&'static str>) -> impl Filter<Extract = (), Error = warp::Rejection> + Copy {
    warp::ext::get::<RequestLabel>()
        .map(move |label: RequestLabel| *label.0.lock().unwrap() = path)
        .untuple_one()
}

#[derive(Clone)]
pub struct Metrics {
    registry: Registry,
    requests: IntCounterVec,
    duration: HistogramVec,
    endpoints: Arc<Mutex<BTreeSet<&'static str>>>,
}

impl Metrics {
    pub fn new() -> prometheus::Result<Self> {
        let labels = HashMap::from([
            (
                "dc".into(),
                env::var("DC").unwrap_or_else(|_| "local".into()),
            ),
            (
                "pod".into(),
                env::var("POD").unwrap_or_else(|_| "local".into()),
            ),
            (
                "service".into(),
                env::var("SERVICE").unwrap_or_else(|_| "ddia-1".into()),
            ),
            (
                "contour".into(),
                env::var("CONTOUR").unwrap_or_else(|_| "local".into()),
            ),
            ("sensor".into(), "http".into()),
        ]);
        let registry = Registry::new_custom(None, Some(labels))?;
        let requests = IntCounterVec::new(
            Opts::new(
                "http_requests_total",
                "HTTP responses by path, method, and status code",
            ),
            &["path", "method", "code"],
        )?;
        let duration = HistogramVec::new(
            HistogramOpts::new(
                "http_request_duration_seconds",
                "Time to produce an HTTP response in seconds (excludes body transmission)",
            ),
            &["path", "method", "code"],
        )?;
        registry.register(Box::new(requests.clone()))?;
        registry.register(Box::new(duration.clone()))?;
        Ok(Self {
            registry,
            requests,
            duration,
            endpoints: Arc::new(Mutex::new(BTreeSet::new())),
        })
    }

    pub fn label(
        &self,
        path: &'static str,
    ) -> impl Filter<Extract = (), Error = warp::Rejection> + Copy + use<> {
        self.endpoints.lock().unwrap().insert(path);
        mark(Some(path))
    }

    pub fn endpoints_route(
        &self,
    ) -> impl Filter<Extract = (warp::reply::Json,), Error = warp::Rejection> + Clone + use<> {
        let endpoints = self.endpoints.clone();
        warp::path!("endpoints")
            .and(self.label("/endpoints"))
            .and(warp::get())
            .map(move || warp::reply::json(&*endpoints.lock().unwrap()))
    }

    pub fn instrument<S, B>(
        &self,
        service: S,
    ) -> impl Service<
        warp::http::Request<B>,
        Response = warp::reply::Response,
        Error = Infallible,
        Future: Send,
    > + Clone
    + use<S, B>
    where
        S: TowerService<
                warp::http::Request<B>,
                Response = warp::reply::Response,
                Error = Infallible,
            > + Clone
            + Send
            + Sync
            + 'static,
        S::Future: Send + 'static,
        B: hyper::body::Body,
    {
        let metrics = self.clone();
        hyper::service::service_fn(move |mut request: warp::http::Request<B>| {
            let label = RequestLabel(Arc::new(Mutex::new(Some("unmatched"))));
            request.extensions_mut().insert(label.clone());
            let method = request.method().clone();
            let started = Instant::now();
            let metrics = metrics.clone();
            let response = service.clone().call(request);

            async move {
                let response = response.await?;
                let path = *label.0.lock().unwrap();
                if let Some(path) = path {
                    let code = response.status().as_u16().to_string();
                    let labels = &[path, method.as_str(), code.as_str()];
                    metrics.requests.with_label_values(labels).inc();
                    metrics
                        .duration
                        .with_label_values(labels)
                        .observe(started.elapsed().as_secs_f64());
                }
                Ok::<_, Infallible>(response)
            }
        })
    }

    pub fn route(
        &self,
    ) -> impl Filter<Extract = (warp::reply::Response,), Error = warp::Rejection> + Clone + use<>
    {
        let registry = self.registry.clone();
        warp::path!("metrics")
            .and(mark(None))
            .and(warp::get())
            .map(move || {
                let encoder = TextEncoder::new();
                match encoder.encode_to_string(&registry.gather()) {
                    Ok(body) => warp::reply::with_header(
                        warp::reply::with_status(body, StatusCode::OK),
                        "content-type",
                        encoder.format_type(),
                    )
                    .into_response(),
                    Err(error) => {
                        eprintln!("failed to encode metrics: {error}");
                        warp::reply::with_status(
                            "Failed to encode metrics",
                            StatusCode::INTERNAL_SERVER_ERROR,
                        )
                        .into_response()
                    }
                }
            })
    }
}
