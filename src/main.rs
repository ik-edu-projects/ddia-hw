use hyper_util::{
    rt::{TokioExecutor, TokioIo},
    server::conn::auto::Builder,
};
use rand_distr::{Distribution, Normal};
use std::collections::BTreeMap;
use std::env;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use warp::{Filter, Rejection, http::StatusCode};

type Store = Arc<Mutex<BTreeMap<String, String>>>;

const TOO_MANY_REQUESTS_PROBABILITY: f64 = 0.01;
const MEAN_DELAY_MS: f64 = 20.0;
const DELAY_STD_DEV_MS: f64 = 5.0;
const KV_PATH: &str = "/kv/{key}";

#[derive(Debug)]
struct TooManyRequests;

impl warp::reject::Reject for TooManyRequests {}

fn simulated_request() -> impl Filter<Extract = (), Error = Rejection> + Clone {
    warp::any()
        .and_then(|| async {
            let delay = {
                let distribution = Normal::<f64>::new(MEAN_DELAY_MS, DELAY_STD_DEV_MS)
                    .expect("valid delay distribution");
                let milliseconds = distribution.sample(&mut rand::rng()).max(0.0);
                Duration::from_secs_f64(milliseconds / 1000.0)
            };
            tokio::time::sleep(delay).await;
            if rand::random_bool(TOO_MANY_REQUESTS_PROBABILITY) {
                Err(warp::reject::custom(TooManyRequests))
            } else {
                Ok(())
            }
        })
        .untuple_one()
}

#[tokio::main]
async fn main() {
    let metrics = metrics::Metrics::new().expect("failed to initialize metrics");
    let store: Store = Arc::new(Mutex::new(BTreeMap::new()));
    let with_store = warp::any().map(move || store.clone());
    let key = warp::path!("kv" / String);
    let simulated_request = simulated_request();

    let get = key
        .and(metrics.endpoint("GET", KV_PATH))
        .and(warp::get())
        .and(simulated_request.clone())
        .and(with_store.clone())
        .map(|key: String, store: Store| {
            let store = store.lock().unwrap();
            match store.get(&key) {
                Some(value) => warp::reply::with_status(value.clone(), StatusCode::OK),
                None => warp::reply::with_status(String::new(), StatusCode::NOT_FOUND),
            }
        });

    let put = key
        .and(metrics.endpoint("PUT", KV_PATH))
        .and(warp::put())
        .and(with_store.clone())
        .and(warp::body::content_length_limit(1024 * 1024))
        .and(warp::body::bytes())
        .and(simulated_request.clone())
        .map(|key: String, store: Store, body: bytes::Bytes| {
            match String::from_utf8(body.to_vec()) {
                Ok(value) => {
                    store.lock().unwrap().insert(key, value);
                    StatusCode::NO_CONTENT
                }
                Err(_) => StatusCode::BAD_REQUEST,
            }
        });

    let delete = key
        .and(metrics.endpoint("DELETE", KV_PATH))
        .and(warp::delete())
        .and(simulated_request)
        .and(with_store)
        .map(|key: String, store: Store| {
            if store.lock().unwrap().remove(&key).is_some() {
                StatusCode::NO_CONTENT
            } else {
                StatusCode::NOT_FOUND
            }
        });

    let routes = get
        .or(put)
        .or(delete)
        .or(metrics.endpoints_route())
        .or(metrics.route())
        .recover(|rejection: Rejection| async move {
            if rejection.find::<TooManyRequests>().is_some() {
                Ok(warp::reply::with_status(
                    String::new(),
                    StatusCode::TOO_MANY_REQUESTS,
                ))
            } else {
                Err(rejection)
            }
        });
    let service = metrics.instrument(warp::service(routes));

    let bind_addr = env::var("BIND_ADDR").unwrap_or_else(|_| "127.0.0.1:3030".into());
    let listener = tokio::net::TcpListener::bind(&bind_addr)
        .await
        .expect("failed to bind HTTP listener");
    println!("Listening on http://{bind_addr}");
    loop {
        let (stream, _) = listener
            .accept()
            .await
            .expect("failed to accept connection");
        let service = service.clone();
        tokio::spawn(async move {
            if let Err(error) = Builder::new(TokioExecutor::new())
                .serve_connection_with_upgrades(TokioIo::new(stream), service)
                .await
            {
                eprintln!("HTTP connection failed: {error}");
            }
        });
    }
}
mod metrics;
