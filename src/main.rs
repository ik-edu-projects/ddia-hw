use hyper_util::{
    rt::{TokioExecutor, TokioIo},
    server::conn::auto::Builder,
};
use std::collections::BTreeMap;
use std::env;
use std::sync::{Arc, Mutex};
use warp::{Filter, http::StatusCode};

type Store = Arc<Mutex<BTreeMap<String, String>>>;

#[tokio::main]
async fn main() {
    let metrics = metrics::Metrics::new().expect("failed to initialize metrics");
    let store: Store = Arc::new(Mutex::new(BTreeMap::new()));
    let with_store = warp::any().map(move || store.clone());
    let key = warp::path!("kv" / String).and(metrics.label("/kv/{key}"));

    let get = key
        .and(warp::get())
        .and(with_store.clone())
        .map(|key: String, store: Store| {
            let store = store.lock().unwrap();
            match store.get(&key) {
                Some(value) => warp::reply::with_status(value.clone(), StatusCode::OK),
                None => warp::reply::with_status(String::new(), StatusCode::NOT_FOUND),
            }
        });

    let put = key
        .and(warp::put())
        .and(with_store.clone())
        .and(warp::body::content_length_limit(1024 * 1024))
        .and(warp::body::bytes())
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
        .and(warp::delete())
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
        .or(metrics.route());
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
