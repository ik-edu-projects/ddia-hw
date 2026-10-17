use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use warp::{Filter, http::StatusCode};

type Store = Arc<Mutex<BTreeMap<String, String>>>;

#[tokio::main]
async fn main() {
    let store: Store = Arc::new(Mutex::new(BTreeMap::new()));
    let with_store = warp::any().map(move || store.clone());
    let key = warp::path!("kv" / String);

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

    println!("Listening on http://127.0.0.1:3030");
    warp::serve(get.or(put).or(delete))
        .run(([127, 0, 0, 1], 3030))
        .await;
}
