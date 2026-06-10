//! End-to-end test of the cross-machine data path: a route whose backends mix
//! a local replica with a remote relay (another fleet machine). Asserts the
//! hub load-balances across both and that a dead backend falls through to a
//! live one instead of failing the visitor. No Docker — stub HTTP servers
//! stand in for the replica and the device relay.

use std::sync::Arc;
use std::time::Duration;

use hades_proxy::{Backend, Metrics, RouteTable};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

/// A trivial HTTP/1.1 server that answers every request with `body`.
async fn stub(body: &'static str) -> u16 {
    let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
    let port = listener.local_addr().unwrap().port();
    tokio::spawn(async move {
        loop {
            let Ok((mut sock, _)) = listener.accept().await else {
                return;
            };
            tokio::spawn(async move {
                let mut buf = [0u8; 2048];
                let _ = sock.read(&mut buf).await;
                let resp = format!(
                    "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                    body.len(),
                    body
                );
                let _ = sock.write_all(resp.as_bytes()).await;
                let _ = sock.shutdown().await;
            });
        }
    });
    port
}

async fn run_proxy_on(table: RouteTable) -> u16 {
    let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
    let port = listener.local_addr().unwrap().port();
    drop(listener); // free the port for run_proxy to bind
    let metrics = Arc::new(Metrics::default());
    tokio::spawn(async move {
        let _ = hades_proxy::run_proxy(table, port, metrics).await;
    });
    // give the listener a moment to come up
    tokio::time::sleep(Duration::from_millis(150)).await;
    port
}

async fn get(client: &reqwest::Client, proxy_port: u16) -> (u16, String) {
    let resp = client
        .get(format!("http://127.0.0.1:{proxy_port}/"))
        .header("host", "site.localhost")
        .timeout(Duration::from_secs(10))
        .send()
        .await
        .unwrap();
    let status = resp.status().as_u16();
    let body = resp.text().await.unwrap_or_default();
    (status, body)
}

#[tokio::test]
async fn load_balances_across_local_and_remote() {
    let local_port = stub("LOCAL").await;
    let device_port = stub("REMOTE").await;
    let table = RouteTable::new();
    table.set_app(
        "site",
        vec!["site.localhost".into()],
        vec![
            Backend::Local(format!("127.0.0.1:{local_port}").parse().unwrap()),
            Backend::Remote {
                // the device relay; our stub ignores the path and just answers
                relay: format!("http://127.0.0.1:{device_port}/_relay/site"),
                token: "device-token".into(),
            },
        ],
        None,
    );
    let proxy = run_proxy_on(table).await;
    let client = reqwest::Client::new();

    let mut bodies = Vec::new();
    for _ in 0..6 {
        let (status, body) = get(&client, proxy).await;
        assert_eq!(status, 200, "every request should succeed");
        bodies.push(body);
    }
    // round-robin must have hit both machines
    assert!(bodies.iter().any(|b| b == "LOCAL"), "local replica served some");
    assert!(bodies.iter().any(|b| b == "REMOTE"), "remote instance served some");
}

#[tokio::test]
async fn dead_local_backend_falls_through_to_remote() {
    let device_port = stub("REMOTE").await;
    let dead = {
        // bind then drop to obtain a port nothing is listening on
        let l = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
        let p = l.local_addr().unwrap().port();
        drop(l);
        p
    };
    let table = RouteTable::new();
    table.set_app(
        "site",
        vec!["site.localhost".into()],
        vec![
            Backend::Local(format!("127.0.0.1:{dead}").parse().unwrap()),
            Backend::Remote {
                relay: format!("http://127.0.0.1:{device_port}/_relay/site"),
                token: "t".into(),
            },
        ],
        None,
    );
    let proxy = run_proxy_on(table).await;
    let client = reqwest::Client::new();

    // even though round-robin leads with the dead local backend on some
    // requests, every response should still be a 200 from the surviving remote
    for _ in 0..6 {
        let (status, body) = get(&client, proxy).await;
        assert_eq!(status, 200);
        assert_eq!(body, "REMOTE");
    }
}
