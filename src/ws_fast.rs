use std::future::Future;
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use anyhow::{Context, anyhow, bail};
use bytes::Bytes;
use fastwebsockets::{WebSocket, WebSocketError, handshake};
use http_body_util::Empty;
use hyper::header::{CONNECTION, HOST, UPGRADE};
use hyper::upgrade::Upgraded;
use hyper::{Request, Uri};
use hyper_util::rt::TokioIo;
use socket2::{SockRef, TcpKeepalive};
use tokio::net::TcpStream;
use tokio_rustls::TlsConnector;
use tokio_rustls::rustls::pki_types::ServerName;
use tokio_rustls::rustls::{ClientConfig, RootCertStore};

type FastWebSocket = WebSocket<TokioIo<Upgraded>>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum WsScheme {
    Ws,
    Wss,
}

#[derive(Debug, Clone)]
struct WsEndpoint {
    scheme: WsScheme,
    host: String,
    port: u16,
    host_header: String,
    request_target: String,
}

struct SpawnExecutor;

impl<Fut> hyper::rt::Executor<Fut> for SpawnExecutor
where
    Fut: Future + Send + 'static,
    Fut::Output: Send + 'static,
{
    fn execute(&self, fut: Fut) {
        tokio::task::spawn(fut);
    }
}

fn parse_endpoint(ws_url: &str) -> anyhow::Result<WsEndpoint> {
    let uri: Uri = ws_url
        .parse()
        .with_context(|| format!("invalid websocket URL: {ws_url}"))?;

    let scheme = match uri.scheme_str() {
        Some("ws") | None => WsScheme::Ws,
        Some("wss") => WsScheme::Wss,
        Some(other) => bail!("unsupported websocket scheme '{other}' in URL: {ws_url}"),
    };

    let host = uri
        .host()
        .map(str::to_owned)
        .ok_or_else(|| anyhow!("missing host in websocket URL: {ws_url}"))?;
    let explicit_port = uri.port_u16();
    let port = explicit_port.unwrap_or(match scheme {
        WsScheme::Ws => 80,
        WsScheme::Wss => 443,
    });
    let host_header = match explicit_port {
        Some(_) => format!("{host}:{port}"),
        None => host.clone(),
    };
    let request_target = uri
        .path_and_query()
        .map(|path| path.as_str().to_owned())
        .unwrap_or_else(|| "/".to_owned());

    Ok(WsEndpoint {
        scheme,
        host,
        port,
        host_header,
        request_target,
    })
}

fn handshake_request(
    request_target: &str,
    host_header: &str,
    extra_headers: &[(&str, &str)],
) -> anyhow::Result<Request<Empty<Bytes>>> {
    let mut builder = Request::builder()
        .method("GET")
        .uri(request_target)
        .header(HOST, host_header)
        .header(UPGRADE, "websocket")
        .header(CONNECTION, "upgrade")
        .header("Sec-WebSocket-Key", handshake::generate_key())
        .header("Sec-WebSocket-Version", "13");

    for (name, value) in extra_headers {
        builder = builder.header(*name, *value);
    }

    builder
        .body(Empty::<Bytes>::new())
        .context("failed to build websocket handshake request")
}

fn tls_connector() -> &'static TlsConnector {
    static CONNECTOR: OnceLock<TlsConnector> = OnceLock::new();
    CONNECTOR.get_or_init(|| {
        let mut roots = RootCertStore::empty();
        roots.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
        let config = ClientConfig::builder()
            .with_root_certificates(roots)
            .with_no_client_auth();
        TlsConnector::from(Arc::new(config))
    })
}

pub fn websocket_status_code(err: &anyhow::Error) -> Option<u16> {
    for source in err.chain() {
        if let Some(WebSocketError::InvalidStatusCode(code)) =
            source.downcast_ref::<WebSocketError>()
        {
            return Some(*code);
        }
    }
    None
}

fn tune_ws_tcp_socket(tcp_stream: &TcpStream) {
    let sock = SockRef::from(tcp_stream);

    let ka = TcpKeepalive::new().with_time(Duration::from_secs(30));
    if let Err(err) = sock.set_tcp_keepalive(&ka) {
        tracing::debug!(error = %err, "failed to set TCP keepalive");
    }

    if let Err(err) = sock.set_recv_buffer_size(256 * 1024) {
        tracing::debug!(error = %err, "failed to set TCP recv buffer size");
    }
}

pub async fn connect_fast_websocket_with_headers(
    ws_url: &str,
    extra_headers: &[(&str, &str)],
) -> anyhow::Result<FastWebSocket> {
    let endpoint = parse_endpoint(ws_url)?;
    let addr = format!("{}:{}", endpoint.host, endpoint.port);
    let request = handshake_request(
        &endpoint.request_target,
        &endpoint.host_header,
        extra_headers,
    )?;
    let tcp_stream = TcpStream::connect(&addr)
        .await
        .with_context(|| format!("failed to connect websocket TCP stream: {addr}"))?;
    let _ = tcp_stream.set_nodelay(true);
    tune_ws_tcp_socket(&tcp_stream);

    match endpoint.scheme {
        WsScheme::Ws => {
            let (ws, _) = handshake::client(&SpawnExecutor, request, tcp_stream)
                .await
                .context("websocket handshake failed")?;
            Ok(ws)
        }
        WsScheme::Wss => {
            let server_name = ServerName::try_from(endpoint.host.clone())
                .map_err(|_| anyhow!("invalid websocket DNS host: {}", endpoint.host))?;
            let tls_stream = tls_connector()
                .connect(server_name, tcp_stream)
                .await
                .context("failed to establish websocket TLS stream")?;
            let (ws, _) = handshake::client(&SpawnExecutor, request, tls_stream)
                .await
                .context("websocket handshake failed")?;
            Ok(ws)
        }
    }
}

pub async fn connect_fast_websocket(ws_url: &str) -> anyhow::Result<FastWebSocket> {
    connect_fast_websocket_with_headers(ws_url, &[]).await
}
