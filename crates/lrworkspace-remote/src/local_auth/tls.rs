//! Ephemeral pinned TLS for local credential channels. No trust-store changes,
//! certificate/key files, session ticket persistence, or TLS key logging.
use super::*;
use rustls::pki_types::{CertificateDer, PrivatePkcs8KeyDer, ServerName};
use rustls::{
    ClientConfig, ClientConnection, RootCertStore, ServerConfig, ServerConnection, StreamOwned,
};

use std::time::Instant;
pub(super) struct TimedSocket {
    socket: TcpStream,
    read_until: Instant,
    write_until: Instant,
}
impl TimedSocket {
    fn new(socket: TcpStream) -> Self {
        let deadline = Instant::now() + Duration::from_secs(3);
        Self {
            socket,
            read_until: deadline,
            write_until: deadline,
        }
    }
    pub fn set_read_timeout(&mut self, duration: Option<Duration>) -> std::io::Result<()> {
        let duration =
            duration.ok_or_else(|| std::io::Error::other("Private channel requires a deadline"))?;
        self.read_until = Instant::now() + duration;
        self.socket.set_read_timeout(Some(duration))
    }
    pub fn set_write_timeout(&mut self, duration: Option<Duration>) -> std::io::Result<()> {
        let duration =
            duration.ok_or_else(|| std::io::Error::other("Private channel requires a deadline"))?;
        self.write_until = Instant::now() + duration;
        self.socket.set_write_timeout(Some(duration))
    }
    fn remaining(deadline: Instant) -> std::io::Result<Duration> {
        deadline
            .checked_duration_since(Instant::now())
            .filter(|left| !left.is_zero())
            .ok_or_else(|| {
                std::io::Error::new(
                    std::io::ErrorKind::TimedOut,
                    "Private channel deadline exceeded",
                )
            })
    }
}
impl Read for TimedSocket {
    fn read(&mut self, buffer: &mut [u8]) -> std::io::Result<usize> {
        self.socket
            .set_read_timeout(Some(Self::remaining(self.read_until)?))?;
        self.socket.read(buffer)
    }
}
impl Write for TimedSocket {
    fn write(&mut self, buffer: &[u8]) -> std::io::Result<usize> {
        self.socket
            .set_write_timeout(Some(Self::remaining(self.write_until)?))?;
        self.socket.write(buffer)
    }
    fn flush(&mut self) -> std::io::Result<()> {
        self.socket.flush()
    }
}
pub(super) type ServerStream = StreamOwned<ServerConnection, TimedSocket>;
pub(super) type ClientStream = StreamOwned<ClientConnection, TimedSocket>;
pub(super) struct Endpoint {
    server: Arc<ServerConfig>,
    certificate: CertificateDer<'static>,
}
fn invalid() -> CoreError {
    CoreError::Connection("Private local TLS channel failed verification".into())
}
impl Endpoint {
    pub fn new() -> CoreResult<Self> {
        let rcgen::CertifiedKey { cert, key_pair } =
            rcgen::generate_simple_self_signed(vec!["127.0.0.1".into()]).map_err(|_| invalid())?;
        let certificate = cert.der().clone();
        let private = PrivatePkcs8KeyDer::from(key_pair.serialize_der());
        let provider = Arc::new(rustls::crypto::ring::default_provider());
        let mut server = ServerConfig::builder_with_provider(provider)
            .with_safe_default_protocol_versions()
            .map_err(|_| invalid())?
            .with_no_client_auth()
            .with_single_cert(vec![certificate.clone()], private.into())
            .map_err(|_| invalid())?;
        server.send_tls13_tickets = 0;
        Ok(Self {
            server: Arc::new(server),
            certificate,
        })
    }
    pub fn certificate_b64(&self) -> String {
        crate::b64::encode(self.certificate.as_ref())
    }
    pub fn accept(&self, socket: TcpStream) -> CoreResult<ServerStream> {
        // Windows accepted sockets inherit a listener's nonblocking mode.
        // These bounded worker handshakes use blocking streams with deadlines.
        socket.set_nonblocking(false)?;
        socket.set_read_timeout(Some(Duration::from_secs(3)))?;
        socket.set_write_timeout(Some(Duration::from_secs(3)))?;
        Ok(StreamOwned::new(
            ServerConnection::new(self.server.clone()).map_err(|_| invalid())?,
            TimedSocket::new(socket),
        ))
    }
    #[cfg(test)]
    pub fn connect(&self, address: SocketAddr) -> CoreResult<ClientStream> {
        connect(address, self.certificate.clone())
    }
}
pub(super) fn connect(
    address: SocketAddr,
    certificate: CertificateDer<'static>,
) -> CoreResult<ClientStream> {
    if !address.ip().is_loopback() {
        return Err(invalid());
    }
    let mut roots = RootCertStore::empty();
    roots.add(certificate).map_err(|_| invalid())?;
    let provider = Arc::new(rustls::crypto::ring::default_provider());
    let client = ClientConfig::builder_with_provider(provider)
        .with_safe_default_protocol_versions()
        .map_err(|_| invalid())?
        .with_root_certificates(roots)
        .with_no_client_auth();
    let mut socket = TimedSocket::new(TcpStream::connect_timeout(
        &address,
        Duration::from_secs(3),
    )?);
    socket.set_read_timeout(Some(Duration::from_secs(3)))?;
    socket.set_write_timeout(Some(Duration::from_secs(3)))?;
    let mut connection = ClientConnection::new(
        Arc::new(client),
        ServerName::try_from("127.0.0.1").map_err(|_| invalid())?,
    )
    .map_err(|_| invalid())?;
    // Verify the pinned peer before writing any capability or form body.
    while connection.is_handshaking() {
        connection.complete_io(&mut socket).map_err(|_| invalid())?;
    }
    socket.set_read_timeout(Some(Duration::from_secs(120)))?;
    Ok(StreamOwned::new(connection, socket))
}
pub(super) fn read_line(stream: &mut impl Read, limit: usize) -> CoreResult<Zeroizing<Vec<u8>>> {
    let mut bytes = Zeroizing::new(Vec::new());
    while bytes.len() <= limit {
        let mut byte = [0];
        stream.read_exact(&mut byte)?;
        if byte[0] == b'\n' {
            return Ok(bytes);
        }
        bytes.push(byte[0]);
    }
    Err(CoreError::Validation(
        "Private local message is too large".into(),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn a_different_local_server_certificate_is_rejected_before_credentials_are_sent() {
        let endpoint = Endpoint::new().unwrap();
        let wrong = Endpoint::new().unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let server = std::thread::spawn(move || {
            let (socket, _) = listener.accept().unwrap();
            let mut stream = endpoint.accept(socket).unwrap();
            assert!(read_line(&mut stream, 1024).is_err());
        });
        assert!(wrong.connect(address).is_err());
        server.join().unwrap();
    }
}
