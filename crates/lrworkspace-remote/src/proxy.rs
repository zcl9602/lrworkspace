//! Native SSH ProxyCommand relay. No Python, shell script or stdout diagnostics.
use std::io::{Read, Write};
use std::net::{IpAddr, Shutdown, TcpStream, ToSocketAddrs};
use std::time::{Duration, Instant};
use lrworkspace_core::{CoreError,CoreResult};

fn remaining(deadline:Instant)->CoreResult<Duration> {
    deadline.checked_duration_since(Instant::now()).filter(|d|!d.is_zero()).ok_or_else(||CoreError::Timeout("SOCKS5 connection deadline exceeded".into()))
}
fn read_handshake(socket:&mut TcpStream,mut bytes:&mut [u8],deadline:Instant)->CoreResult<()> {
    while !bytes.is_empty() {
        socket.set_read_timeout(Some(remaining(deadline)?))?;
        match socket.read(bytes) {
            Ok(0)=>return Err(std::io::Error::from(std::io::ErrorKind::UnexpectedEof).into()),
            Ok(count)=>bytes=&mut bytes[count..],
            Err(error) if error.kind()==std::io::ErrorKind::Interrupted=>{},
            Err(error) if matches!(error.kind(),std::io::ErrorKind::TimedOut|std::io::ErrorKind::WouldBlock)=>return Err(CoreError::Timeout("SOCKS5 connection deadline exceeded".into())),
            Err(error)=>return Err(error.into()),
        }
    }
    Ok(())
}

pub fn connect_socks5(proxy:&str,host:&str,port:u16,timeout:Duration)->CoreResult<TcpStream> {
    if port==0 || host.is_empty() || host.chars().any(char::is_control) || timeout.is_zero() {return Err(CoreError::Validation("A target hostname, port 1-65535 and positive timeout are required".into()));}
    let deadline=Instant::now().checked_add(timeout).ok_or_else(||CoreError::Validation("SOCKS5 timeout is too large".into()))?;
    let mut socket=None;
    for address in proxy.to_socket_addrs()? {
        let Some(budget)=deadline.checked_duration_since(Instant::now()) else {break};
        if let Ok(stream)=TcpStream::connect_timeout(&address,budget){socket=Some(stream);break;}
    }
    let mut socket=socket.ok_or_else(||CoreError::Connection(format!("Cannot connect to SOCKS5 proxy {proxy}")))?;
    let budget=deadline.checked_duration_since(Instant::now()).ok_or_else(||CoreError::Timeout("SOCKS5 connection deadline exceeded".into()))?;
    socket.set_read_timeout(Some(budget))?;socket.set_write_timeout(Some(budget))?;
    socket.write_all(&[5,1,0])?;
    let mut greeting=[0;2];read_handshake(&mut socket,&mut greeting,deadline)?;
    if greeting!=[5,0]{return Err(CoreError::Connection("SOCKS5 proxy did not accept no-authentication mode; use an appropriate authenticated ProxyCommand instead".into()));}
    let mut request=vec![5,1,0];
    match host.trim_matches(['[',']']).parse::<IpAddr>() {
        Ok(IpAddr::V4(ip))=>{request.push(1);request.extend_from_slice(&ip.octets());},
        Ok(IpAddr::V6(ip))=>{request.push(4);request.extend_from_slice(&ip.octets());},
        Err(_)=>{let length=u8::try_from(host.len()).map_err(|_|CoreError::Validation("SOCKS5 hostname exceeds 255 bytes".into()))?;request.extend_from_slice(&[3,length]);request.extend_from_slice(host.as_bytes());},
    }
    request.extend_from_slice(&port.to_be_bytes());socket.set_write_timeout(Some(remaining(deadline)?))?;socket.write_all(&request)?;
    let mut header=[0;4];read_handshake(&mut socket,&mut header,deadline)?;
    if header[0]!=5{return Err(CoreError::Connection("Invalid SOCKS5 response version".into()));}
    if header[1]!=0 {
        let reason=match header[1]{2=>"connection not permitted",3=>"network unreachable",4=>"host unreachable",5=>"target connection refused",6=>"TTL expired",7=>"command unsupported",8=>"address type unsupported",_=>"proxy failure"};
        return Err(CoreError::Connection(format!("SOCKS5 {reason} for {host}:{port} (reply {})",header[1])));
    }
    let count=match header[3]{1=>4,4=>16,3=>{let mut size=[0];read_handshake(&mut socket,&mut size,deadline)?;size[0] as usize},_=>return Err(CoreError::Connection("Invalid SOCKS5 address type".into()))};
    let mut bound=vec![0;count+2];read_handshake(&mut socket,&mut bound,deadline)?;
    socket.set_read_timeout(None)?;socket.set_write_timeout(None)?;
    Ok(socket)
}

pub fn relay_stdio(proxy:&str,host:&str,port:u16,timeout:Duration)->CoreResult<()> {
    let mut socket=connect_socks5(proxy,host,port,timeout)?;
    let mut writer=socket.try_clone()?;
    // Detach the stdin reader: when the peer closes, main can exit even if
    // SSH still holds stdin open. std threads do not delay process termination.
    std::thread::spawn(move||{let _=std::io::copy(&mut std::io::stdin().lock(),&mut writer);let _=writer.shutdown(Shutdown::Write);});
    // StdoutLock is line buffered. SSH key-exchange packets need an immediate
    // flush even without a newline, or both endpoints can wait indefinitely.
    let mut output=std::io::stdout().lock();let mut buffer=[0;32768];
    loop {let count=socket.read(&mut buffer)?;if count==0 {break;}output.write_all(&buffer[..count])?;output.flush()?;}
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn fragmented_handshake_obeys_one_total_deadline() {
        let listener=std::net::TcpListener::bind("127.0.0.1:0").unwrap();let endpoint=listener.local_addr().unwrap();
        let worker=std::thread::spawn(move||{let (mut socket,_)=listener.accept().unwrap();socket.set_nodelay(true).unwrap();let mut greeting=[0;3];socket.read_exact(&mut greeting).unwrap();std::thread::sleep(Duration::from_millis(100));let _=socket.write_all(&[5]);std::thread::sleep(Duration::from_millis(200));let _=socket.write_all(&[0]);});
        let error=connect_socks5(&endpoint.to_string(),"10.0.0.1",22,Duration::from_millis(250)).unwrap_err();assert!(matches!(error,CoreError::Timeout(_)),"{error}");worker.join().unwrap();
        assert!(connect_socks5(&endpoint.to_string(),"host",22,Duration::ZERO).is_err());
    }
    #[test]
    fn fragmented_proxy_reply_keeps_domain_port_and_binary_stream_intact() {
        let listener=std::net::TcpListener::bind("127.0.0.1:0").unwrap();let endpoint=listener.local_addr().unwrap();
        let worker=std::thread::spawn(move||{
            let (mut socket,_)=listener.accept().unwrap();socket.set_read_timeout(Some(Duration::from_secs(3))).unwrap();
            let mut greeting=[0;3];socket.read_exact(&mut greeting).unwrap();assert_eq!(greeting,[5,1,0]);
            socket.write_all(&[5]).unwrap();socket.write_all(&[0]).unwrap();
            let mut header=[0;5];socket.read_exact(&mut header).unwrap();assert_eq!(&header[..4],&[5,1,0,3]);
            let mut name=vec![0;header[4] as usize];socket.read_exact(&mut name).unwrap();assert_eq!(name,b"cluster.test");
            let mut port=[0;2];socket.read_exact(&mut port).unwrap();assert_eq!(u16::from_be_bytes(port),2048);
            for byte in [5,0,0,4,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,1,0,22] {socket.write_all(&[byte]).unwrap();}
            socket.write_all(b"SSH-2.0-test\r\n\0\xff").unwrap();
        });
        let mut stream=connect_socks5(&endpoint.to_string(),"cluster.test",2048,Duration::from_secs(3)).unwrap();let mut bytes=Vec::new();stream.read_to_end(&mut bytes).unwrap();assert_eq!(bytes,b"SSH-2.0-test\r\n\0\xff");worker.join().unwrap();
    }
    #[test]
    fn refused_target_reports_target_not_an_ambiguous_timeout() {
        let listener=std::net::TcpListener::bind("127.0.0.1:0").unwrap();let endpoint=listener.local_addr().unwrap();
        let worker=std::thread::spawn(move||{let (mut socket,_)=listener.accept().unwrap();let mut greeting=[0;3];socket.read_exact(&mut greeting).unwrap();socket.write_all(&[5,0]).unwrap();let mut request=[0;10];socket.read_exact(&mut request).unwrap();socket.write_all(&[5,5,0,1]).unwrap();});
        let error=connect_socks5(&endpoint.to_string(),"10.0.0.1",22,Duration::from_secs(3)).unwrap_err().to_string();assert!(error.contains("refused")&&error.contains("10.0.0.1:22"));worker.join().unwrap();
    }
}
