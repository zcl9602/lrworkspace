"""Synthetic localhost SSH authentication fixture. Test dependency only."""
import hmac
import json
import logging
import socket
import sys
import threading
import paramiko

logging.disable(logging.CRITICAL)
settings=json.loads(sys.stdin.readline())
expected=settings.pop('password')
key=paramiko.RSAKey.generate(2048)
listener=socket.socket()
listener.bind(('127.0.0.1',0))
listener.listen(8)
listener.settimeout(0.2)
stopped=threading.Event()
print(json.dumps({'port':listener.getsockname()[1],'host_key':key.get_name()+' '+key.get_base64()}),flush=True)

class Server(paramiko.ServerInterface):
    def __init__(self):self.ready=threading.Event()
    def get_allowed_auths(self,username):return 'password'
    def check_auth_password(self,username,password):
        return paramiko.AUTH_SUCCESSFUL if username=='fixture' and hmac.compare_digest(password.encode('utf-8'),expected.encode('utf-8')) else paramiko.AUTH_FAILED
    def check_channel_request(self,kind,chanid):
        return paramiko.OPEN_SUCCEEDED if kind=='session' else paramiko.OPEN_FAILED_ADMINISTRATIVELY_PROHIBITED
    def check_channel_exec_request(self,channel,command):
        self.ready.set();return True

def serve(connection):
    transport=paramiko.Transport(connection)
    try:
        transport.add_server_key(key)
        server=Server()
        transport.start_server(server=server)
        channel=transport.accept(5)
        if channel is not None and server.ready.wait(5):
            channel.sendall(b'AUTHENTICATED\n')
            channel.send_exit_status(0)
            channel.shutdown_write()
            channel.close()
            # Let OpenSSH consume channel-close and send its disconnect before
            # closing TCP. Abrupt teardown races Linux's final disconnect send.
            transport.join(timeout=5)
    except Exception:
        pass
    finally:
        transport.close()

def accept():
    while not stopped.is_set():
        try:connection,_=listener.accept()
        except socket.timeout:continue
        except OSError:break
        threading.Thread(target=serve,args=(connection,),daemon=True).start()

threading.Thread(target=accept,daemon=True).start()
# Parent keeps this pipe open. No credentials or command bodies are echoed.
sys.stdin.read()
stopped.set()
listener.close()
