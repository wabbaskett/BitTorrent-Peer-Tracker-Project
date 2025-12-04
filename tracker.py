import bencodepy as bencode
import threading
import socket
import time
import json

HOST = '127.0.0.1'
PORT = 5555

PEER_LIST = {}
PEER_TIMEOUT = 10  
ANNOUNCE_INTERVAL = 5

#Function that will parse the peers HTTP GET request
def parse_http_get(request_data):
    try:
        lines = request_data.split("\r\n")
        request_line = lines[0]

        method, full_path, _ = request_line.split(" ", 2)
        if method != "GET":
            return None, None

        if "?" in full_path:
            path, query = full_path.split("?", 1)
        else:
            path, query = full_path, ""

        return path, query
    except:
        return None, None

# Manually parse key=value&key2=value2 into a dictionary
# This lets us pass it to other peers
def parse_query_string(qs):
    params = {}
    if not qs:
        return params

    parts = qs.split("&")
    for part in parts:
        if "=" in part:
            k, v = part.split("=", 1)
            params[k] = v

    return params

def handle_client(conn, addr):
    data = conn.recv(4096).decode(errors="ignore")
    if not data:
        conn.close()
        return

    print(data)
    path, query = parse_http_get(data)
    if path != "/announce":
        conn.sendall(b"HTTP/1.1 404 Not Found\r\n\r\n")
        conn.close()
        return

    params = parse_query_string(query)
    
    print(params)

    info_hash = params.get("info_hash")
    peer_id   = params.get("peer_id")
    port      = params.get("port")
    event     = params.get("event", "started")

    if not info_hash or not peer_id or not port:
        conn.sendall(b"HTTP/1.1 400 Bad Request\r\n\r\nMissing parameters")
        conn.close()
        return

    ip = addr[0]  # use connection address, safer than trusting param

    # Register peer
    PEER_LIST.setdefault(info_hash, {})
    peers = PEER_LIST[info_hash]

    if event != "stopped":
        peers[peer_id] = {
            "ip": ip,
            "peer_id":peer_id,
            "port": int(port),
            "last_seen": time.time()
        }
    else:
        peers.pop(peer_id, None)

    # Build list of peers except caller
    peer_list = [
        {"ip": peer["ip"], "peer_id":peer["peer_id"], "port": peer["port"], "last_seen":peer["last_seen"]}
        for pid, peer in peers.items()
        if pid != peer_id
    ]

    body = json.dumps([peer_list, ANNOUNCE_INTERVAL]).encode()

    print(f"SENDING: {body}")

    response = (
        b"HTTP/1.1 200 OK\r\n"
        b"Content-Type: application/json\r\n"
        b"Content-Length: " + str(len(body)).encode() + b"\r\n"
        b"\r\n" +
        body
    )

    conn.sendall(response)
    conn.close()

def timeout_peers():
    while True:
        print("RUNNING TIMEOUT CHECK")
        now = time.time()
        for info_hash in list(PEER_LIST.keys()):
            peers = PEER_LIST[info_hash]
            for pid in list(peers.keys()):
                if now - peers[pid]["last_seen"] > PEER_TIMEOUT:
                    print(f"removing {pid}")
                    del peers[pid]
            if not peers:
                print(f"no more seeders for {info_hash}")
                del PEER_LIST[info_hash]
        time.sleep(PEER_TIMEOUT)



if __name__ == "__main__":

    # Cleanup thread
    threading.Thread(target=timeout_peers, daemon=True).start()

    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.bind((HOST, PORT))
    server.listen(100)
    peer_lock = threading.Lock()
    print(f"Tracker running on http://{HOST}:{PORT}/announce")

    while True:
        conn, addr = server.accept()
        threading.Thread(target=handle_client, args=(conn, addr), daemon=True).start()



