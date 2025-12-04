import bencodepy
import socket
import hashlib
import threading
import struct
import time
import sys
import json
import os
import hashlib

HOST = '127.0.0.1'
PORT = 5555
ANNOUNCE_INTERVAL = 9999

# TODO: ADD CONCURRENCY

# ---- constants for message types ----
MSG_HELLO    = 0x00  # no body
MSG_BITFIELD = 0x01  # ASCII list "0,1,3,5"
MSG_WANT     = 0x02  # 4-byte piece index
MSG_PIECE    = 0x03  # 4-byte piece index + payload bytes

# ---- helpers ----

# Reads exactly n bytes from sock or raise ConnectionError
def recv_exact(sock, n):
    buf = b""
    while len(buf) < n:
        chunk = sock.recv(n - len(buf))
        if not chunk:
            raise ConnectionError("socket closed before receiving expected bytes")
        buf += chunk
    return buf

# Formats the message and sens it into the provided socket
# Frame: [4-byte BE length L][1-byte type][L-1 bytes body] (length counts the type byte + body)
def send_message(sock, msg_type, body=b""):
    total_len = 1 + len(body)
    sock.sendall(struct.pack(">I", total_len))
    sock.sendall(struct.pack("B", msg_type))
    if body:
        sock.sendall(body)

# Reads the 4-byte length then the type+body.
# Returns the following tuple (msg_type:int, body:bytes)
def recv_message(sock):
    len_bytes = recv_exact(sock, 4)
    total_len = struct.unpack(">I", len_bytes)[0]
    if total_len < 1:
        raise ConnectionError("invalid message length")
    raw = recv_exact(sock, total_len)
    msg_type = raw[0]
    body = raw[1:]
    return msg_type, body

# ---- torrent loader ----
# Takes the .torrent file and decodes it for the peer
def load_torrent(torrent_path):
    raw = open(torrent_path, "rb").read()
    meta = bencodepy.decode(raw)
    info = meta[b"info"]
    # compute info_hash (sha1 of bencoded info dict)
    info_b = bencodepy.encode(info)
    info_hash = hashlib.sha1(info_b).hexdigest()
    piece_length = info[b"piece length"]
    total_length = None
    if b"length" in info:
        total_length = info[b"length"]
        file_name = info[b"name"].decode(errors="ignore")
        files = [(file_name, total_length)]
    elif b"files" in info:
        files = []
        total = 0
        for f in info[b"files"]:
            path = b"/".join(f[b"path"]).decode(errors="ignore")
            length = f[b"length"]
            files.append((path, length))
            total += length
        total_length = total
    else:
        raise Exception("Unsupported torrent structure")
    num_pieces = (total_length + piece_length - 1) // piece_length
    raw_pieces = info[b"pieces"]
    piece_hashes = [raw_pieces[i:i+20] for i in range(0, len(raw_pieces), 20)]
    return {
        "info_hash": info_hash,
        "piece_length": piece_length,
        "total_length": total_length,
        "files": files,
        "num_pieces": num_pieces,
        "piece_hashes": piece_hashes
    }

# ---- utility functions ----
def bitfield_to_body(have_pieces):
    # comma separated ASCII indices (empty -> b"")
    if not have_pieces:
        return b""
    return ",".join(str(i) for i in sorted(have_pieces)).encode()

def body_to_bitfield(body):
    if not body:
        return []
    return [int(i) for i in body.decode().split(",") if i]

def send_piece(sock, idx, payload):
    body = struct.pack(">I", idx) + payload
    send_message(sock, MSG_PIECE, body)

def parse_piece_body(body):
    if len(body) < 4:
        raise ValueError("piece body too small")
    idx = struct.unpack(">I", body[:4])[0]
    payload = body[4:]
    return idx, payload

# ---- server: respond to peers (uploads) ----
def handle_peer_conn(conn, pieces, have_pieces, piece_length):
    try:
        # Read HELLO
        msg_type, body = recv_message(conn)
        if msg_type != MSG_HELLO:
            # protocol violation
            conn.close()
            return
        # reply HELLO
        send_message(conn, MSG_HELLO)

        # Expect peer BITFIELD (or it might be empty)
        msg_type, body = recv_message(conn)
        if msg_type == MSG_BITFIELD:
            # we could inspect remote bitfield if needed
            peer_has = set(body_to_bitfield(body))
        else:
            peer_has = set()
            # if it's something else, handle accordingly below by pushing it back into processing loop

        # send bitfield
        send_message(conn, MSG_BITFIELD, bitfield_to_body(have_pieces))

        # main loop: wait for WANT messages and reply with PIECE messages
        while True:
            try:
                msg_type, body = recv_message(conn)
            except ConnectionError:
                break
            if msg_type == MSG_WANT:
                if len(body) != 4:
                    # malformed WANT
                    continue
                idx = struct.unpack(">I", body)[0]
                if idx in have_pieces:
                    chunk = pieces["data"][idx]
                    # send piece using length-prefixed framing
                    send_piece(conn, idx, chunk)
                else:
                    # peer requested a piece we don't have; ignore 
                    pass
            else:
                # ignore unknown or unexpected messages
                pass
    except Exception as e:
        # debug print for server side
        print("Peer handler error:", repr(e))
    finally:
        try:
            conn.shutdown(socket.SHUT_RDWR)
        except:
            pass
        conn.close()

# ---- server -----
# Accepts client connections and makes a thread to handle that connection
def run_peer_server(my_port, pieces, have_pieces, piece_length):
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    s.bind(("0.0.0.0", my_port))
    s.listen(8)
    print("Peer server listening on port", my_port)
    while True:
        conn, addr = s.accept()
        threading.Thread(target=handle_peer_conn, args=(conn, pieces, have_pieces, piece_length), daemon=True).start()

# ---- client: connect to peer and request pieces (downloads) ----
def connect_and_exchange(ip, port, pieces, have_pieces, piece_length, total_length, timeout=10.0):
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.settimeout(timeout)
    try:
        s.connect((ip, port))
    except Exception as e:
        print("Connect failed:", e)
        return
    try:
        # 1) HELLO handshake
        send_message(s, MSG_HELLO)
        msg_type, body = recv_message(s)
        if msg_type != MSG_HELLO:
            print("Handshake failed: expected HELLO")
            s.close()
            return

        # 2) send bitfield
        send_message(s, MSG_BITFIELD, bitfield_to_body(have_pieces))

        # 3) receive peer bitfield
        msg_type, body = recv_message(s)
        peer_pieces = []
        if msg_type == MSG_BITFIELD:
            peer_pieces = body_to_bitfield(body)

        # 4) request pieces that peer has and we don't
        for idx in peer_pieces:
            if idx in have_pieces:
                continue

            # compute piece size (last piece may be shorter so we have to account for that)
            start = idx * piece_length
            end = min(start + piece_length, total_length)
            size = end - start

            # send WANT (4-byte index)
            send_message(s, MSG_WANT, struct.pack(">I", idx))

            # now wait for a PIECE message for this idx (or timeout)
            try:
                msg_type, body = recv_message(s)
            except Exception as e:
                print(f"Failed receiving piece {idx} from {ip}: {e}")
                continue

            if msg_type != MSG_PIECE:
                # unexpected message; ignore and continue
                print(f"Expected PIECE but got type {msg_type} from {ip}")
                continue

            got_idx, payload = parse_piece_body(body)
            if got_idx != idx:
                print(f"Peer returned piece {got_idx} but we asked for {idx} — ignoring")
                continue

            if len(payload) != size:
                print(f"Piece {idx} length mismatch: expected {size} got {len(payload)}. Discarding")
                continue

            dh = hashlib.sha1(payload).digest()
            expected = pieces["piece_hashes"][idx]
            if dh == expected:
                pieces["data"][idx] = payload
                have_pieces.add(idx)
                print(f"Got piece {idx} from {ip}:{port}")
            else:
                print(f"Hash mismatch for piece {idx} from {ip}")
                print(f"expected {expected[:8]}... got {dh[:8]}...")
    except Exception as e:
        print("connect_and_exchange error:", repr(e))
    finally:
        try:
            s.shutdown(socket.SHUT_RDWR)
        except:
            pass
        s.close()

 # ---- reconstruct file ----
 # Takes all the pieces and merges them together
def reconstruct_file(pieces, torrent_meta, out_path):
    total_bytes = b"".join(pieces["data"][i] for i in range(torrent_meta["num_pieces"]))
    with open(out_path, "wb") as f:
        f.write(total_bytes)
    print("File written to", out_path)


# ---- HTTP GET formatter ----
# Formats the peers data into an HTTP GET request to send to the tracker
def http_get(host, port, path, params):
    query = "&".join([f"{k}={v}" for k, v in params.items()])
    full_path = f"{path}?{query}"
    req = (
        f"GET {full_path} HTTP/1.1\r\n"
        f"Host: {host}\r\n"
        "Connection: close\r\n"
        "\r\n"
    )
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.connect((host, port))
    s.sendall(req.encode())
    data = b""
    while True:
        chunk = s.recv(4096)
        if not chunk:
            break
        data += chunk
    s.close()
    if b"\r\n\r\n" not in data:
        return ""
    _, body = data.split(b"\r\n\r\n", 1)
    return body.decode(errors="ignore")

# ---- simple tracker announce ----
def announce_to_tracker(tracker_host, tracker_port, info_hash, peer_id, my_port, event="started"):
    params = {
        "peer_id": peer_id,
        "info_hash": info_hash,
        "port": my_port,
        "event": event
    }
    return http_get(tracker_host, tracker_port, "/announce", params)

# ---- main ----
if __name__ == "__main__":
    if len(sys.argv) <= 6:
        print("Usage: peer_client.py <.torrent-file> <tracker_ip> <tracker_port> <peer_id> <my_port> <stay_seeding>")
        sys.exit(1)

    torrent_path = sys.argv[1]
    tracker_ip = sys.argv[2]
    tracker_port = int(sys.argv[3])
    peer_id = sys.argv[4]
    my_port = int(sys.argv[5])
    stay_seeding = bool(sys.argv[6])

    torrent_meta = load_torrent(torrent_path)
    print("Loaded torrent:", torrent_meta)

    num = torrent_meta["num_pieces"]
    pieces = {
        "data": [b"" for _ in range(num)],
        "piece_hashes": torrent_meta["piece_hashes"]
    }
    have_pieces = set()

    # preload file if exists (verify pieces)
    # If it exists and is complete, we don't need to go and download from peers
    fname = torrent_meta["files"][0][0]
    if os.path.exists(fname):
        print(f"File '{fname}' exists locally — checking integrity...")
        with open(fname, "rb") as f:
            for i in range(num):
                start = i * torrent_meta["piece_length"]
                end = min(start + torrent_meta["piece_length"], torrent_meta["total_length"])
                size = end - start
                chunk = f.read(size)
                if len(chunk) != size:
                    break
                if hashlib.sha1(chunk).digest() == torrent_meta["piece_hashes"][i]:
                    pieces["data"][i] = chunk
                    have_pieces.add(i)
        if len(have_pieces) == num:
            print("File is already complete. Acting as a seeder only.")
            done_leeching = True
        else:
            done_leeching = False
    else:
        done_leeching = False

    # start server thread for interacting with other peers
    threading.Thread(target=run_peer_server, args=(my_port, pieces, have_pieces, torrent_meta["piece_length"]), daemon=True).start()

    # announce
    body = announce_to_tracker(tracker_ip, tracker_port, torrent_meta["info_hash"], peer_id, my_port)
    try:
        ANNOUNCE_INTERVAL = json.loads(body)[1]
    except Exception:
        ANNOUNCE_INTERVAL = 9999

    if not done_leeching:
        try:
            peers = json.loads(body)[0]
        except Exception:
            peers = []

        # Looping until we've downloaded the complete file
        while True:

            # retry until peers found 
            while not peers:
                print(f"No peers yet — retrying in {ANNOUNCE_INTERVAL}s")
                time.sleep(ANNOUNCE_INTERVAL)
                body = announce_to_tracker(tracker_ip, tracker_port, torrent_meta["info_hash"], peer_id, my_port)
                try:
                    peers = json.loads(body)[0]
                except Exception:
                    peers = []

            print("Got peers:", peers)

            for p in peers:
                connect_and_exchange(p["ip"], p["port"], pieces, have_pieces, torrent_meta["piece_length"], torrent_meta["total_length"])

            if len(have_pieces) == num:
                print("All pieces downloaded — reconstructing file")
                announce_to_tracker(tracker_ip, tracker_port, torrent_meta["info_hash"], peer_id, my_port, "completed")
                reconstruct_file(pieces, torrent_meta, fname)
                break
            else:
                print("Missing pieces:", set(range(num)) - have_pieces)
                print("Looking for peers to leech missing pieces")
            

    while stay_seeding:
        print("Staying online to seed for others")
        announce_to_tracker(tracker_ip, tracker_port, torrent_meta["info_hash"], peer_id, my_port)
        time.sleep(ANNOUNCE_INTERVAL)