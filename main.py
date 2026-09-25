from pathlib import Path
from datetime import datetime
import hashlib
import math
import os
import shutil
import sqlite3
import uuid

from fastapi import FastAPI, UploadFile, File as FastAPIFile, HTTPException
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

BASE_DIR = Path(__file__).resolve().parent.parent
DB_PATH = BASE_DIR / "vault.db"
STORAGE_DIR = BASE_DIR / "storage_nodes"
FRONTEND_DIR = BASE_DIR / "frontend"

CHUNK_SIZE = 4 * 1024 * 1024
REPLICATION_FACTOR = 3

app = FastAPI(title="Vault", version="1.0.0")

nodes = {
    f"node{i}": {
        "name": f"Node {i:02d}",
        "status": "ONLINE",
        "total_storage": 10 * 1024**3,
        "used_storage": 0,
        "last_heartbeat": datetime.utcnow().isoformat(),
    }
    for i in range(1, 6)
}

logs = []


def log(message):
    stamp = datetime.now().strftime("%H:%M:%S")
    logs.insert(0, f"[{stamp}] {message}")
    del logs[80:]


def db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def init_db():
    conn = db()
    conn.executescript("""
    CREATE TABLE IF NOT EXISTS files (
        id TEXT PRIMARY KEY,
        filename TEXT NOT NULL,
        original_size INTEGER NOT NULL,
        total_chunks INTEGER NOT NULL,
        replication_factor INTEGER NOT NULL,
        file_checksum TEXT NOT NULL,
        status TEXT NOT NULL,
        created_at TEXT NOT NULL
    );

    CREATE TABLE IF NOT EXISTS chunks (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        file_id TEXT NOT NULL,
        chunk_index INTEGER NOT NULL,
        chunk_size INTEGER NOT NULL,
        checksum TEXT NOT NULL
    );

    CREATE TABLE IF NOT EXISTS replicas (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        chunk_id INTEGER NOT NULL,
        node_id TEXT NOT NULL,
        path TEXT NOT NULL,
        checksum TEXT NOT NULL,
        status TEXT NOT NULL
    );

    CREATE TABLE IF NOT EXISTS repairs (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        chunk_id INTEGER NOT NULL,
        source_node TEXT,
        destination_node TEXT,
        reason TEXT,
        status TEXT,
        started_at TEXT,
        completed_at TEXT
    );
    """)
    conn.commit()
    conn.close()


init_db()


def sha256(data):
    return hashlib.sha256(data).hexdigest()


def available_nodes():
    return [n for n, info in nodes.items() if info["status"] in ("ONLINE", "DEGRADED")]


def choose_nodes(count):
    return available_nodes()[:count]


def node_path(node_id, file_id, chunk_index):
    folder = STORAGE_DIR / node_id / "chunks" / file_id
    folder.mkdir(parents=True, exist_ok=True)
    return folder / f"chunk_{chunk_index:06d}.bin"


def get_file_row(file_id):
    conn = db()
    row = conn.execute("SELECT * FROM files WHERE id=?", (file_id,)).fetchone()
    conn.close()
    return row


@app.get("/api/health")
def health():
    online = len(available_nodes())
    status = "HEALTHY" if online == len(nodes) else "DEGRADED"
    return {
        "success": True,
        "status": status,
        "database": "HEALTHY",
        "online_nodes": online,
        "total_nodes": len(nodes),
    }


@app.get("/api/metrics")
def metrics():
    conn = db()
    total_files = conn.execute("SELECT COUNT(*) c FROM files").fetchone()["c"]
    total_chunks = conn.execute("SELECT COUNT(*) c FROM chunks").fetchone()["c"]
    total_replicas = conn.execute("SELECT COUNT(*) c FROM replicas WHERE status='HEALTHY'").fetchone()["c"]
    degraded = conn.execute("SELECT COUNT(*) c FROM files WHERE status='DEGRADED'").fetchone()["c"]
    repairs = conn.execute("SELECT COUNT(*) c FROM repairs WHERE status='COMPLETED'").fetchone()["c"]
    conn.close()
    used = sum(v["used_storage"] for v in nodes.values())
    return {
        "total_files": total_files,
        "total_chunks": total_chunks,
        "total_replicas": total_replicas,
        "degraded_files": degraded,
        "repair_operations": repairs,
        "total_storage": sum(v["total_storage"] for v in nodes.values()),
        "used_storage": used,
        "online_nodes": len(available_nodes()),
    }


@app.get("/api/nodes")
def get_nodes():
    conn = db()
    replica_counts = {
        row["node_id"]: row["c"]
        for row in conn.execute(
            "SELECT node_id, COUNT(*) c FROM replicas WHERE status='HEALTHY' GROUP BY node_id"
        ).fetchall()
    }
    conn.close()
    result = []
    for node_id, info in nodes.items():
        item = dict(info)
        item["id"] = node_id
        item["chunks"] = replica_counts.get(node_id, 0)
        item["replicas"] = replica_counts.get(node_id, 0)
        result.append(item)
    return result


@app.post("/api/nodes/{node_id}/toggle")
def toggle_node(node_id: str):
    if node_id not in nodes:
        raise HTTPException(404, "Node not found")
    old = nodes[node_id]["status"]
    nodes[node_id]["status"] = "OFFLINE" if old != "OFFLINE" else "ONLINE"
    if nodes[node_id]["status"] == "OFFLINE":
        conn = db()
        conn.execute(
            "UPDATE replicas SET status='UNAVAILABLE' WHERE node_id=? AND status='HEALTHY'",
            (node_id,),
        )
        conn.execute(
            """UPDATE files SET status='DEGRADED'
               WHERE id IN (
                   SELECT DISTINCT c.file_id
                   FROM chunks c JOIN replicas r ON r.chunk_id=c.id
                   WHERE r.node_id=? AND r.status='UNAVAILABLE'
               )""",
            (node_id,),
        )
        conn.commit()
        conn.close()
        log(f"NODE FAILURE • {nodes[node_id]['name']} marked OFFLINE")
    else:
        conn = db()
        conn.execute(
            "UPDATE replicas SET status='HEALTHY' WHERE node_id=? AND status='UNAVAILABLE'",
            (node_id,),
        )
        conn.commit()
        conn.close()
        log(f"NODE RESTORED • {nodes[node_id]['name']} is ONLINE")
    return {"success": True, "node": node_id, "status": nodes[node_id]["status"]}


@app.get("/api/files")
def list_files():
    conn = db()
    rows = conn.execute("SELECT * FROM files ORDER BY created_at DESC").fetchall()
    conn.close()
    return [dict(r) for r in rows]


@app.get("/api/files/{file_id}")
def file_details(file_id: str):
    row = get_file_row(file_id)
    if not row:
        raise HTTPException(404, "File not found")
    conn = db()
    chunks = conn.execute(
        "SELECT * FROM chunks WHERE file_id=? ORDER BY chunk_index", (file_id,)
    ).fetchall()
    data = []
    for chunk in chunks:
        replicas = conn.execute(
            "SELECT * FROM replicas WHERE chunk_id=?", (chunk["id"],)
        ).fetchall()
        data.append({**dict(chunk), "replicas": [dict(r) for r in replicas]})
    conn.close()
    return {**dict(row), "chunks": data}


@app.post("/api/files/upload")
async def upload(file: UploadFile = FastAPIFile(...)):
    data = await file.read()
    if not data:
        raise HTTPException(400, "Empty file")
    file_id = uuid.uuid4().hex[:12]
    checksum = sha256(data)
    total_chunks = math.ceil(len(data) / CHUNK_SIZE)
    selected = choose_nodes(REPLICATION_FACTOR)
    if not selected:
        raise HTTPException(503, "No storage nodes available")

    status = "HEALTHY" if len(selected) == REPLICATION_FACTOR else "DEGRADED"
    conn = db()
    conn.execute(
        """INSERT INTO files
           (id, filename, original_size, total_chunks, replication_factor, file_checksum, status, created_at)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
        (file_id, file.filename, len(data), total_chunks, REPLICATION_FACTOR, checksum, status, datetime.utcnow().isoformat()),
    )

    for idx in range(total_chunks):
        chunk = data[idx * CHUNK_SIZE:(idx + 1) * CHUNK_SIZE]
        chunk_sum = sha256(chunk)
        cur = conn.execute(
            "INSERT INTO chunks (file_id, chunk_index, chunk_size, checksum) VALUES (?, ?, ?, ?)",
            (file_id, idx, len(chunk), chunk_sum),
        )
        chunk_id = cur.lastrowid
        for node_id in selected:
            path = node_path(node_id, file_id, idx)
            path.write_bytes(chunk)
            conn.execute(
                "INSERT INTO replicas (chunk_id, node_id, path, checksum, status) VALUES (?, ?, ?, ?, 'HEALTHY')",
                (chunk_id, node_id, str(path), chunk_sum),
            )
            nodes[node_id]["used_storage"] += len(chunk)
    conn.commit()
    conn.close()

    log(f"UPLOAD • {file.filename} • {total_chunks} chunks • {len(selected)}x replicas")
    return {"success": True, "file_id": file_id, "checksum": checksum, "chunks": total_chunks, "status": status}


@app.get("/api/files/{file_id}/download")
def download(file_id: str):
    row = get_file_row(file_id)
    if not row:
        raise HTTPException(404, "File not found")

    conn = db()
    chunks = conn.execute(
        "SELECT * FROM chunks WHERE file_id=? ORDER BY chunk_index", (file_id,)
    ).fetchall()
    output = bytearray()

    for chunk in chunks:
        replicas = conn.execute(
            "SELECT * FROM replicas WHERE chunk_id=? AND status='HEALTHY'",
            (chunk["id"],),
        ).fetchall()

        valid = False
        for replica in replicas:
            p = Path(replica["path"])
            if not p.exists():
                continue
            raw = p.read_bytes()
            if sha256(raw) == chunk["checksum"]:
                output.extend(raw)
                valid = True
                break
            conn.execute("UPDATE replicas SET status='CORRUPTED' WHERE id=?", (replica["id"],))

        if not valid:
            conn.commit()
            conn.close()
            raise HTTPException(503, f"No valid replica available for chunk {chunk['chunk_index']}")

    conn.close()

    temp = BASE_DIR / f".download_{file_id}_{uuid.uuid4().hex}"
    temp.write_bytes(output)
    log(f"DOWNLOAD • {row['filename']} • checksum verified")
    return FileResponse(temp, filename=row["filename"], media_type="application/octet-stream", background=None)


@app.delete("/api/files/{file_id}")
def delete_file(file_id: str):
    row = get_file_row(file_id)
    if not row:
        raise HTTPException(404, "File not found")
    conn = db()
    replicas = conn.execute(
        """SELECT r.* FROM replicas r
           JOIN chunks c ON c.id=r.chunk_id
           WHERE c.file_id=?""", (file_id,)
    ).fetchall()
    for r in replicas:
        try:
            Path(r["path"]).unlink(missing_ok=True)
        except Exception:
            pass
    conn.execute(
        "DELETE FROM replicas WHERE chunk_id IN (SELECT id FROM chunks WHERE file_id=?)",
        (file_id,),
    )
    conn.execute("DELETE FROM chunks WHERE file_id=?", (file_id,))
    conn.execute("DELETE FROM files WHERE id=?", (file_id,))
    conn.commit()
    conn.close()
    log(f"DELETE • {row['filename']}")
    return {"success": True}


@app.post("/api/repair/{file_id}")
def repair(file_id: str):
    row = get_file_row(file_id)
    if not row:
        raise HTTPException(404, "File not found")

    conn = db()
    chunks = conn.execute("SELECT * FROM chunks WHERE file_id=?", (file_id,)).fetchall()
    repaired = 0

    for chunk in chunks:
        healthy = conn.execute(
            "SELECT * FROM replicas WHERE chunk_id=? AND status='HEALTHY'",
            (chunk["id"],),
        ).fetchall()
        needed = REPLICATION_FACTOR - len(healthy)
        if needed <= 0:
            continue

        source = None
        for r in healthy:
            if Path(r["path"]).exists() and sha256(Path(r["path"]).read_bytes()) == chunk["checksum"]:
                source = r
                break
        if not source:
            continue

        candidates = [n for n in available_nodes() if n not in {r["node_id"] for r in healthy}]
        for dest in candidates[:needed]:
            src = Path(source["path"])
            dst = node_path(dest, file_id, chunk["chunk_index"])
            shutil.copy2(src, dst)
            conn.execute(
                "INSERT INTO replicas (chunk_id, node_id, path, checksum, status) VALUES (?, ?, ?, ?, 'HEALTHY')",
                (chunk["id"], dest, str(dst), chunk["checksum"]),
            )
            nodes[dest]["used_storage"] += chunk["chunk_size"]
            conn.execute(
                """INSERT INTO repairs
                   (chunk_id, source_node, destination_node, reason, status, started_at, completed_at)
                   VALUES (?, ?, ?, ?, 'COMPLETED', ?, ?)""",
                (chunk["id"], source["node_id"], dest, "Replica failure", datetime.utcnow().isoformat(), datetime.utcnow().isoformat()),
            )
            repaired += 1
            log(f"REPAIR • chunk {chunk['chunk_index']} • {source['node_id']} → {dest}")

    remaining = conn.execute(
        """SELECT COUNT(*) c FROM chunks c
           WHERE c.file_id=? AND
           (SELECT COUNT(*) FROM replicas r WHERE r.chunk_id=c.id AND r.status='HEALTHY') < ?""",
        (file_id, REPLICATION_FACTOR),
    ).fetchone()["c"]
    conn.execute(
        "UPDATE files SET status=? WHERE id=?",
        ("HEALTHY" if remaining == 0 else "DEGRADED", file_id),
    )
    conn.commit()
    conn.close()
    return {"success": True, "repaired_replicas": repaired, "status": "HEALTHY" if remaining == 0 else "DEGRADED"}


@app.get("/api/logs")
def get_logs():
    return logs


@app.get("/api/replication")
def replication():
    conn = db()
    rows = conn.execute("""
        SELECT f.id, f.filename, f.total_chunks, f.replication_factor, f.status
        FROM files f ORDER BY f.created_at DESC
    """).fetchall()
    result = []
    for f in rows:
        chunks = conn.execute("""
            SELECT c.chunk_index, r.node_id, r.status
            FROM chunks c JOIN replicas r ON r.chunk_id=c.id
            WHERE c.file_id=? ORDER BY c.chunk_index
        """, (f["id"],)).fetchall()
        result.append({**dict(f), "replicas": [dict(x) for x in chunks]})
    conn.close()
    return result


app.mount("/static", StaticFiles(directory=FRONTEND_DIR), name="static")


@app.get("/{full_path:path}")
def frontend(full_path: str):
    return FileResponse(FRONTEND_DIR / "index.html")
