/**
 * Baby Monitor — WebRTC Signaling Server (Fixed)
 * Run: node signaling-server.js
 * Requires: npm install ws
 */

const WebSocket = require('ws');
const http = require('http');
const PORT = process.env.PORT || 8080;

const server = http.createServer((req, res) => {
  res.writeHead(200, { 'Content-Type': 'text/plain' });
  res.end('Baby Monitor Signaling Server Running\n');
});

const wss = new WebSocket.Server({ server });

// rooms[roomCode] = { broadcaster: ws, viewers: [ws, ...] }
const rooms = {};

function log(msg) {
  console.log(`[${new Date().toISOString()}] ${msg}`);
}

function send(ws, obj) {
  if (ws && ws.readyState === WebSocket.OPEN) {
    ws.send(JSON.stringify(obj));
  }
}

wss.on('connection', (ws, req) => {
  const url   = new URL(req.url, `http://localhost:${PORT}`);
  const room  = url.searchParams.get('room') || 'default';
  const role  = url.searchParams.get('role') || 'viewer';

  ws.room = room;
  ws.role = role;

  if (!rooms[room]) rooms[room] = { broadcaster: null, viewers: [] };
  const r = rooms[room];

  if (role === 'broadcaster') {
    r.broadcaster = ws;
    log(`Broadcaster joined room: ${room}`);
    // Don't notify viewers yet — wait for broadcaster to send "ready"

  } else {
    r.viewers.push(ws);
    log(`Viewer joined room: ${room}`);
    // Don't notify broadcaster yet — wait for viewer to send "viewer_ready"
    // Just tell viewer whether broadcaster is present
    if (r.broadcaster) {
      send(ws, { type: 'broadcaster_present' });
    } else {
      send(ws, { type: 'waiting', message: 'Broadcaster not connected yet' });
    }
  }

  ws.on('message', (data) => {
    let msg;
    try { msg = JSON.parse(data); } catch { return; }

    log(`[${room}][${role}] ${msg.type}`);

    if (role === 'broadcaster') {
      // ── Broadcaster messages ──────────────────────────────
      switch (msg.type) {

        case 'ready':
          // Broadcaster is ready — tell any waiting viewers
          r.viewers.forEach(v => send(v, { type: 'broadcaster_joined' }));
          break;

        case 'offer':
        case 'ice_candidate':
          // Forward to all viewers
          r.viewers.forEach(v => send(v, msg));
          break;
      }

    } else {
      // ── Viewer messages ───────────────────────────────────
      switch (msg.type) {

        case 'viewer_ready':
          // Viewer is ready — NOW tell broadcaster to create offer
          if (r.broadcaster) {
            send(r.broadcaster, { type: 'viewer_joined' });
          } else {
            send(ws, { type: 'waiting', message: 'Broadcaster not connected' });
          }
          break;

        case 'answer':
        case 'ice_candidate':
          // Forward to broadcaster
          send(r.broadcaster, msg);
          break;
      }
    }
  });

  ws.on('close', () => {
    if (role === 'broadcaster') {
      r.broadcaster = null;
      log(`Broadcaster left: ${room}`);
      r.viewers.forEach(v => send(v, { type: 'broadcaster_left' }));
    } else {
      r.viewers = r.viewers.filter(v => v !== ws);
      log(`Viewer left: ${room}`);
      send(r.broadcaster, { type: 'viewer_left' });
    }
    if (!r.broadcaster && r.viewers.length === 0) {
      delete rooms[room];
      log(`Room deleted: ${room}`);
    }
  });

  ws.on('error', err => log(`WS error [${room}]: ${err.message}`));
});

server.listen(PORT, () => {
  log(`Signaling server on port ${PORT}`);
});
