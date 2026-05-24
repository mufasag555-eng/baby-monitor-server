/**
 * Baby Monitor — WebRTC Signaling Server
 * Run: node signaling-server.js
 * Requires: npm install ws
 */

const WebSocket = require('ws');
const http = require('http');

const PORT = process.env.PORT || 8080;

// Simple HTTP server (for health check / hosting viewer.html)
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

wss.on('connection', (ws, req) => {
  const url = new URL(req.url, `http://localhost:${PORT}`);
  const room = url.searchParams.get('room') || 'default';
  const role = url.searchParams.get('role') || 'viewer'; // 'broadcaster' or 'viewer'

  ws.room = room;
  ws.role = role;

  if (!rooms[room]) rooms[room] = { broadcaster: null, viewers: [] };

  if (role === 'broadcaster') {
    rooms[room].broadcaster = ws;
    log(`Broadcaster joined room: ${room}`);

    // Notify existing viewers
    rooms[room].viewers.forEach(v => {
      if (v.readyState === WebSocket.OPEN) {
        v.send(JSON.stringify({ type: 'broadcaster_joined' }));
      }
    });

  } else {
    rooms[room].viewers.push(ws);
    log(`Viewer joined room: ${room}`);

    // Notify broadcaster that viewer is ready
    const bc = rooms[room].broadcaster;
    if (bc && bc.readyState === WebSocket.OPEN) {
      bc.send(JSON.stringify({ type: 'viewer_joined' }));
    } else {
      ws.send(JSON.stringify({ type: 'waiting', message: 'Broadcaster not yet connected' }));
    }
  }

  ws.on('message', (data) => {
    let msg;
    try { msg = JSON.parse(data); } catch { return; }

    const r = rooms[room];
    if (!r) return;

    if (role === 'broadcaster') {
      // Forward offer and ICE candidates to all viewers
      if (msg.type === 'offer' || msg.type === 'ice_candidate') {
        r.viewers.forEach(v => {
          if (v.readyState === WebSocket.OPEN) {
            v.send(JSON.stringify(msg));
          }
        });
      }
      // "ready" — broadcast to viewers
      if (msg.type === 'ready') {
        r.viewers.forEach(v => {
          if (v.readyState === WebSocket.OPEN) {
            v.send(JSON.stringify({ type: 'broadcaster_joined' }));
          }
        });
      }

    } else {
      // Forward answer and ICE candidates from viewer to broadcaster
      if (msg.type === 'answer' || msg.type === 'ice_candidate' || msg.type === 'viewer_ready') {
        const bc = r.broadcaster;
        if (bc && bc.readyState === WebSocket.OPEN) {
          bc.send(JSON.stringify(msg));
        }
      }
    }
  });

  ws.on('close', () => {
    const r = rooms[room];
    if (!r) return;

    if (role === 'broadcaster') {
      r.broadcaster = null;
      log(`Broadcaster left room: ${room}`);
      // Notify viewers
      r.viewers.forEach(v => {
        if (v.readyState === WebSocket.OPEN) {
          v.send(JSON.stringify({ type: 'broadcaster_left' }));
        }
      });
    } else {
      r.viewers = r.viewers.filter(v => v !== ws);
      log(`Viewer left room: ${room}`);
      const bc = r.broadcaster;
      if (bc && bc.readyState === WebSocket.OPEN) {
        bc.send(JSON.stringify({ type: 'viewer_left' }));
      }
    }

    // Cleanup empty rooms
    if (!r.broadcaster && r.viewers.length === 0) {
      delete rooms[room];
      log(`Room deleted: ${room}`);
    }
  });

  ws.on('error', (err) => {
    log(`WebSocket error in room ${room}: ${err.message}`);
  });
});

server.listen(PORT, () => {
  log(`Signaling server started on port ${PORT}`);
  log(`WebSocket: ws://localhost:${PORT}`);
});
