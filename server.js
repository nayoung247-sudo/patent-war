// ═══════════════════════════════════════════════════════════════
//  특허 전쟁 — 게임 서빙 + 랭킹(CSV) + 2인 협동 릴레이
//   · 게임 로직은 전부 브라우저에서 돈다. 서버는 방을 관리하고 메시지를 전달만 한다.
//   · 몬스터 AI/스폰/웨이브는 방장(호스트) 브라우저가 계산한다.
//   · 기록은 CSV 한 장. 서버를 다시 켜면 초기화된다(의도된 동작).
// ═══════════════════════════════════════════════════════════════
const express = require('express');
const http = require('http');
const path = require('path');
const fs = require('fs');
const { WebSocketServer } = require('ws');

const app = express();
const server = http.createServer(app);
const PORT = process.env.PORT || 3000;

// ───────────────────────── 랭킹 (CSV) ─────────────────────────
const DATA_DIR = path.join(__dirname, 'data');
const CSV_PATH = path.join(DATA_DIR, 'scores.csv');
const CSV_HEADER = 'timestamp,mode,name,partner,character,score,kills,wave\n';
fs.mkdirSync(DATA_DIR, { recursive: true });
fs.writeFileSync(CSV_PATH, CSV_HEADER, 'utf8');

let writeQueue = Promise.resolve();
const appendRow = row => (writeQueue = writeQueue.then(() => fs.promises.appendFile(CSV_PATH, row, 'utf8')));
const csvField = v => {
  const s = String(v ?? '');
  return /[",\n]/.test(s) ? '"' + s.replace(/"/g, '""') + '"' : s;
};
function parseCsv(text) {
  const lines = text.split('\n').filter(l => l.length);
  const rows = [];
  for (let i = 1; i < lines.length; i++) {
    const line = lines[i]; const out = []; let cur = '', inQ = false;
    for (let c = 0; c < line.length; c++) {
      const ch = line[c];
      if (inQ) {
        if (ch === '"' && line[c + 1] === '"') { cur += '"'; c++; }
        else if (ch === '"') inQ = false; else cur += ch;
      } else {
        if (ch === '"') inQ = true;
        else if (ch === ',') { out.push(cur); cur = ''; }
        else cur += ch;
      }
    }
    out.push(cur);
    const [timestamp, mode, name, partner, character, score, kills, wave] = out;
    rows.push({ timestamp, mode: mode || 'solo', name, partner, character,
                score: +score || 0, kills: +kills || 0, wave: +wave || 0 });
  }
  return rows;
}
const readAll = () => parseCsv(fs.readFileSync(CSV_PATH, 'utf8'));

app.use(express.json());

app.post('/api/scores', async (req, res) => {
  const { mode, name, partner, character, score, kills, wave } = req.body || {};
  if (typeof score !== 'number' || !Number.isFinite(score)) return res.status(400).json({ error: 'score 필요' });
  const m = mode === 'coop' ? 'coop' : 'solo';
  const row = [
    new Date().toISOString(), m,
    csvField(String(name || '요원').slice(0, 10)),
    csvField(String(partner || '').slice(0, 10)),
    csvField(String(character || '').slice(0, 20)),
    Math.max(0, Math.round(score)),
    Math.max(0, Math.round(kills || 0)),
    Math.max(0, Math.round(wave || 0))
  ].join(',') + '\n';
  await appendRow(row);
  const same = readAll().filter(r => r.mode === m);
  res.json({ ok: true, rank: same.filter(r => r.score > score).length + 1, total: same.length });
});

app.get('/api/ranks', (req, res) => {
  const m = req.query.mode === 'coop' ? 'coop' : 'solo';
  const limit = Math.min(50, Math.max(1, +req.query.limit || 10));
  const all = readAll().filter(r => r.mode === m)
    .sort((a, b) => b.score - a.score || new Date(a.timestamp) - new Date(b.timestamp));
  res.json({
    total: all.length,
    ranks: all.slice(0, limit).map((r, i) => ({
      rank: i + 1, name: r.name, partner: r.partner, character: r.character,
      score: r.score, kills: r.kills, wave: r.wave, date: r.timestamp.slice(0, 10)
    }))
  });
});

app.get('/api/ranks.csv', (req, res) => {
  res.setHeader('Content-Type', 'text/csv; charset=utf-8');
  res.setHeader('Content-Disposition', 'attachment; filename="scores.csv"');
  res.send('\uFEFF' + fs.readFileSync(CSV_PATH, 'utf8'));   // 엑셀용 BOM
});

app.use(express.static(path.join(__dirname, 'public')));

// ───────────────────── 협동 플레이 릴레이 ─────────────────────
const ROOM_COUNT = 10;
// rooms[1..10] = { host: ws|null, guest: ws|null, playing: bool }
const rooms = new Map();
for (let i = 1; i <= ROOM_COUNT; i++) rooms.set(i, { host: null, guest: null, playing: false });

const send = (ws, obj) => { if (ws && ws.readyState === 1) ws.send(JSON.stringify(obj)); };
const peerOf = (room, ws) => (room.host === ws ? room.guest : room.host);

function roomList() {
  const out = [];
  for (let i = 1; i <= ROOM_COUNT; i++) {
    const r = rooms.get(i);
    const names = [];
    if (r.host) names.push(r.host.pname || '요원');
    if (r.guest) names.push(r.guest.pname || '요원');
    out.push({
      id: i,
      state: r.playing ? 'playing' : (r.host && r.guest) ? 'full' : r.host ? 'waiting' : 'empty',
      names
    });
  }
  return out;
}
function broadcastRooms() {
  const msg = JSON.stringify({ t: 'rooms', rooms: roomList() });
  wss.clients.forEach(c => { if (c.readyState === 1 && c.watchingLobby) c.send(msg); });
}

function leaveRoom(ws) {
  if (!ws.roomId) return;
  const r = rooms.get(ws.roomId);
  if (!r) { ws.roomId = null; return; }
  const other = peerOf(r, ws);
  const wasHost = r.host === ws;
  if (wasHost) { r.host = r.guest; r.guest = null; } // 게스트가 남으면 그가 방장이 된다
  else if (r.guest === ws) r.guest = null;
  if (!r.host) { r.guest = null; r.playing = false; }
  ws.roomId = null;
  if (other) {
    send(other, { t: 'peerleft', nowHost: r.host === other });
    if (r.host === other) other.isHost = true;
  }
  if (!r.host) r.playing = false;
  broadcastRooms();
}

const wss = new WebSocketServer({ server, path: '/ws' });

wss.on('connection', ws => {
  ws.isAlive = true;
  ws.on('pong', () => { ws.isAlive = true; });

  ws.on('message', raw => {
    let m; try { m = JSON.parse(raw); } catch (e) { return; }

    if (m.t === 'hello') {
      ws.pname = String(m.name || '요원').slice(0, 10);
      ws.pchar = String(m.char || '').slice(0, 20);
      return;
    }
    if (m.t === 'lobby') { ws.watchingLobby = true; send(ws, { t: 'rooms', rooms: roomList() }); return; }
    if (m.t === 'unlobby') { ws.watchingLobby = false; return; }

    if (m.t === 'join') {
      const id = +m.room;
      const r = rooms.get(id);
      if (!r) return send(ws, { t: 'joinfail', reason: '없는 방입니다.' });
      if (r.playing) return send(ws, { t: 'joinfail', reason: '이미 플레이 중인 방입니다.' });
      if (r.host && r.guest) return send(ws, { t: 'joinfail', reason: '정원이 찼습니다.' });
      leaveRoom(ws);
      ws.roomId = id;
      if (!r.host) { r.host = ws; ws.isHost = true; }
      else { r.guest = ws; ws.isHost = false; }
      const other = peerOf(r, ws);
      send(ws, {
        t: 'joined', room: id, isHost: ws.isHost,
        peer: other ? { name: other.pname, char: other.pchar } : null
      });
      if (other) send(other, { t: 'peer', name: ws.pname, char: ws.pchar });
      broadcastRooms();
      return;
    }

    if (m.t === 'leave') { leaveRoom(ws); return; }

    if (m.t === 'start') {
      const r = rooms.get(ws.roomId);
      if (!r || r.host !== ws || !r.guest) return;
      r.playing = true;
      const payload = { t: 'start', seed: (Math.random() * 1e9) | 0 };
      send(r.host, payload); send(r.guest, payload);
      broadcastRooms();
      return;
    }

    if (m.t === 'over') {
      const r = rooms.get(ws.roomId);
      if (r) { r.playing = false; broadcastRooms(); }
      return;
    }

    if (m.t === 'r') {                      // 상대에게 그대로 전달 (게임 상태 동기화)
      const r = rooms.get(ws.roomId);
      if (!r) return;
      const other = peerOf(r, ws);
      if (other && other.readyState === 1) other.send(JSON.stringify({ t: 'r', d: m.d }));
      return;
    }
  });

  ws.on('close', () => leaveRoom(ws));
  ws.on('error', () => leaveRoom(ws));
});

// 끊어진 연결 정리
setInterval(() => {
  wss.clients.forEach(ws => {
    if (ws.isAlive === false) return ws.terminate();
    ws.isAlive = false;
    try { ws.ping(); } catch (e) {}
  });
}, 30000);

server.listen(PORT, () => console.log('특허 전쟁 서버 실행 중 — 포트 ' + PORT));
