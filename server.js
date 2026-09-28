// ═══════════════════════════════════════════════════════════════
//  특허 전쟁 — 게임 서빙 + 랭킹(CSV) + 2인 협동 릴레이 + 자유 공원(N명)
//   · 게임 로직은 전부 브라우저에서 돈다. 서버는 방을 관리하고 메시지를 전달만 한다.
//   · 몬스터 AI/스폰/웨이브는 방장(호스트) 브라우저가 계산한다.
//   · 기록은 CSV 한 장. GitHub 저장소에 커밋해 두고, 켜질 때 다시 내려받는다.
// ═══════════════════════════════════════════════════════════════
const express = require('express');
const http = require('http');
const path = require('path');
const fs = require('fs');
const { WebSocketServer } = require('ws');

const app = express();
const server = http.createServer(app);
const PORT = process.env.PORT || 3000;

// ───────────────────────── 랭킹 (CSV → GitHub 커밋) ─────────────────────────
//  · 기록은 CSV 한 장. 서버가 켜질 때 GitHub 저장소에서 내려받고, 새 기록이 들어오면
//    잠시 모았다가(GH_DEBOUNCE_MS) 파일 전체를 한 번의 커밋으로 올린다.
//  · 무료 인스턴스는 디스크가 날아가므로 GitHub 이 원본, 로컬 파일은 캐시다.
//  · GH_TOKEN 이 없으면 로컬 CSV 만 쓴다(재시작하면 사라질 수 있음).
const DATA_DIR = path.join(__dirname, 'data');
const CSV_PATH = path.join(DATA_DIR, 'scores.csv');
const CSV_HEADER = 'timestamp,mode,name,partner,character,score,kills,wave,dur\n';
const GH = {
  token: process.env.GH_TOKEN || '',
  repo: process.env.GH_REPO || '',                 // "owner/repo"
  path: process.env.GH_PATH || 'data/scores.csv',
  branch: process.env.GH_BRANCH || 'main',
  api: (process.env.GH_API || 'https://api.github.com').replace(/\/$/, ''),
  debounce: +process.env.GH_DEBOUNCE_MS || 15000,
  sha: null, timer: null, dirty: false, busy: false, lastError: null, lastCommit: null
};
const ghOn = () => !!(GH.token && GH.repo);
const ghUrl = () => GH.api + '/repos/' + GH.repo + '/contents/' + GH.path.split('/').map(encodeURIComponent).join('/');
const ghHeaders = () => ({ Authorization: 'Bearer ' + GH.token, Accept: 'application/vnd.github+json',
                           'X-GitHub-Api-Version': '2022-11-28', 'User-Agent': 'patentwar-server' });

fs.mkdirSync(DATA_DIR, { recursive: true });
if (!fs.existsSync(CSV_PATH)) fs.writeFileSync(CSV_PATH, CSV_HEADER, 'utf8');

let writeQueue = Promise.resolve();
const appendRow = row => (writeQueue = writeQueue.then(() => fs.promises.appendFile(CSV_PATH, row, 'utf8')));
const csvField = v => {
  const s = String(v ?? '');
  return /[",\n]/.test(s) ? '"' + s.replace(/"/g, '""') + '"' : s;
};
function parseCsv(text) {
  const lines = text.split('\n').filter(l => l.trim().length);
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
    const [timestamp, mode, name, partner, character, score, kills, wave, dur] = out;
    const t = Date.parse(timestamp);
    if (!Number.isFinite(t)) continue;
    rows.push({ timestamp, t, mode: mode === 'coop' ? 'coop' : 'solo', name, partner, character,
                score: +score || 0, kills: +kills || 0, wave: +wave || 0, dur: +dur || 0 });
  }
  return rows;
}
const readAll = () => parseCsv(fs.readFileSync(CSV_PATH, 'utf8'));

//  GitHub 에서 최신 CSV 를 내려받아 로컬 캐시로 쓴다 (부팅 시 한 번)
async function ghPull() {
  if (!ghOn()) return;
  const r = await fetch(ghUrl() + '?ref=' + encodeURIComponent(GH.branch), { headers: ghHeaders() });
  if (r.status === 404) { GH.sha = null; console.log('[랭킹] 저장소에 ' + GH.path + ' 이 없어 새로 만듭니다.'); return; }
  if (!r.ok) throw new Error('GitHub GET ' + r.status + ' ' + (await r.text()).slice(0, 200));
  const j = await r.json();
  GH.sha = j.sha;
  const text = Buffer.from(String(j.content || '').replace(/\n/g, ''), 'base64').toString('utf8');
  //  헤더가 옛 형식(dur 없음)이어도 parseCsv 가 그대로 읽는다. 저장할 때 새 헤더로 바뀐다.
  const body = text.split('\n').slice(1).join('\n');
  fs.writeFileSync(CSV_PATH, CSV_HEADER + body.replace(/^\n+/, ''), 'utf8');
  console.log('[랭킹] GitHub 에서 ' + readAll().length + '건 불러옴 (' + GH.repo + '/' + GH.path + ')');
}
//  로컬 CSV 전체를 한 커밋으로 올린다. sha 가 어긋나면(다른 곳에서 수정) 한 번 다시 받아 재시도.
async function ghPush(retry = true) {
  if (!ghOn() || GH.busy) return;
  GH.busy = true; GH.dirty = false;
  try {
    await writeQueue;
    const text = fs.readFileSync(CSV_PATH, 'utf8');
    const rows = readAll();
    const last = rows[rows.length - 1];
    const msg = last ? '랭킹: ' + (last.mode === 'coop' ? last.name + ' & ' + last.partner : last.name) + ' ' + last.score.toLocaleString() + '점 (총 ' + rows.length + '건)'
                     : '랭킹 갱신';
    //  게임 코드와 같은 저장소에 두면 커밋마다 Render 가 다시 배포하려 든다 → [skip render] 로 막는다
    const body = { message: msg + ' [skip render]', content: Buffer.from(text, 'utf8').toString('base64'), branch: GH.branch };
    if (GH.sha) body.sha = GH.sha;
    const r = await fetch(ghUrl(), { method: 'PUT', headers: { ...ghHeaders(), 'Content-Type': 'application/json' }, body: JSON.stringify(body) });
    if ((r.status === 409 || r.status === 422) && retry) {
      //  sha 불일치 — 원격이 바뀌었다. 원격 sha 만 다시 받고(내용은 로컬이 최신) 재시도
      const g = await fetch(ghUrl() + '?ref=' + encodeURIComponent(GH.branch), { headers: ghHeaders() });
      GH.sha = g.ok ? (await g.json()).sha : null;
      GH.busy = false;
      return ghPush(false);
    }
    if (!r.ok) throw new Error('GitHub PUT ' + r.status + ' ' + (await r.text()).slice(0, 200));
    const j = await r.json();
    GH.sha = j.content && j.content.sha; GH.lastCommit = new Date().toISOString(); GH.lastError = null;
    console.log('[랭킹] 커밋 완료 — ' + msg);
  } catch (e) {
    GH.lastError = String(e.message || e); GH.dirty = true;
    console.error('[랭킹] 커밋 실패:', GH.lastError);
    clearTimeout(GH.timer); GH.timer = setTimeout(ghPush, 60000);   // 1분 뒤 다시
  } finally { GH.busy = false; if (GH.dirty && !GH.timer) ghSchedule(); }
}
function ghSchedule() {
  if (!ghOn()) return;
  GH.dirty = true;
  clearTimeout(GH.timer);
  GH.timer = setTimeout(() => { GH.timer = null; ghPush(); }, GH.debounce);
}
ghPull().catch(e => { GH.lastError = String(e.message || e); console.error('[랭킹] GitHub 불러오기 실패:', GH.lastError); });
//  종료 신호를 받으면 밀린 기록을 바로 올리고 나간다 (Render 재배포 · 잠들기)
for (const sig of ['SIGTERM', 'SIGINT']) process.on(sig, async () => {
  clearTimeout(GH.timer);
  if (GH.dirty) { try { await ghPush(); } catch (e) {} }
  process.exit(0);
});

//  일간 · 주간 · 월간 경계는 한국 시간(KST, UTC+9) 기준. 주는 월요일 시작.
const KST = 9 * 3600 * 1000;
function rangeStart(range) {
  const now = Date.now();
  const k = new Date(now + KST);           // KST 벽시계를 UTC 필드로 읽는다
  let start;
  if (range === 'day') start = Date.UTC(k.getUTCFullYear(), k.getUTCMonth(), k.getUTCDate());
  else if (range === 'week') { const dow = (k.getUTCDay() + 6) % 7; start = Date.UTC(k.getUTCFullYear(), k.getUTCMonth(), k.getUTCDate() - dow); }
  else if (range === 'month') start = Date.UTC(k.getUTCFullYear(), k.getUTCMonth(), 1);
  else return -Infinity;
  return start - KST;
}
const byScore = (a, b) => b.score - a.score || a.t - b.t;
const rankOf = (rows, score, t) => rows.filter(r => r.score > score || (r.score === score && r.t < t)).length + 1;

app.use(express.json());

app.post('/api/scores', async (req, res) => {
  const { mode, name, partner, character, score, kills, wave, dur } = req.body || {};
  if (typeof score !== 'number' || !Number.isFinite(score)) return res.status(400).json({ error: 'score 필요' });
  const m = mode === 'coop' ? 'coop' : 'solo';
  const now = new Date();
  const row = [
    now.toISOString(), m,
    csvField(String(name || '요원').slice(0, 8)),
    csvField(String(partner || '').slice(0, 8)),
    csvField(String(character || '').slice(0, 20)),
    Math.max(0, Math.round(score)),
    Math.max(0, Math.round(kills || 0)),
    Math.max(0, Math.round(wave || 0)),
    Math.max(0, Math.min(3600, Math.round(dur || 0)))
  ].join(',') + '\n';
  await appendRow(row);
  ghSchedule();
  const mine = readAll().filter(r => r.mode === m);              // 순위는 같은 모드 안에서
  res.json({ ok: true, mode: m, rank: rankOf(mine, Math.round(score), now.getTime()), total: mine.length });
});

//  GET /api/ranks?mode=solo|coop|all&range=day|week|month|all&limit=50
//    mode 를 주면 그 모드만, 없으면 합산. range 는 KST 달력 기준으로 자른다.
app.get('/api/ranks', (req, res) => {
  const mode = ['solo', 'coop'].includes(req.query.mode) ? req.query.mode : 'all';
  const range = ['day', 'week', 'month'].includes(req.query.range) ? req.query.range : 'all';
  const limit = Math.min(100, Math.max(1, +req.query.limit || 50));
  const since = rangeStart(range);
  const all = readAll().filter(r => r.t >= since && (mode === 'all' || r.mode === mode)).sort(byScore);
  res.json({
    mode, range, total: all.length,
    ranks: all.slice(0, limit).map((r, i) => ({
      rank: i + 1, mode: r.mode, name: r.name, partner: r.partner, character: r.character,
      score: r.score, kills: r.kills, wave: r.wave, dur: r.dur, ts: r.timestamp
    }))
  });
});

app.get('/api/ping', (req, res) => res.json({ ok: true, park: park.size }));   // 무료 인스턴스 잠들기 방지용
app.get('/api/ranks/status', (req, res) => res.json({ github: ghOn(), repo: ghOn() ? GH.repo + '/' + GH.path + '@' + GH.branch : null,
                                                       rows: readAll().length, pending: GH.dirty, lastCommit: GH.lastCommit, lastError: GH.lastError }));

app.get('/api/ranks.csv', (req, res) => {
  res.setHeader('Content-Type', 'text/csv; charset=utf-8');
  res.setHeader('Content-Disposition', 'attachment; filename="scores.csv"');
  res.send('﻿' + fs.readFileSync(CSV_PATH, 'utf8'));   // 엑셀용 BOM
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

// ───────────────────── 자유 공원 (N명, 방장 없음) ─────────────────────
//  · 피해도 몬스터도 없으므로 권위가 필요 없다. 각자 자기 위치를 보내고,
//    서버가 초당 PARK_HZ 회 모두의 위치를 한 묶음으로 돌려준다.
//  · 한 명당 (N-1)번 전달하는 대신 tick 마다 N번만 보낸다 → 20명이어도 초당 200건.
const PARK_MAX = 20, PARK_HZ = 10, CHAT_MAX = 60, CHAT_GAP_MS = 500;
const park = new Map();            // ws → { id, name, char, x, y, z, ry, sw }
let parkSeq = 1;
const parkSend = (obj, except) => { const s = JSON.stringify(obj); for (const ws of park.keys()) if (ws !== except && ws.readyState === 1) ws.send(s); };
function parkLeave(ws) {
  const p = park.get(ws); if (!p) return;
  park.delete(ws);
  parkSend({ t: 'pl', id: p.id, name: p.name });
}
setInterval(() => {
  if (!park.size) return;
  const arr = [];
  for (const p of park.values()) arr.push([p.id, p.name, p.char, p.x, p.y, p.z, p.ry, p.sw]);
  const s = JSON.stringify({ t: 'pks', p: arr });
  for (const ws of park.keys()) if (ws.readyState === 1) ws.send(s);
  for (const p of park.values()) p.sw = 0;         // 손짓 표시는 한 묶음에만 실린다
}, 1000 / PARK_HZ);

const wss = new WebSocketServer({ server, path: '/ws' });

wss.on('connection', ws => {
  ws.isAlive = true;
  ws.on('pong', () => { ws.isAlive = true; });

  ws.on('message', raw => {
    let m; try { m = JSON.parse(raw); } catch (e) { return; }

    if (m.t === 'hello') {
      ws.pname = String(m.name || '요원').slice(0, 8);
      ws.pchar = String(m.char || '').slice(0, 20);
      return;
    }
    if (m.t === 'lobby') { ws.watchingLobby = true; send(ws, { t: 'rooms', rooms: roomList() }); return; }
    if (m.t === 'unlobby') { ws.watchingLobby = false; return; }

    if (m.t === 'park') {
      leaveRoom(ws); parkLeave(ws);
      if (park.size >= PARK_MAX) return send(ws, { t: 'parkfull', max: PARK_MAX });
      const p = { id: parkSeq++, name: String(m.name || ws.pname || '요원').slice(0, 8), char: String(m.char || ws.pchar || '').slice(0, 20),
                  x: 0, y: 0, z: 0, ry: 0, sw: 0, lastChat: 0 };
      park.set(ws, p);
      send(ws, { t: 'parkok', id: p.id, n: park.size, max: PARK_MAX });
      parkSend({ t: 'pj', id: p.id, name: p.name }, ws);
      return;
    }
    if (m.t === 'unpark') { parkLeave(ws); return; }
    if (m.t === 'pk') {                       // 내 위치 [x, y, z, ry, swing]
      const p = park.get(ws); if (!p || !Array.isArray(m.d)) return;
      const n = v => (typeof v === 'number' && Number.isFinite(v)) ? v : 0;
      p.x = n(m.d[0]); p.y = n(m.d[1]); p.z = n(m.d[2]); p.ry = n(m.d[3]); if (m.d[4]) p.sw = 1;
      return;
    }
    if (m.t === 'pc') {                       // 채팅 — 길이 · 속도 제한, 제어문자 제거
      const p = park.get(ws); if (!p) return;
      const now = Date.now(); if (now - p.lastChat < CHAT_GAP_MS) return; p.lastChat = now;
      const text = String(m.m || '').replace(/[\u0000-\u001f\u007f]/g, '').trim().slice(0, CHAT_MAX);
      if (!text) return;
      parkSend({ t: 'pc', id: p.id, name: p.name, m: text });
      return;
    }

    if (m.t === 'join') {
      parkLeave(ws);
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

  ws.on('close', () => { leaveRoom(ws); parkLeave(ws); });
  ws.on('error', () => { leaveRoom(ws); parkLeave(ws); });
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
