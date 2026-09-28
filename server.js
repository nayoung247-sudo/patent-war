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
  sha: null, timer: null, dirty: false, busy: false, lastError: null, lastCommit: null,
  synced: false                                    // 원격 CSV 를 한 번이라도 제대로 받아 합쳤는가 — 아니면 절대 올리지 않는다
};
const ghOn = () => !!(GH.token && GH.repo);
const ghUrl = () => GH.api + '/repos/' + GH.repo + '/contents/' + GH.path.split('/').map(encodeURIComponent).join('/');
const ghHeaders = () => ({ Authorization: 'Bearer ' + GH.token, Accept: 'application/vnd.github+json',
                           'X-GitHub-Api-Version': '2022-11-28', 'User-Agent': 'patentwar-server' });

fs.mkdirSync(DATA_DIR, { recursive: true });
if (!fs.existsSync(CSV_PATH)) fs.writeFileSync(CSV_PATH, CSV_HEADER, 'utf8');

let writeQueue = Promise.resolve();
//  파일 작업을 한 줄로 세운다. 한 번 실패해도 대기열이 막히지 않게 실패는 대기열에서 삼킨다.
//  파일을 바꾸는 작업은 모두 이 대기열을 거친다 → 바꿀 때마다 읽기 캐시를 비운다
let rowsCache = null;
const queueWrite = fn => { const p = writeQueue.then(fn).finally(() => { rowsCache = null; }); writeQueue = p.catch(() => {}); return p; };
const appendRow = row => queueWrite(() => fs.promises.appendFile(CSV_PATH, row, 'utf8'));
const csvField = v => {
  const s = String(v ?? '');
  return /[",\n]/.test(s) ? '"' + s.replace(/"/g, '""') + '"' : s;
};
//  CSV 는 한 줄 = 한 기록이다. 이름 등에 줄바꿈 · 제어문자가 섞이면 줄이 쪼개져 기록이 깨지므로 저장 전에 걷어낸다.
const cleanText = (v, max) => String(v ?? '').replace(/[\u0000-\u001f\u007f]/g, '').slice(0, max);
function splitCsvLine(line) {
  const out = []; let cur = '', inQ = false;
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
  return out;
}
function parseCsv(text) {
  const lines = text.split('\n').filter(l => l.trim().length);
  const rows = [];
  for (let i = 1; i < lines.length; i++) {
    const out = splitCsvLine(lines[i].replace(/\r$/, ''));
    const [timestamp, mode, name, partner, character, score, kills, wave, dur] = out;
    const t = Date.parse(timestamp);
    if (!Number.isFinite(t)) continue;
    rows.push({ timestamp, t, mode: mode === 'coop' ? 'coop' : 'solo', name, partner, character,
                score: +score || 0, kills: +kills || 0, wave: +wave || 0, dur: +dur || 0 });
  }
  return rows;
}
//  요청마다 파일 전체를 다시 읽고 파싱하지 않도록 캐시한다 (파일이 바뀌면 queueWrite 가 비운다).
//  호출하는 쪽은 filter · slice 로만 쓰고 배열 자체를 바꾸지 않는다.
const readAll = () => rowsCache || (rowsCache = parseCsv(fs.readFileSync(CSV_PATH, 'utf8')));

//  CSV 두 벌(로컬 · 원격)을 합친다. 헤더는 새 형식으로, 같은 줄은 한 번만, 시간순으로.
//  한쪽에만 있는 기록도 절대 버리지 않는다 — 어느 쪽이 "최신"인지 가정하지 않는다.
function mergeCsv(localText, remoteText) {
  //  첫 줄은 헤더일 때만 뺀다 — 손으로 만들었거나 헤더가 지워진 파일이면 첫 줄도 기록이다
  const dataLines = t => {
    const lines = String(t || '').replace(/\r/g, '').split('\n');
    if (lines.length && /^﻿?timestamp,/.test(lines[0])) lines.shift();
    return lines.filter(l => l.trim().length);
  };
  const remote = dataLines(remoteText);
  const seen = new Set(), out = [];
  for (const l of remote.concat(dataLines(localText))) if (!seen.has(l)) { seen.add(l); out.push(l); }
  const ts = l => { const t = Date.parse(l.slice(0, l.indexOf(','))); return Number.isFinite(t) ? t : 0; };
  out.sort((a, b) => ts(a) - ts(b));
  return { text: CSV_HEADER + out.map(l => l + '\n').join(''), added: out.length - new Set(remote).size };
}

//  GitHub 의 CSV 를 받는다. 1MB 가 넘으면 contents API 가 content 를 비워서 주므로,
//  방금 받은 sha 로 blob 을 받는다 (100MB 까지). sha 로 받으므로 내용과 sha 가 반드시 같은 버전이다
//  — 다른 요청으로 내용을 따로 받으면 그 사이 파일이 바뀌어 sha 보다 옛 내용으로 덮어쓸 수 있다.
async function ghFetch() {
  const url = ghUrl() + '?ref=' + encodeURIComponent(GH.branch);
  const r = await fetch(url, { headers: ghHeaders() });
  if (r.status === 404) return { sha: null, text: '' };
  if (!r.ok) throw new Error('GitHub GET ' + r.status + ' ' + (await r.text()).slice(0, 200));
  const j = await r.json();
  let text = '';
  if (j.encoding === 'base64' && j.content) {
    text = Buffer.from(String(j.content).replace(/\n/g, ''), 'base64').toString('utf8');
  } else if (j.size > 0) {
    const b = await fetch(GH.api + '/repos/' + GH.repo + '/git/blobs/' + encodeURIComponent(j.sha), { headers: ghHeaders() });
    if (!b.ok) throw new Error('GitHub GET(blob) ' + b.status + ' ' + (await b.text()).slice(0, 200));
    const bj = await b.json();
    if (bj.encoding !== 'base64') throw new Error('GitHub blob 인코딩을 알 수 없음: ' + bj.encoding);
    text = Buffer.from(String(bj.content || '').replace(/\n/g, ''), 'base64').toString('utf8');
  }
  //  크기가 있는데 내용이 비었다면 받기에 실패한 것 — 빈 파일로 여기고 덮어쓰면 기록이 모두 사라진다
  if (j.size > 0 && !text.length) throw new Error('GitHub 에서 받은 내용이 비어 있음 (size ' + j.size + ')');
  return { sha: j.sha, text };
}

//  GitHub 의 최신 CSV 를 받아 로컬과 합친다 (부팅 시, 그리고 올리기 전 sha 가 어긋났을 때).
//  반환값: 로컬에만 있던 기록이 있어 올려야 하면 true
async function ghPull() {
  if (!ghOn()) return false;
  const remote = await ghFetch();
  let added = 0;
  await queueWrite(() => {
    const m = mergeCsv(fs.readFileSync(CSV_PATH, 'utf8'), remote.text);
    fs.writeFileSync(CSV_PATH, m.text, 'utf8');
    added = m.added;
  });
  GH.sha = remote.sha; GH.synced = true;
  if (!remote.sha) console.log('[랭킹] 저장소에 ' + GH.path + ' 이 없어 새로 만듭니다.');
  else console.log('[랭킹] GitHub 에서 불러와 합침 — 총 ' + readAll().length + '건 (' + GH.repo + '/' + GH.path + ')');
  return added > 0;
}
//  로컬 CSV 전체를 한 커밋으로 올린다.
//  · 원격을 한 번도 제대로 받지 못했으면 먼저 받아 합친다 (못 받으면 올리지 않는다 → 원격 기록 보호)
//  · sha 가 어긋나면(다른 곳에서 수정) 원격 내용을 받아 합친 뒤 한 번 재시도한다
async function ghPush() {
  if (!ghOn() || GH.busy) return;
  GH.busy = true; GH.dirty = false;
  try {
    if (!GH.synced) await ghPull();
    for (let attempt = 0; ; attempt++) {
      const text = await queueWrite(() => fs.readFileSync(CSV_PATH, 'utf8'));
      const rows = parseCsv(text);
      const last = rows[rows.length - 1];
      const msg = last ? '랭킹: ' + (last.mode === 'coop' ? last.name + ' & ' + last.partner : last.name) + ' ' + last.score.toLocaleString() + '점 (총 ' + rows.length + '건)'
                       : '랭킹 갱신';
      //  게임 코드와 같은 저장소에 두면 커밋마다 Render 가 다시 배포하려 든다 → [skip render] 로 막는다
      const body = { message: msg + ' [skip render]', content: Buffer.from(text, 'utf8').toString('base64'), branch: GH.branch };
      if (GH.sha) body.sha = GH.sha;
      const r = await fetch(ghUrl(), { method: 'PUT', headers: { ...ghHeaders(), 'Content-Type': 'application/json' }, body: JSON.stringify(body) });
      if ((r.status === 409 || r.status === 422) && attempt === 0) {
        await ghPull();                          // 원격 내용까지 받아 합친 뒤 그 sha 로 다시 올린다
        continue;
      }
      if (!r.ok) throw new Error('GitHub PUT ' + r.status + ' ' + (await r.text()).slice(0, 200));
      const j = await r.json();
      GH.sha = j.content && j.content.sha; GH.lastCommit = new Date().toISOString(); GH.lastError = null;
      console.log('[랭킹] 커밋 완료 — ' + msg);
      break;
    }
  } catch (e) {
    GH.lastError = String(e.message || e); GH.dirty = true;
    console.error('[랭킹] 커밋 실패:', GH.lastError);
    clearTimeout(GH.timer); GH.timer = setTimeout(() => { GH.timer = null; ghPush(); }, 60000);   // 1분 뒤 다시
  } finally { GH.busy = false; if (GH.dirty && !GH.timer) ghSchedule(); }
}
function ghSchedule() {
  if (!ghOn()) return;
  GH.dirty = true;
  clearTimeout(GH.timer);
  GH.timer = setTimeout(() => { GH.timer = null; ghPush(); }, GH.debounce);
}
//  부팅 시 불러오기. 실패하면 1분마다 다시 시도한다 (그동안 들어온 기록은 로컬에 쌓였다가 합쳐진다).
function ghBoot() {
  if (!ghOn() || GH.synced) return;
  ghPull().then(needPush => { if (needPush) ghSchedule(); })
    .catch(e => { GH.lastError = String(e.message || e); console.error('[랭킹] GitHub 불러오기 실패 (1분 뒤 재시도):', GH.lastError); setTimeout(ghBoot, 60000); });
}
ghBoot();
//  종료 신호를 받으면 밀린 기록을 바로 올리고 나간다 (Render 재배포 · 잠들기)
for (const sig of ['SIGTERM', 'SIGINT']) process.on(sig, async () => {
  clearTimeout(GH.timer);
  //  이미 올리는 중이면 끝날 때까지 기다린 뒤, 그 사이 밀린 기록이 있으면 한 번 더 올린다 (최대 10초)
  for (let i = 0; i < 100 && GH.busy; i++) await new Promise(r => setTimeout(r, 100));
  clearTimeout(GH.timer);
  if (GH.dirty && !GH.busy) { try { await ghPush(); } catch (e) {} }
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
  const int = v => { const n = Math.round(+v); return Number.isFinite(n) ? Math.max(0, n) : 0; };   // 숫자가 아니면 NaN 대신 0
  const row = [
    now.toISOString(), m,
    csvField(cleanText(name, 8) || '요원'),
    csvField(cleanText(partner, 8)),
    csvField(cleanText(character, 20)),
    int(score),
    int(kills),
    int(wave),
    Math.min(3600, int(dur))
  ].join(',') + '\n';
  try { await appendRow(row); }
  catch (e) { console.error('[랭킹] 기록 저장 실패:', e.message || e); return res.status(500).json({ error: '기록 저장 실패' }); }
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
                                                       rows: readAll().length, synced: GH.synced, pending: GH.dirty, lastCommit: GH.lastCommit, lastError: GH.lastError }));

app.get('/api/ranks.csv', (req, res) => {
  res.setHeader('Content-Type', 'text/csv; charset=utf-8');
  res.setHeader('Content-Disposition', 'attachment; filename="scores.csv"');
  //  엑셀이 = + - @ 로 시작하는 칸을 수식으로 실행하지 않도록 앞에 ' 를 붙여 내보낸다 (CSV 인젝션 방지).
  //  저장된 원본과 랭킹 화면의 이름은 그대로 둔다 — 내려받는 파일에만 적용.
  const lines = fs.readFileSync(CSV_PATH, 'utf8').split('\n');
  const safe = lines.map((l, i) => (i === 0 || !l.trim()) ? l
    : splitCsvLine(l.replace(/\r$/, '')).map(f => csvField(/^[=+\-@\t\r]/.test(f) ? "'" + f : f)).join(','));
  res.send('﻿' + safe.join('\n'));   // 엑셀용 BOM
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

//  메시지 크기 상한 — 기본값(100MB)이면 거대한 메시지 하나로 서버 메모리를 채울 수 있다.
//  가장 큰 정상 메시지(방장의 월드 상태)도 수십 KB 수준이라 256KB 면 넉넉하다.
const wss = new WebSocketServer({ server, path: '/ws', maxPayload: 256 * 1024 });

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
