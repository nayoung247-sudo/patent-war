// The Patent War - 배포용 백엔드 서버
// public/index.html(게임)을 정적으로 서빙하고,
// 게임 내 랭킹 화면이 호출하는 두 개의 API를 제공합니다.
//   POST /api/scores      점수 등록
//   GET  /api/ranks       상위 랭킹 조회

const express = require('express');
const path = require('path');

const app = express();
const PORT = process.env.PORT || 3000; // Render가 지정하는 포트를 반드시 사용해야 합니다.

app.use(express.json());
app.use(express.static(path.join(__dirname, 'public')));

// ── 랭킹 저장소 ─────────────────────────────────────────────
// 이번 배포는 공모전 시연(1회성) 목적이라 별도 DB 없이 메모리에 저장합니다.
// 단, Render 무료 플랜은 15분간 요청이 없으면 인스턴스가 슬립되고,
// 슬립 후 다시 깨어날 때(혹은 재배포 시) 메모리가 초기화되어 랭킹이 리셋됩니다.
// 시연 도중에는 계속 트래픽이 있으므로 문제없지만, 행사 전날 미리 서버를
// 깨워두고 그 상태를 유지하는 것을 권장합니다.
let scores = [];

app.post('/api/scores', (req, res) => {
  const { name, char, score, wave, kills, bosses, combo } = req.body || {};

  if (!name || typeof score !== 'number' || Number.isNaN(score)) {
    return res.status(400).json({ ok: false, error: 'invalid payload' });
  }

  const entry = {
    name: String(name).slice(0, 8),
    char: char ? String(char).slice(0, 20) : '',
    score: Math.max(0, Math.floor(score)),
    wave: Number(wave) || 1,
    kills: Number(kills) || 0,
    bosses: Number(bosses) || 0,
    combo: Number(combo) || 0,
    ts: Date.now()
  };

  scores.push(entry);
  scores.sort((a, b) => b.score - a.score);

  const rank = scores.indexOf(entry) + 1;
  res.json({ ok: true, rank, total: scores.length });
});

app.get('/api/ranks', (req, res) => {
  const limit = Math.min(parseInt(req.query.limit, 10) || 20, 100);
  res.json({ ranks: scores.slice(0, limit) });
});

// 헬스체크 겸 슬립 방지용 핑 엔드포인트 (선택 사용)
app.get('/healthz', (req, res) => res.json({ ok: true, uptime: process.uptime() }));

// 그 외 모든 경로는 게임 화면(index.html)으로 폴백
app.get('*', (req, res) => {
  res.sendFile(path.join(__dirname, 'public', 'index.html'));
});

app.listen(PORT, () => {
  console.log(`The Patent War server running on port ${PORT}`);
});
