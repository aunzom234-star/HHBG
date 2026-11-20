// SecretVoice relay v2.2
// - one "lobby" per Minecraft server (the mod picks it automatically)
// - players invite each other into calls; a call can have up to MAX_GROUP people
// - open channels: named rooms (optional password) that anybody in the lobby can create / join
// - voice frames are only forwarded to people in the sender's call or channel
//
// Run:  npm install  &&  node server.js
// Env:  PORT (default 8765)
//       VC_PASSWORD   (strongly recommended: only people who know it can connect)
//       TRUST_PROXY=1 (set when behind Render/Railway/nginx so the real client IP is used)
//       MAX_CONNECTIONS (default 200)
//       MAX_CHANNELS    (default 20 open channels per lobby)
const http = require('http');
const crypto = require('crypto');
const { WebSocketServer } = require('ws');

const PORT = Number(process.env.PORT) || 8765;
const PASSWORD = process.env.VC_PASSWORD || '';
const TRUST_PROXY = process.env.TRUST_PROXY === '1';
const MAX_CONNECTIONS = Number(process.env.MAX_CONNECTIONS) || 200;
const MAX_GROUP = 16;
const INVITE_TTL_MS = 60_000;
const FRAME_BYTES = 320; // 20 ms of 16 kHz mu-law, must match the mod's Codec.WIRE_SAMPLES
const MAX_PENDING_INVITES = 8;
const MAX_CHANNELS = Number(process.env.MAX_CHANNELS) || 20;
const MAX_CHANNEL_NAME = 24;

// plain HTTP answer so hosting health-checks (Render/Railway/uptime pingers) get a 200
const httpServer = http.createServer((req, res) => {
  res.writeHead(200, { 'Content-Type': 'text/plain; charset=utf-8' });
  res.end('SecretVoice relay is running\n');
});
const wss = new WebSocketServer({ server: httpServer, maxPayload: 8192 });
// lobbyName -> { users: Map<id, ws>, groups: Map<gid, Set<ws>>, nextGid, channels: Map<lowerName, channel> }
// a channel is { name, gid, pwHash }; its group (Set<ws>) has group.channel pointing back at it
const lobbies = new Map();
const fails = new Map(); // ip -> { count, first, blockedUntil }

// ------------------------------------------------------------------ helpers

function send(ws, text) {
  if (ws.readyState === 1) ws.send(text);
}

const listOf = (iterable) => [...iterable].map((c) => `${c.id}:${c.name}`).join(',');

function clientIp(req) {
  if (TRUST_PROXY) {
    const xf = req.headers['x-forwarded-for'];
    if (xf) return String(xf).split(',')[0].trim();
  }
  return req.socket.remoteAddress || 'unknown';
}

function passwordOk(given) {
  if (!PASSWORD) return true;
  // hash both so timingSafeEqual always gets equal-length buffers
  const a = crypto.createHash('sha256').update(String(given)).digest();
  const b = crypto.createHash('sha256').update(PASSWORD).digest();
  return crypto.timingSafeEqual(a, b);
}

function isBlocked(ip) {
  const f = fails.get(ip);
  return !!f && f.blockedUntil > Date.now();
}

function recordFail(ip) {
  const now = Date.now();
  let f = fails.get(ip);
  if (!f || now - f.first > 60_000) {
    f = { count: 0, first: now, blockedUntil: 0 };
    fails.set(ip, f);
  }
  f.count++;
  if (f.count >= 5) f.blockedUntil = now + 5 * 60_000; // 5 wrong passwords in a minute -> 5 min ban
}

/** token bucket: returns false when the client is sending too fast */
function take(ws, key, perSecond, burst) {
  const now = Date.now();
  const b = ws[key] || (ws[key] = { t: burst, last: now });
  b.t = Math.min(burst, b.t + ((now - b.last) * perSecond) / 1000);
  b.last = now;
  if (b.t < 1) return false;
  b.t -= 1;
  return true;
}

function reject(ws, message) {
  send(ws, `error|${message}`);
  ws.close();
  setTimeout(() => ws.terminate(), 2000).unref();
}

// ------------------------------------------------------------------ lobbies / calls

function broadcastUsers(lobby) {
  const list = listOf(lobby.users.values());
  for (const c of lobby.users.values()) send(c, `users|${list}`);
}

function cleanChannelName(s) {
  return String(s || '')
    .replace(/[^\p{L}\p{N}_.\- ]/gu, '')
    .replace(/\s+/g, ' ')
    .trim()
    .slice(0, MAX_CHANNEL_NAME);
}

const hashPw = (s) => crypto.createHash('sha256').update(String(s)).digest();

/** name:locked:member+member,... (names never contain | : , or +) */
function channelsLine(lobby) {
  return [...lobby.channels.values()]
    .map((ch) => {
      const g = lobby.groups.get(ch.gid);
      const members = g ? [...g].map((m) => m.name).join('+') : '';
      return `${ch.name}:${ch.pwHash ? 1 : 0}:${members}`;
    })
    .join(',');
}

function broadcastChannels(lobby) {
  const line = `channels|${channelsLine(lobby)}`;
  for (const c of lobby.users.values()) send(c, line);
}

function sendGroup(ws) {
  const lobby = lobbies.get(ws.lobby);
  const group = lobby && ws.group ? lobby.groups.get(ws.group) : null;
  if (!group) return send(ws, 'group|0|');
  send(ws, `group|${ws.group}|${listOf(group)}|${group.channel ? group.channel.name : ''}`);
}

function leaveGroup(ws) {
  const lobby = lobbies.get(ws.lobby);
  if (!lobby || !ws.group) return;
  const gid = ws.group;
  const group = lobby.groups.get(gid);
  ws.group = 0;
  sendGroup(ws);
  if (!group) return;
  group.delete(ws);
  if (group.channel) {
    // an open channel lives as long as somebody is in it (even one person)
    if (group.size === 0) {
      lobby.groups.delete(gid);
      lobby.channels.delete(group.channel.name.toLowerCase());
    } else {
      for (const m of group) sendGroup(m);
    }
    broadcastChannels(lobby);
    return;
  }
  if (group.size <= 1) {
    // a call with one person is no call: dissolve it
    for (const m of group) {
      m.group = 0;
      sendGroup(m);
      send(m, 'info|Everyone else left the call.');
    }
    lobby.groups.delete(gid);
  } else {
    for (const m of group) sendGroup(m);
  }
}

/** Idempotent: safe to call from "close" and from a name takeover. */
function removeClient(ws) {
  if (ws.removed || !ws.lobby) return;
  ws.removed = true;
  const lobby = lobbies.get(ws.lobby);
  if (!lobby) return;
  leaveGroup(ws);
  // only delete our own slot: the id may already belong to somebody who took our place
  if (lobby.users.get(ws.id) === ws) lobby.users.delete(ws.id);
  if (lobby.users.size === 0) lobbies.delete(ws.lobby);
  else broadcastUsers(lobby);
  console.log(`- ${ws.name} left lobby "${ws.lobby}"`);
}

function handleJoin(ws, parts) {
  const name = (parts[1] || '').replace(/[^\w.\-]/g, '').slice(0, 16);
  const lobbyName = (parts[2] || '').replace(/[\u0000-\u001f]/g, '').slice(0, 64) || 'default';
  const password = parts.slice(3).join('|');
  if (!passwordOk(password)) {
    recordFail(ws.ip);
    return reject(ws, 'wrong password');
  }
  if (!name) return reject(ws, 'bad name');

  let lobby = lobbies.get(lobbyName);
  if (lobby) {
    const dup = [...lobby.users.values()].find((c) => c.name.toLowerCase() === name.toLowerCase());
    if (dup) {
      // same player reconnecting (crashed client / changed network): let the new connection replace
      // the stale one instead of locking them out until the old socket times out.
      if (dup.ip === ws.ip || !dup.isAlive) {
        send(dup, 'error|You connected from another place.');
        removeClient(dup);
        dup.terminate();
        lobby = lobbies.get(lobbyName); // removeClient may have deleted an empty lobby
      } else {
        return reject(ws, 'that name is already connected');
      }
    }
  }
  if (!lobby) {
    lobby = { users: new Map(), groups: new Map(), nextGid: 1, channels: new Map() };
    lobbies.set(lobbyName, lobby);
  }
  let id = 1;
  while (lobby.users.has(id) && id < 255) id++;
  if (lobby.users.has(id)) return reject(ws, 'lobby full');

  ws.id = id;
  ws.name = name;
  ws.lobby = lobbyName;
  ws.group = 0;
  ws.invites = new Map(); // gid -> { time, from }
  lobby.users.set(id, ws);
  send(ws, `hello|${id}`);
  broadcastUsers(lobby);
  sendGroup(ws);
  send(ws, `channels|${channelsLine(lobby)}`);
  console.log(`+ ${name} joined lobby "${lobbyName}" (${lobby.users.size} online)`);
}

function handleText(ws, parts) {
  const lobby = lobbies.get(ws.lobby);
  if (!lobby) return;
  switch (parts[0]) {
    case 'invite': {
      const now = Date.now();
      if (ws.lastInvite && now - ws.lastInvite < 800) return; // anti-spam
      ws.lastInvite = now;
      const wanted = (parts[1] || '').toLowerCase();
      const target = [...lobby.users.values()].find((c) => c.name.toLowerCase() === wanted);
      if (!target) return send(ws, 'info|That player is not using SecretVoice.');
      if (target === ws) return send(ws, "info|You can't invite yourself.");
      if (!ws.group) {
        const gid = lobby.nextGid++;
        lobby.groups.set(gid, new Set([ws]));
        ws.group = gid;
        sendGroup(ws);
      }
      const group = lobby.groups.get(ws.group);
      if (!group) return;
      if (group.has(target)) return send(ws, `info|${target.name} is already in your call.`);
      if (group.size >= MAX_GROUP) return send(ws, 'info|Your call is full.');
      for (const [g, inv] of target.invites) {
        if (now - inv.time > INVITE_TTL_MS) target.invites.delete(g);
      }
      if (target.invites.size >= MAX_PENDING_INVITES) return send(ws, `info|${target.name} has too many pending invites.`);
      group.lastInvite = now;
      target.invites.set(ws.group, { time: now, from: ws });
      send(target, `invite|${ws.group}|${ws.name}`);
      send(ws, `info|Invited ${target.name}.`);
      return;
    }
    case 'accept': {
      const gid = Number(parts[1]);
      const inv = ws.invites.get(gid);
      ws.invites.delete(gid);
      const group = lobby.groups.get(gid);
      if (!inv || Date.now() - inv.time > INVITE_TTL_MS || !group) return send(ws, 'info|That invite expired.');
      if (group.size >= MAX_GROUP) return send(ws, 'info|That call is full.');
      if (ws.group === gid) return;
      leaveGroup(ws);
      // leaving may have dissolved the target call if it was just us + the inviter: re-check
      const target = lobby.groups.get(gid);
      if (!target) return send(ws, 'info|That invite expired.');
      target.add(ws);
      ws.group = gid;
      for (const m of target) sendGroup(m);
      if (target.channel) broadcastChannels(lobby);
      return;
    }
    case 'decline': {
      const gid = Number(parts[1]);
      const inv = ws.invites.get(gid);
      ws.invites.delete(gid);
      if (inv && inv.from) send(inv.from, `info|${ws.name} declined your call.`);
      return;
    }
    case 'leave':
      leaveGroup(ws);
      return;
    case 'chcreate': {
      const now = Date.now();
      if (ws.lastChannelOp && now - ws.lastChannelOp < 1000) return; // anti-spam
      ws.lastChannelOp = now;
      const name = cleanChannelName(parts[1]);
      if (!name) return send(ws, 'info|Give the channel a name (letters, numbers, spaces).');
      const key = name.toLowerCase();
      if (lobby.channels.has(key)) return send(ws, `info|Channel "${name}" already exists.`);
      if (lobby.channels.size >= MAX_CHANNELS) return send(ws, 'info|Too many channels here, join one instead.');
      const pw = parts.slice(2).join('|');
      leaveGroup(ws);
      const gid = lobby.nextGid++;
      const ch = { name, gid, pwHash: pw ? hashPw(pw) : null };
      const group = new Set([ws]);
      group.channel = ch;
      lobby.groups.set(gid, group);
      lobby.channels.set(key, ch);
      ws.group = gid;
      sendGroup(ws);
      broadcastChannels(lobby);
      console.log(`# ${ws.name} created channel "${name}"${ch.pwHash ? ' (locked)' : ''} in lobby "${ws.lobby}"`);
      return;
    }
    case 'chjoin': {
      const now = Date.now();
      if (ws.lastChannelOp && now - ws.lastChannelOp < 500) return; // anti-spam
      ws.lastChannelOp = now;
      const key = cleanChannelName(parts[1]).toLowerCase();
      const ch = lobby.channels.get(key);
      const group = ch ? lobby.groups.get(ch.gid) : null;
      if (!ch || !group) {
        if (ch) { // stale entry: clean it up
          lobby.channels.delete(key);
          broadcastChannels(lobby);
        }
        return send(ws, 'info|That channel does not exist (anymore).');
      }
      if (ws.group === ch.gid) return;
      if (ch.pwHash) {
        if (ws.chBlockedUntil && now < ws.chBlockedUntil) {
          return send(ws, 'info|Too many wrong channel passwords. Wait a minute.');
        }
        if (!crypto.timingSafeEqual(hashPw(parts.slice(2).join('|')), ch.pwHash)) {
          ws.chFails = (ws.chFails || 0) + 1;
          if (ws.chFails >= 5) {
            ws.chBlockedUntil = now + 60_000;
            ws.chFails = 0;
          }
          return send(ws, 'info|Wrong channel password.');
        }
        ws.chFails = 0;
      }
      if (group.size >= MAX_GROUP) return send(ws, 'info|That channel is full.');
      leaveGroup(ws);
      group.add(ws);
      ws.group = ch.gid;
      for (const m of group) sendGroup(m);
      broadcastChannels(lobby);
      return;
    }
    default:
  }
}

// ------------------------------------------------------------------ connections

wss.on('connection', (ws, req) => {
  ws.ip = clientIp(req);
  ws.isAlive = true;
  ws.lobby = null;
  ws.group = 0;
  ws.on('pong', () => { ws.isAlive = true; });

  if (isBlocked(ws.ip)) return reject(ws, 'too many wrong passwords, try again later');
  if (wss.clients.size > MAX_CONNECTIONS) return reject(ws, 'relay is full');

  const joinTimer = setTimeout(() => { if (!ws.lobby) ws.terminate(); }, 10_000);

  ws.on('message', (data, isBinary) => {
    if (!isBinary) {
      if (!take(ws, 'textBucket', 20, 40)) return; // flooding: drop
      const parts = data.toString().split('|');
      if (parts[0] === 'join') {
        if (!ws.lobby) handleJoin(ws, parts);
        return;
      }
      if (ws.lobby) handleText(ws, parts);
      return;
    }
    // binary = one voice frame -> forward to the other people in my call
    if (!ws.lobby || !ws.group || !Buffer.isBuffer(data) || data.length !== FRAME_BYTES) return;
    if (!take(ws, 'voiceBucket', 80, 120)) return; // normal speech is 50 frames/s
    const lobby = lobbies.get(ws.lobby);
    const group = lobby && lobby.groups.get(ws.group);
    if (!group) return;
    const frame = Buffer.concat([Buffer.from([ws.id]), data]);
    for (const peer of group) {
      if (peer !== ws && peer.readyState === 1 && peer.bufferedAmount < 65536) {
        peer.send(frame, { binary: true });
      }
    }
  });

  ws.on('close', () => {
    clearTimeout(joinTimer);
    removeClient(ws);
  });
  ws.on('error', () => {});
});

// ------------------------------------------------------------------ housekeeping

// drop dead connections / keep idle proxies awake
setInterval(() => {
  for (const ws of wss.clients) {
    if (!ws.isAlive) { ws.terminate(); continue; }
    ws.isAlive = false;
    ws.ping();
  }
}, 30_000);

// dissolve calls where the inviter has been waiting alone, forget old failed-password records
setInterval(() => {
  const now = Date.now();
  for (const lobby of lobbies.values()) {
    for (const [gid, group] of lobby.groups) {
      if (group.channel) continue; // open channels are not "waiting for an invite"
      if (group.size <= 1 && now - (group.lastInvite || 0) > INVITE_TTL_MS + 2000) {
        for (const m of group) {
          m.group = 0;
          sendGroup(m);
          send(m, 'info|Nobody joined your call.');
        }
        lobby.groups.delete(gid);
      }
    }
  }
  for (const [ip, f] of fails) {
    if (f.blockedUntil < now && now - f.first > 60_000) fails.delete(ip);
  }
}, 5_000);

function shutdown() {
  console.log('Shutting down...');
  for (const ws of wss.clients) {
    send(ws, 'info|The voice relay is restarting.');
    ws.close();
  }
  setTimeout(() => process.exit(0), 500).unref();
}
process.on('SIGINT', shutdown);
process.on('SIGTERM', shutdown);
// a bug in one handler must not take the whole relay down
process.on('uncaughtException', (e) => console.error('uncaughtException:', e));

httpServer.listen(PORT, () => {
  console.log(`SecretVoice relay listening on port ${PORT}${PASSWORD ? ' (password protected)' : ' (NO PASSWORD - set VC_PASSWORD)'}`);
});
