/**
 * WA Inbox Engine — multi-account WhatsApp client (read + manual reply)
 * For the Herzl dashboard. Baileys-based. REST + WebSocket. Always-on (Railway).
 *
 * This is NOT an auto-responder bot. It only mirrors your chats and sends
 * messages that YOU trigger from the dashboard — like WhatsApp Web, multi-account.
 *
 * Security: every HTTP/WS call must carry the WA_API_TOKEN (Bearer header or ?token=).
 */
import { webcrypto } from 'node:crypto';
// Node 18 has no global Web Crypto; Baileys' hkdf needs it. Polyfill before use.
if (!globalThis.crypto) globalThis.crypto = webcrypto;
import express from 'express';
import cors from 'cors';
import { WebSocketServer } from 'ws';
import http from 'http';
import fs from 'fs';
import path from 'path';
import qrcode from 'qrcode';
import pino from 'pino';
import * as BaileysNS from '@whiskeysockets/baileys';
// Robust interop: Baileys 6.7 ESM may expose exports on default or namespace.
const Baileys = (BaileysNS.default && typeof BaileysNS.default === 'object')
  ? { ...BaileysNS, ...BaileysNS.default } : BaileysNS;
const makeWASocket = Baileys.makeWASocket || BaileysNS.default;
const { useMultiFileAuthState, DisconnectReason, fetchLatestBaileysVersion, makeCacheableSignalKeyStore } = Baileys;
const downloadMediaMessage = Baileys.downloadMediaMessage;
import { Boom } from '@hapi/boom';

const PORT = process.env.PORT || 3000;
const TOKEN = process.env.WA_API_TOKEN || '';            // REQUIRED — set in Railway
const DATA_DIR = process.env.DATA_DIR || './data';        // mount a Railway volume here
const ALLOW_ORIGIN = process.env.ALLOW_ORIGIN || '*';     // e.g. https://control.alaw.co.il
const MAX_MSGS_PER_CHAT = 80;

const log = pino({ level: process.env.LOG_LEVEL || 'warn' });
fs.mkdirSync(path.join(DATA_DIR, 'auth'), { recursive: true });
const ACCOUNTS_FILE = path.join(DATA_DIR, 'accounts.json');
const STATE_FILE = path.join(DATA_DIR, 'state.json'); // persisted chats+messages (survive restarts)

// ---------- chat/message persistence (so a restart/redeploy never wipes the inbox) ----------
let _saveTimer = null;
function saveState() {
  try {
    const out = {};
    for (const [id, a] of Object.entries(accounts)) {
      const msgs = {};
      for (const [jid, arr] of a.msgs.entries()) msgs[jid] = arr;
      out[id] = { chats: [...a.chats.values()], msgs };
    }
    fs.writeFileSync(STATE_FILE, JSON.stringify(out));
  } catch (e) { log.error('saveState ' + e); }
}
function scheduleSave() { if (_saveTimer) return; _saveTimer = setTimeout(() => { _saveTimer = null; saveState(); }, 4000); }
function loadStateInto(id, acc) {
  try {
    const st = JSON.parse(fs.readFileSync(STATE_FILE, 'utf8'));
    const s = st[id]; if (!s) return;
    if (Array.isArray(s.chats)) for (const c of s.chats) { if (isBadName(c.name)) c.name = niceName(c.jid); acc.chats.set(c.jid, c); }
    if (s.msgs) for (const jid of Object.keys(s.msgs)) acc.msgs.set(jid, s.msgs[jid]);
  } catch (e) {}
}

// ---------- account registry ----------
function loadAccounts() {
  try { return JSON.parse(fs.readFileSync(ACCOUNTS_FILE, 'utf8')); } catch { return {}; }
}
function saveAccounts(obj) {
  const out = {};
  for (const [id, a] of Object.entries(obj)) out[id] = { id: a.id, name: a.name };
  fs.writeFileSync(ACCOUNTS_FILE, JSON.stringify(out, null, 2));
}

/** runtime state per account: { id, name, sock, status, qr, me, chats:Map, msgs:Map } */
const accounts = {};

// ---------- helpers ----------
function msgText(m) {
  const c = (m && m.message) || {};
  return (
    c.conversation ||
    (c.extendedTextMessage && c.extendedTextMessage.text) ||
    (c.imageMessage && (c.imageMessage.caption || '[image]')) ||
    (c.videoMessage && (c.videoMessage.caption || '[video]')) ||
    (c.documentMessage && (c.documentMessage.caption || '[document]')) ||
    (c.audioMessage && '[audio]') ||
    (c.stickerMessage && '[sticker]') ||
    (c.locationMessage && '[location]') ||
    (c.contactMessage && '[contact]') ||
    ''
  );
}
function mediaInfo(m) {
  const c = (m && m.message) || {};
  if (c.imageMessage)    return { type: 'image',    mime: c.imageMessage.mimetype || 'image/jpeg' };
  if (c.videoMessage)    return { type: 'video',    mime: c.videoMessage.mimetype || 'video/mp4' };
  if (c.audioMessage)    return { type: 'audio',    mime: c.audioMessage.mimetype || 'audio/ogg' };
  if (c.stickerMessage)  return { type: 'sticker',  mime: 'image/webp' };
  if (c.documentMessage) return { type: 'document', mime: c.documentMessage.mimetype || 'application/octet-stream', name: c.documentMessage.fileName || 'document' };
  return null;
}
// human-friendly fallback name from a jid (when no pushName/contact name is known)
function niceName(jid) {
  if (!jid) return '';
  const n = jid.split('@')[0];
  if (jid.endsWith('@s.whatsapp.net')) return '+' + n;
  if (jid.endsWith('@g.us')) return 'קבוצה';
  if (jid.endsWith('@newsletter')) return 'ערוץ';
  return n; // @lid or other: show the identifier number (real phone shown separately when known)
}
function isBadName(s) { return !s || /^\d{6,}$/.test(s) || s === 'קבוצה' || s === 'ערוץ' || s === 'איש קשר'; }
function tsOf(m) {
  const t = m.messageTimestamp;
  if (!t) return Date.now();
  return (typeof t === 'number' ? t : Number(t.low || t)) * 1000;
}

function recordMessage(acc, m, broadcast = true) {
  if (!m || !m.key || !m.key.remoteJid) return;
  const jid = m.key.remoteJid;
  if (jid === 'status@broadcast') return;
  const text = msgText(m);
  const entry = {
    id: m.key.id,
    jid,
    fromMe: !!m.key.fromMe,
    author: m.pushName || (m.key.fromMe ? 'You' : jid.split('@')[0]),
    text,
    ts: tsOf(m),
  };
  const mi = mediaInfo(m);
  if (mi) {
    entry.media = { type: mi.type, mime: mi.mime, name: mi.name || null };
    if (!acc.raw) acc.raw = new Map();
    acc.raw.set(entry.id, m);
    if (acc.raw.size > 400) { const k = acc.raw.keys().next().value; acc.raw.delete(k); }
  }
  if (!acc.msgs.has(jid)) acc.msgs.set(jid, []);
  const arr = acc.msgs.get(jid);
  if (!arr.find((x) => x.id === entry.id)) {
    arr.push(entry);
    arr.sort((a, b) => a.ts - b.ts);
    if (arr.length > MAX_MSGS_PER_CHAT) arr.splice(0, arr.length - MAX_MSGS_PER_CHAT);
  }
  const chat = acc.chats.get(jid) || { jid, name: niceName(jid), unread: 0, ts: 0, last: '' };
  if (entry.ts >= chat.ts) { chat.ts = entry.ts; chat.last = text; }
  if (!entry.fromMe && broadcast) chat.unread = (chat.unread || 0) + 1; // count only live messages, never history
  // try to capture the real phone behind a LID-only contact (WhatsApp exposes it in *Alt fields when known)
  if (!chat.phone) {
    const cand = (m.key && (m.key.senderPn || m.key.participantPn || m.key.remoteJidAlt || m.key.participantAlt || m.key.participant)) || m.participant || m.senderPn || '';
    if (/@s\.whatsapp\.net$/.test(cand)) chat.phone = cand.split('@')[0];
    else if (jid.endsWith('@s.whatsapp.net')) chat.phone = jid.split('@')[0];
  }
  const cn = acc.contacts && acc.contacts.get(jid);
  if (!jid.endsWith('@g.us') && m.pushName && m.pushName.trim()) chat.name = m.pushName.trim();
  else if (cn && cn.trim() && !/^\d{5,}$/.test(cn)) chat.name = cn.trim();
  else if (isBadName(chat.name)) chat.name = chat.phone ? ('+' + chat.phone) : niceName(jid);
  acc.chats.set(jid, chat);
  if (broadcast) wsBroadcast({ type: 'message', accountId: acc.id, message: entry, chat });
  scheduleSave();
}

// ---------- baileys connection ----------
async function startAccount(id, name) {
  const authDir = path.join(DATA_DIR, 'auth', id);
  fs.mkdirSync(authDir, { recursive: true });
  const { state, saveCreds } = await useMultiFileAuthState(authDir);
  const { version } = await fetchLatestBaileysVersion();

  const acc = accounts[id] || { id, name: name || id, status: 'connecting', qr: null, me: null, chats: new Map(), msgs: new Map(), raw: new Map(), contacts: new Map() };
  if (!acc.raw) acc.raw = new Map();
  if (!acc.contacts) acc.contacts = new Map();
  acc.name = name || acc.name; acc.status = 'connecting'; accounts[id] = acc;
  if (acc.chats.size === 0) loadStateInto(id, acc); // restore persisted chats/messages after a restart

  const sock = makeWASocket({
    version,
    logger: log,
    printQRInTerminal: false,
    auth: { creds: state.creds, keys: makeCacheableSignalKeyStore(state.keys, log) },
    markOnlineOnConnect: false,
    syncFullHistory: true,
    browser: ['Herzl Inbox', 'Chrome', '1.0'],
  });
  acc.sock = sock;

  sock.ev.on('creds.update', saveCreds);

  sock.ev.on('connection.update', async (u) => {
    const { connection, lastDisconnect, qr } = u;
    if (qr) {
      acc.qr = await qrcode.toDataURL(qr);
      acc.status = 'qr';
      wsBroadcast({ type: 'qr', accountId: id, qr: acc.qr });
    }
    if (connection === 'open') {
      acc.status = 'connected';
      acc.qr = null;
      acc.me = sock.user ? { id: sock.user.id, name: sock.user.name } : null;
      saveAccounts(accounts);
      wsBroadcast({ type: 'status', accountId: id, status: 'connected', me: acc.me });
    }
    if (connection === 'close') {
      const code = (lastDisconnect && lastDisconnect.error instanceof Boom)
        ? lastDisconnect.error.output.statusCode : 0;
      if (code === DisconnectReason.loggedOut) {
        acc.status = 'logged_out';
        wsBroadcast({ type: 'status', accountId: id, status: 'logged_out' });
      } else {
        acc.status = 'reconnecting';
        wsBroadcast({ type: 'status', accountId: id, status: 'reconnecting' });
        setTimeout(() => startAccount(id, acc.name).catch((e) => log.error(e)), 2500);
      }
    }
  });

  // history sync on first login → populate chats/messages
  sock.ev.on('messaging-history.set', ({ chats = [], messages = [] }) => {
    for (const ch of chats) {
      if (!ch.id || ch.id === 'status@broadcast') continue;
      const ex = acc.chats.get(ch.id) || { jid: ch.id, name: '', unread: 0, ts: 0, last: '' };
      if (ch.name && ch.name.trim()) ex.name = ch.name.trim();
      else if (isBadName(ex.name)) ex.name = niceName(ch.id);
      if (typeof ch.unreadCount === 'number') ex.unread = ch.unreadCount;
      if (typeof ch.archived !== 'undefined') ex.archived = !!ch.archived;
      if (typeof ch.pinned !== 'undefined') ex.pinned = !!ch.pinned;
      if (ch.muteEndTime) ex.muted = Number(ch.muteEndTime) * 1000;
      acc.chats.set(ch.id, ex);
    }
    for (const m of messages) recordMessage(acc, m, false);
    wsBroadcast({ type: 'chats', accountId: id });
    scheduleSave();
  });

  // track archive / unarchive (and pin) changes coming from the phone
  sock.ev.on('chats.update', (updates) => {
    for (const u of updates || []) {
      const c = acc.chats.get(u.id);
      if (!c) continue;
      if (typeof u.archived !== 'undefined') c.archived = !!u.archived;
      if (typeof u.pinned !== 'undefined') c.pinned = !!u.pinned;
      if (typeof u.mute !== 'undefined') c.muted = u.mute ? Number(u.mute) : 0;
    }
    wsBroadcast({ type: 'chats', accountId: id });
    scheduleSave();
  });

  sock.ev.on('messages.upsert', ({ messages, type }) => {
    if (type !== 'notify' && type !== 'append') return;
    for (const m of messages) recordMessage(acc, m, true);
  });

  // contact name sync (so chats show real names like the official app, not LID numbers)
  const onContacts = (list) => {
    let changed = false;
    for (const c of (list || [])) {
      if (!c || !c.id) continue;
      const nm = c.name || c.notify || c.verifiedName;
      if (nm && nm.trim()) {
        acc.contacts.set(c.id, nm.trim());
        const ex = acc.chats.get(c.id);
        if (ex && isBadName(ex.name)) { ex.name = nm.trim(); changed = true; }
      }
    }
    if (changed) { wsBroadcast({ type: 'chats', accountId: id }); scheduleSave(); }
  };
  sock.ev.on('contacts.upsert', onContacts);
  sock.ev.on('contacts.set', (a) => onContacts(a && a.contacts ? a.contacts : a));
  sock.ev.on('contacts.update', onContacts);

  return acc;
}

async function bootstrap() {
  const saved = loadAccounts();
  for (const id of Object.keys(saved)) {
    try { await startAccount(id, saved[id].name); }
    catch (e) { log.error('start failed ' + id, e); }
  }
}

// ---------- http api ----------
const app = express();
app.use(cors({ origin: ALLOW_ORIGIN }));
app.use(express.json({ limit: '30mb' }));

function auth(req, res, next) {
  const h = req.headers.authorization || '';
  const t = h.startsWith('Bearer ') ? h.slice(7) : (req.query.token || '');
  if (!TOKEN || t !== TOKEN) return res.status(401).json({ error: 'unauthorized' });
  next();
}

app.get('/health', (req, res) => res.json({ ok: true, accounts: Object.keys(accounts).length }));

app.get('/accounts', auth, (req, res) => {
  res.json(Object.values(accounts).map((a) => ({
    id: a.id, name: a.name, status: a.status, me: a.me,
    chats: a.chats.size,
    unread: [...a.chats.values()].reduce((s, c) => s + (c.unread || 0), 0),
  })));
});

app.post('/accounts', auth, async (req, res) => {
  const id = (req.body.id || ('acc_' + Date.now())).toString().replace(/[^a-zA-Z0-9_-]/g, '');
  const name = (req.body.name || id).toString().slice(0, 60);
  if (accounts[id] && accounts[id].status === 'connected')
    return res.json({ id, status: 'connected' });
  try {
    await startAccount(id, name);
    res.json({ id, name, status: accounts[id].status });
  } catch (e) {
    log.error(e);
    res.status(500).json({ error: 'start_failed', detail: String(e).slice(0, 200) });
  }
});

// rename an account (custom label set by the user)
app.patch('/accounts/:id', auth, (req, res) => {
  const a = accounts[req.params.id];
  if (!a) return res.status(404).json({ error: 'no_account' });
  const name = (req.body && req.body.name ? String(req.body.name) : '').slice(0, 60).trim();
  if (!name) return res.status(400).json({ error: 'name_required' });
  a.name = name;
  saveAccounts(accounts);
  wsBroadcast({ type: 'status', accountId: a.id, status: a.status, name });
  res.json({ ok: true, id: a.id, name });
});

// proactive re-sync: pull the address book (contact names) + chat metadata (archive) from WhatsApp app-state
app.post('/accounts/:id/sync', auth, async (req, res) => {
  const a = accounts[req.params.id];
  if (!a || !a.sock) return res.status(404).json({ error: 'no_account' });
  if (a.status !== 'connected') return res.status(409).json({ error: 'not_connected', status: a.status });
  try {
    await a.sock.resyncAppState(['critical_unblock_low', 'regular_high', 'regular_low'], false);
    wsBroadcast({ type: 'chats', accountId: a.id });
    scheduleSave();
    res.json({ ok: true, contacts: a.contacts ? a.contacts.size : 0, chats: a.chats.size });
  } catch (e) {
    res.status(500).json({ error: 'sync_failed', detail: String(e).slice(0, 200) });
  }
});

// chat actions (pin / archive / mute / read / delete) — mirrors to WhatsApp via chatModify
app.post('/accounts/:id/chat-action', auth, async (req, res) => {
  const a = accounts[req.params.id];
  if (!a || !a.sock) return res.status(404).json({ error: 'no_account' });
  if (a.status !== 'connected') return res.status(409).json({ error: 'not_connected', status: a.status });
  const { jid, action, value } = req.body || {};
  if (!jid || !action) return res.status(400).json({ error: 'jid_and_action_required' });
  const chat = a.chats.get(jid);
  const arr = a.msgs.get(jid) || [];
  const last = arr[arr.length - 1];
  const lastMessages = last ? [{ key: { remoteJid: jid, id: last.id, fromMe: !!last.fromMe }, messageTimestamp: Math.floor(last.ts / 1000) }] : [];
  try {
    let mod;
    if (action === 'pin') mod = { pin: !!value };
    else if (action === 'mute') mod = { mute: value ? Number(value) : null };
    else if (action === 'archive') mod = { archive: !!value, lastMessages };
    else if (action === 'read') mod = { markRead: value !== false, lastMessages };
    else if (action === 'delete') mod = { delete: true, lastMessages };
    else return res.status(400).json({ error: 'bad_action' });
    await a.sock.chatModify(mod, jid);
    if (chat) {
      if (action === 'pin') chat.pinned = !!value;
      else if (action === 'mute') chat.muted = value ? (Date.now() + Number(value)) : 0;
      else if (action === 'archive') chat.archived = !!value;
      else if (action === 'read') chat.unread = (value === false) ? (chat.unread || 1) : 0;
      else if (action === 'delete') a.chats.delete(jid);
    }
    wsBroadcast({ type: 'chats', accountId: a.id });
    scheduleSave();
    res.json({ ok: true });
  } catch (e) {
    res.status(500).json({ error: 'action_failed', detail: String(e).slice(0, 200) });
  }
});

// compact digest for Herzl: connected accounts + recent chats (+ optional recent messages)
app.get('/digest', auth, (req, res) => {
  const perAcc = parseInt(req.query.chats) || 35;
  const withMsgs = req.query.msgs === '1';
  const out = Object.values(accounts).map((a) => {
    const chats = [...a.chats.values()].sort((x, y) => y.ts - x.ts).slice(0, perAcc).map((c) => {
      const o = { name: c.name, unread: c.unread || 0, last: (c.last || '').slice(0, 120),
        ts: c.ts, time: c.ts ? new Date(c.ts).toISOString() : null };
      if (withMsgs) {
        const arr = a.msgs.get(c.jid) || [];
        o.recent = arr.slice(-6).map((m) => ({ from: m.fromMe ? 'me' : (m.author || ''), text: (m.text || '').slice(0, 200), time: new Date(m.ts).toISOString() }));
      }
      return o;
    });
    return { id: a.id, name: a.name, status: a.status,
      totalChats: a.chats.size,
      totalUnread: [...a.chats.values()].reduce((s, c) => s + (c.unread || 0), 0),
      chats };
  });
  res.json(out);
});

// search messages across all chats of an account by text
app.get('/accounts/:id/search', auth, (req, res) => {
  const a = accounts[req.params.id];
  if (!a) return res.status(404).json({ error: 'no_account' });
  const q = String(req.query.q || '').toLowerCase().trim();
  if (!q) return res.status(400).json({ error: 'q_required' });
  const hits = [];
  for (const [jid, arr] of a.msgs.entries()) {
    const chat = a.chats.get(jid);
    for (const m of arr) {
      if ((m.text || '').toLowerCase().includes(q)) {
        hits.push({ chat: chat ? chat.name : jid.split('@')[0], jid, from: m.fromMe ? 'me' : (m.author || ''), text: m.text, ts: m.ts });
      }
    }
  }
  hits.sort((x, y) => y.ts - x.ts);
  res.json(hits.slice(0, parseInt(req.query.limit) || 50));
});

app.get('/accounts/:id/qr', auth, (req, res) => {
  const a = accounts[req.params.id];
  if (!a) return res.status(404).json({ error: 'no_account' });
  res.json({ status: a.status, qr: a.qr || null, me: a.me || null });
});

app.get('/accounts/:id/chats', auth, (req, res) => {
  const a = accounts[req.params.id];
  if (!a) return res.status(404).json({ error: 'no_account' });
  let list = [...a.chats.values()];
  const arch = req.query.archived;
  if (arch === '1') list = list.filter((c) => c.archived);
  else if (arch !== 'all') list = list.filter((c) => !c.archived);
  list = list.sort((x, y) => ((y.pinned ? 1 : 0) - (x.pinned ? 1 : 0)) || (y.ts - x.ts)).slice(0, 300);
  res.json(list);
});

// count of archived chats (for the "Archived" row in the UI)
app.get('/accounts/:id/archived-count', auth, (req, res) => {
  const a = accounts[req.params.id];
  if (!a) return res.status(404).json({ error: 'no_account' });
  res.json({ count: [...a.chats.values()].filter((c) => c.archived).length });
});

app.get('/accounts/:id/messages', auth, (req, res) => {
  const a = accounts[req.params.id];
  if (!a) return res.status(404).json({ error: 'no_account' });
  const jid = req.query.jid;
  if (!jid) return res.status(400).json({ error: 'jid_required' });
  const arr = a.msgs.get(jid) || [];
  const chat = a.chats.get(jid);
  if (chat) { chat.unread = 0; acc_touch(a); }   // mark read on open
  res.json(arr.slice(-(parseInt(req.query.limit) || 60)));
});

app.post('/accounts/:id/send', auth, async (req, res) => {
  const a = accounts[req.params.id];
  if (!a || !a.sock) return res.status(404).json({ error: 'no_account' });
  if (a.status !== 'connected') return res.status(409).json({ error: 'not_connected', status: a.status });
  const { jid, text } = req.body || {};
  if (!jid || !text) return res.status(400).json({ error: 'jid_and_text_required' });
  try {
    const sent = await a.sock.sendMessage(jid, { text: String(text) });
    recordMessage(a, sent, true);
    res.json({ ok: true });
  } catch (e) {
    res.status(500).json({ error: 'send_failed', detail: String(e).slice(0, 200) });
  }
});

// download media for a stored message (returns binary with content-type)
app.get('/accounts/:id/media', auth, async (req, res) => {
  const a = accounts[req.params.id];
  if (!a) return res.status(404).json({ error: 'no_account' });
  const m = a.raw && a.raw.get(req.query.mid);
  if (!m) return res.status(404).json({ error: 'no_media' });
  try {
    const buf = await downloadMediaMessage(m, 'buffer', {}, { logger: log, reuploadRequest: a.sock && a.sock.updateMediaMessage });
    const mi = mediaInfo(m) || { mime: 'application/octet-stream' };
    res.setHeader('Content-Type', mi.mime);
    res.setHeader('Cache-Control', 'private, max-age=86400');
    if (mi.name && mi.type === 'document') res.setHeader('Content-Disposition', 'inline; filename="' + encodeURIComponent(mi.name) + '"');
    res.end(buf);
  } catch (e) {
    res.status(500).json({ error: 'download_failed', detail: String(e).slice(0, 150) });
  }
});

// send media (image / video / document) — base64 data URL or raw base64
app.post('/accounts/:id/sendMedia', auth, async (req, res) => {
  const a = accounts[req.params.id];
  if (!a || !a.sock) return res.status(404).json({ error: 'no_account' });
  if (a.status !== 'connected') return res.status(409).json({ error: 'not_connected', status: a.status });
  const { jid, data, mime, caption, kind, fileName } = req.body || {};
  if (!jid || !data) return res.status(400).json({ error: 'jid_and_data_required' });
  try {
    const buf = Buffer.from(String(data).replace(/^data:[^;]+;base64,/, ''), 'base64');
    const k = kind || ((mime || '').startsWith('video') ? 'video' : (mime || '').startsWith('image') ? 'image' : 'document');
    let msg;
    if (k === 'image') msg = { image: buf, caption: caption || '' };
    else if (k === 'video') msg = { video: buf, caption: caption || '' };
    else msg = { document: buf, mimetype: mime || 'application/octet-stream', fileName: fileName || 'file' };
    const sent = await a.sock.sendMessage(jid, msg);
    recordMessage(a, sent, true);
    res.json({ ok: true });
  } catch (e) {
    res.status(500).json({ error: 'send_failed', detail: String(e).slice(0, 150) });
  }
});

app.delete('/accounts/:id', auth, async (req, res) => {
  const a = accounts[req.params.id];
  if (!a) return res.status(404).json({ error: 'no_account' });
  try { if (a.sock) await a.sock.logout(); } catch {}
  delete accounts[req.params.id];
  saveAccounts(accounts);
  try { fs.rmSync(path.join(DATA_DIR, 'auth', req.params.id), { recursive: true, force: true }); } catch {}
  wsBroadcast({ type: 'status', accountId: req.params.id, status: 'removed' });
  res.json({ ok: true });
});

function acc_touch(a) { /* hook for future persistence */ }

// ---------- websocket (real-time push) ----------
const server = http.createServer(app);
const wss = new WebSocketServer({ server, path: '/ws' });
wss.on('connection', (ws, req) => {
  const u = new URL(req.url, 'http://x');
  if (!TOKEN || u.searchParams.get('token') !== TOKEN) { ws.close(4001, 'unauthorized'); return; }
  ws.isAlive = true;
  ws.on('pong', () => { ws.isAlive = true; });
  ws.send(JSON.stringify({ type: 'hello', accounts: Object.values(accounts).map((a) => ({ id: a.id, name: a.name, status: a.status })) }));
});
function wsBroadcast(obj) {
  const data = JSON.stringify(obj);
  wss.clients.forEach((c) => { if (c.readyState === 1) c.send(data); });
}
setInterval(() => {
  wss.clients.forEach((c) => { if (!c.isAlive) return c.terminate(); c.isAlive = false; c.ping(); });
}, 30000);

// periodic + on-exit persistence so chats survive restarts/redeploys
setInterval(saveState, 60000);
process.on('SIGTERM', () => { try { saveState(); } catch (e) {} process.exit(0); });
process.on('SIGINT', () => { try { saveState(); } catch (e) {} process.exit(0); });

// keep the process alive even if a Baileys error escapes
process.on('uncaughtException', (e) => log.error({ err: String((e && e.stack) || e) }, 'uncaught'));
process.on('unhandledRejection', (e) => log.error({ err: String(e) }, 'unhandled'));

server.listen(PORT, () => {
  if (!TOKEN) log.warn('WA_API_TOKEN is empty — set it in Railway before exposing publicly!');
  log.warn('WA Inbox Engine listening on :' + PORT);
  bootstrap().catch((e) => log.error(e));
});
