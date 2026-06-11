const { WechatyBuilder } = require('wechaty');
const express = require('express');
const axios = require('axios');
const QRCode = require('qrcode');

const HERMES_INBOUND_URL = process.env.HERMES_INBOUND_URL || 'http://localhost:8000/hermes/inbound/wechat';
const BRIDGE_PORT = parseInt(process.env.BRIDGE_PORT || '3000', 10);
const QR_PORT = parseInt(process.env.QR_PORT || '3001', 10);

let currentQrData = null;

// ── Wechaty bot ──────────────────────────────────────────────────────────────
const bot = WechatyBuilder.build({
  name: 'hermes-wechat',
  puppet: 'wechaty-puppet-wechat4u',
});

bot.on('scan', async (qrcode, status) => {
  console.log(`[hermes-wechaty] QR scan required (status=${status}). Open http://localhost:${QR_PORT}/qr`);
  currentQrData = qrcode;
});

bot.on('login', (user) => {
  console.log(`[hermes-wechaty] Logged in as: ${user}`);
  currentQrData = null;
});

bot.on('logout', (user) => {
  console.log(`[hermes-wechaty] Logged out: ${user}`);
});

bot.on('message', async (message) => {
  // Skip self, groups (unless @mentioned), and system messages
  if (message.self()) return;
  if (message.room()) return;  // skip group chats for now
  if (!message.text()) return;

  const contact = message.talker();
  const payload = {
    source: 'wechat',
    sender_id: contact.id,
    sender_name: contact.name(),
    text: message.text(),
    timestamp: message.date().toISOString(),
  };

  try {
    await axios.post(HERMES_INBOUND_URL, payload, { timeout: 5000 });
  } catch (err) {
    console.error('[hermes-wechaty] Failed to forward message:', err.message);
  }
});

bot.start().catch((err) => {
  console.error('[hermes-wechaty] Bot start error:', err);
  process.exit(1);
});

// ── Send API (port 3000) ─────────────────────────────────────────────────────
const sendApp = express();
sendApp.use(express.json());

sendApp.post('/send', async (req, res) => {
  const { to, text } = req.body;
  if (!to || !text) {
    return res.status(400).json({ error: 'to and text are required' });
  }
  try {
    const contact = await bot.Contact.find({ id: to });
    if (!contact) {
      return res.status(404).json({ error: `Contact not found: ${to}` });
    }
    await contact.say(text);
    res.json({ ok: true });
  } catch (err) {
    console.error('[hermes-wechaty] Send error:', err);
    res.status(500).json({ error: err.message });
  }
});

sendApp.listen(BRIDGE_PORT, () => {
  console.log(`[hermes-wechaty] Send API listening on port ${BRIDGE_PORT}`);
});

// ── QR code server (port 3001, for initial setup only) ───────────────────────
const qrApp = express();

qrApp.get('/qr', async (req, res) => {
  if (!currentQrData) {
    return res.send('<html><body><h2>Already logged in or QR not ready yet. Check back in a moment.</h2></body></html>');
  }
  const imgDataUrl = await QRCode.toDataURL(currentQrData);
  res.send(`
    <html><body style="font-family:sans-serif;text-align:center;padding:40px">
      <h2>Scan with WeChat to log in Hermes</h2>
      <img src="${imgDataUrl}" style="width:300px;height:300px"/>
      <p>Refresh this page if the QR code expires</p>
    </body></html>
  `);
});

qrApp.listen(QR_PORT, () => {
  console.log(`[hermes-wechaty] QR server listening on port ${QR_PORT}`);
});
