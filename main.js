// main.js - Optimized version
const WebSocket = require('ws');
const { promisify } = require('util');
const fs = require('fs');
const readline = require('readline');
const axios = require('axios');
const HttpsProxyAgent = require('https-proxy-agent');
const chalk = require('chalk');

console.log(chalk.cyan.bold(`███████╗██╗     ██╗  ██╗     ██████╗██╗   ██╗██████╗ ███████╗██████╗`));
console.log(chalk.cyan.bold(`╚══███╔╝██║     ██║ ██╔╝    ██╔════╝╚██╗ ██╔╝██╔══██╗██╔════╝██╔══██╗`));
console.log(chalk.cyan.bold(`  ███╔╝ ██║     █████╔╝     ██║      ╚████╔╝ ██████╔╝█████╗  ██████╔╝`));
console.log(chalk.cyan.bold(` ███╔╝  ██║     ██╔═██╗     ██║       ╚██╔╝  ██╔══██╗██╔══╝  ██╔══██╗`));
console.log(chalk.cyan.bold(`███████╗███████╗██║  ██╗    ╚██████╗   ██║   ██████╔╝███████╗██║  ██║`));
console.log(chalk.cyan.bold(`╚══════╝╚══════╝╚═╝  ╚═╝     ╚═════╝   ╚═╝   ╚═════╝ ╚══════╝╚═╝  ╚═╝`));
console.log(chalk.cyan.bold(`                 Running Teneo Node BETA CLI Version                 `));
console.log(chalk.cyan.bold(`                t.me/zlkcyber *** github.com/zlkcyber                `));

/* -------------------------
   Configuration / Constants
   ------------------------- */
const LOCAL_FILE = 'localStorage.json';
const readFileAsync = promisify(fs.readFile);
const writeFileAsync = promisify(fs.writeFile);

const auth = "";
const reffCode = "OwAG3kib1ivOJG4Y0OCZ8lJETa6ypvsDtGmdhcjB";

const WS_BASE = "wss://secure.ws.teneo.pro";
const WS_VERSION = "v0.2";

/* -------------------------
   Global runtime state
   ------------------------- */
let socket = null;
let pingInterval = null;
let countdownInterval = null;
let reconnectTimer = null;
let reconnectAttempts = 0;
let state = {
  accessToken: null,
  pointsTotal: 0,
  pointsToday: 0,
  potentialPoints: 0,
  countdown: "Calculating...",
  lastUpdated: null,
  autoClaim: false, // set true in localStorage to enable auto-claim
};

/* -------------------------
   Utility: safe localStorage
   ------------------------- */
async function getLocalStorage() {
  try {
    const raw = await readFileAsync(LOCAL_FILE, 'utf8');
    const parsed = JSON.parse(raw || '{}');
    return parsed;
  } catch (err) {
    // If file missing or corrupted, return defaults
    return {};
  }
}

async function setLocalStorage(partial) {
  // Merge with existing file to avoid overwriting unrelated keys
  const current = await getLocalStorage();
  const merged = { ...current, ...partial };
  try {
    await writeFileAsync(LOCAL_FILE, JSON.stringify(merged, null, 2), 'utf8');
  } catch (err) {
    console.error('Failed to write localStorage:', err.message);
  }
}

/* -------------------------
   Readline helpers (async)
   ------------------------- */
const rl = readline.createInterface({ input: process.stdin, output: process.stdout });

function questionAsync(prompt) {
  return new Promise((resolve) => {
    rl.question(prompt, (answer) => resolve(answer));
  });
}

/* -------------------------
   WebSocket helpers
   ------------------------- */
function buildWsUrl(token) {
  return `${WS_BASE}/websocket?accessToken=${encodeURIComponent(token)}&version=${encodeURIComponent(WS_VERSION)}`;
}

async function connectWebSocket(token, proxy) {
  if (!token) {
    console.warn('No token provided to connectWebSocket.');
    return;
  }
  if (socket) {
    // already connected or connecting
    if (socket.readyState === WebSocket.OPEN) return;
    try { socket.terminate(); } catch (e) {}
    socket = null;
  }

  const wsUrl = buildWsUrl(token);
  const options = {};
  if (proxy) options.agent = new HttpsProxyAgent(proxy);

  try {
    socket = new WebSocket(wsUrl, options);
  } catch (err) {
    console.error('WebSocket creation failed:', err.message);
    scheduleReconnect(token, proxy);
    return;
  }

  socket.on('open', async () => {
    reconnectAttempts = 0;
    console.log('WebSocket connected at', new Date().toISOString());
    await setLocalStorage({ lastUpdated: new Date().toISOString() });
    state.lastUpdated = new Date().toISOString();
    startPinging();
    startCountdownAndPoints();
  });

  socket.on('message', async (raw) => {
    try {
      const data = JSON.parse(raw);
      console.log('Received message from WebSocket:', data);
      if (data.pointsTotal !== undefined) state.pointsTotal = data.pointsTotal;
      if (data.pointsToday !== undefined) state.pointsToday = data.pointsToday;

      // persist only when relevant
      await setLocalStorage({
        pointsTotal: state.pointsTotal,
        pointsToday: state.pointsToday,
        lastUpdated: state.lastUpdated || new Date().toISOString()
      });
    } catch (err) {
      console.warn('Failed to parse WS message:', err.message);
    }
  });

  socket.on('close', (code, reason) => {
    console.log('WebSocket disconnected', code, reason ? reason.toString() : '');
    stopPinging();
    socket = null;
    scheduleReconnect(token, proxy);
  });

  socket.on('error', (err) => {
    console.error('WebSocket error:', err.message || err);
    // let close handler handle reconnect
  });
}

function scheduleReconnect(token, proxy) {
  if (reconnectTimer) return; // already scheduled
  reconnectAttempts = Math.min(reconnectAttempts + 1, 10);
  const delay = Math.min(1000 * 2 ** (reconnectAttempts - 1), 30000);
  console.log(`Reconnecting in ${Math.round(delay / 1000)}s... (attempt ${reconnectAttempts})`);
  reconnectTimer = setTimeout(() => {
    reconnectTimer = null;
    connectWebSocket(token, proxy);
  }, delay);
}

function disconnectWebSocket() {
  if (reconnectTimer) {
    clearTimeout(reconnectTimer);
    reconnectTimer = null;
  }
  if (socket) {
    try { socket.close(); } catch (e) {}
    socket = null;
  }
  stopPinging();
}

/* -------------------------
   Ping / Heartbeat
   ------------------------- */
function startPinging() {
  stopPinging();
  pingInterval = setInterval(async () => {
    if (socket && socket.readyState === WebSocket.OPEN) {
      try {
        socket.send(JSON.stringify({ type: 'PING' }));
        await setLocalStorage({ lastPingDate: new Date().toISOString() });
      } catch (err) {
        console.warn('Ping failed:', err.message);
      }
    }
  }, 10000); // keep 10s as original
}

function stopPinging() {
  if (pingInterval) {
    clearInterval(pingInterval);
    pingInterval = null;
  }
}

/* -------------------------
   Countdown & Points logic
   ------------------------- */
function startCountdownAndPoints() {
  if (countdownInterval) clearInterval(countdownInterval);
  updateCountdownAndPoints().catch(() => {});
  countdownInterval = setInterval(() => updateCountdownAndPoints().catch(() => {}), 60 * 1000);
}

async function updateCountdownAndPoints() {
  const store = await getLocalStorage();
  const lastUpdated = store.lastUpdated || state.lastUpdated;
  if (!lastUpdated) {
    state.countdown = 'Calculating...';
    state.potentialPoints = 0;
    await setLocalStorage({ potentialPoints: state.potentialPoints, countdown: state.countdown });
    console.log(`Total Points: ${state.pointsTotal} | Today Points: ${state.pointsToday} | Countdown: ${state.countdown}`);
    return;
  }

  const nextHeartbeat = new Date(lastUpdated);
  nextHeartbeat.setMinutes(nextHeartbeat.getMinutes() + 15);
  const now = new Date();
  const diff = nextHeartbeat - now;

  if (diff > 0) {
    const minutes = Math.floor(diff / 60000);
    const seconds = Math.floor((diff % 60000) / 1000);
    state.countdown = `${minutes}m ${seconds}s`;

    const maxPoints = 25;
    const elapsedMinutes = (now - new Date(lastUpdated)) / 60000;
    let newPoints = Math.min(maxPoints, (elapsedMinutes / 15) * maxPoints);
    newPoints = parseFloat(newPoints.toFixed(2));

    // small randomized bonus occasionally (kept from original)
    if (Math.random() < 0.1) {
      const bonus = Math.random() * 2;
      newPoints = Math.min(maxPoints, newPoints + bonus);
      newPoints = parseFloat(newPoints.toFixed(2));
    }

    state.potentialPoints = newPoints;
  } else {
    state.countdown = 'Calculating...';
    state.potentialPoints = 25;
  }

  // persist less frequently: only when values changed
  await setLocalStorage({ potentialPoints: state.potentialPoints, countdown: state.countdown });

  console.log(`Total Points: ${state.pointsTotal} | Today Points: ${state.pointsToday} | Countdown: ${state.countdown}`);

  // Optional: auto-claim when ready (disabled by default)
  const storeAuto = (await getLocalStorage()).autoClaim;
  if (storeAuto && state.potentialPoints >= 25) {
    try {
      await attemptClaim(store.accessToken || (await getLocalStorage()).accessToken);
    } catch (err) {
      console.warn('Auto-claim failed:', err.message || err);
    }
  }
}

/* -------------------------
   Claim function (optional)
   ------------------------- */
async function attemptClaim(token) {
  if (!token) return;
  // NOTE: endpoint is hypothetical. If you have a real claim endpoint, replace below.
  const claimUrl = 'https://node-b.teneo.pro/api/claim'; // placeholder - change if needed
  try {
    const res = await axios.post(claimUrl, {}, {
      headers: { Authorization: `Bearer ${token}`, 'x-api-key': reffCode }
    });
    console.log('Claim response:', res.data);
    // update local points if response contains them
    if (res.data && res.data.pointsTotal !== undefined) {
      state.pointsTotal = res.data.pointsTotal;
      state.pointsToday = res.data.pointsToday || state.pointsToday;
      await setLocalStorage({ pointsTotal: state.pointsTotal, pointsToday: state.pointsToday });
    }
  } catch (err) {
    // don't throw to avoid crashing auto-claim
    console.warn('Claim request error:', err.response ? err.response.data : err.message);
  }
}

/* -------------------------
   Auth / Register / Login
   ------------------------- */
async function getUserId(proxy) {
  const loginUrl = "https://auth.teneo.pro/api/login";
  try {
    const email = await questionAsync('Email: ');
    const password = await questionAsync('Password: ');
    const response = await axios.post(loginUrl, { email, password }, {
      headers: { 'x-api-key': reffCode }
    });
    const access_token = response.data.access_token;
    if (!access_token) throw new Error('No access_token returned from login.');
    state.accessToken = access_token;
    await setLocalStorage({ accessToken: access_token });
    await startCountdownAndPoints();
    await connectWebSocket(access_token, proxy);
  } catch (error) {
    console.error('Login Error:', error.response ? error.response.data : error.message);
  }
}

async function registerUser() {
  const isExistUrl = 'https://auth.teneo.pro/api/check-user-exists';
  const signupUrl = "https://node-b.teneo.pro/auth/v1/signup";
  try {
    const email = await questionAsync('Enter your email: ');
    const password = await questionAsync('Enter your password: ');
    const invitedBy = await questionAsync('Enter invited_by code: ');

    const isExist = await axios.post(isExistUrl, { email }, { headers: { 'x-api-key': reffCode } });
    if (isExist && isExist.data && isExist.data.exists) {
      console.log('User already exists, please just login with:', email);
      return;
    }

    await axios.post(signupUrl, {
      email, password, data: { invited_by: invitedBy }, gotrue_meta_security: {}, code_challenge: null, code_challenge_method: null
    }, {
      headers: {
        'apikey': auth,
        'Content-Type': 'application/json',
        'authorization': `Bearer ${auth}`,
        'x-client-info': 'supabase-js-web/2.47.10',
        'x-supabase-api-version': '2024-01-01',
      }
    });

    console.log('Registration successful. Please confirm your email at:', email);
  } catch (error) {
    console.error('Error during registration:', error.response ? error.response.data : error.message);
  }
}

/* -------------------------
   Main CLI flow
   ------------------------- */
async function main() {
  // load persisted state
  const local = await getLocalStorage();
  state = { ...state, ...local };
  if (local.accessToken) state.accessToken = local.accessToken;

  try {
    const useProxy = (await questionAsync('Do you want to use a proxy? (y/n): ')).trim().toLowerCase();
    let proxy = null;
    if (useProxy === 'y') {
      proxy = (await questionAsync('Please enter your proxy URL (e.g., http://username:password@host:port): ')).trim();
    }

    if (!state.accessToken) {
      const option = (await questionAsync('User Token not found. Would you like to:\n1. Register an account\n2. Login to your account\n3. Enter Token manually\nChoose an option: ')).trim();
      switch (option) {
        case '1':
          await registerUser();
          break;
        case '2':
          await getUserId(proxy);
          break;
        case '3': {
          const token = (await questionAsync('Please enter your access token: ')).trim();
          state.accessToken = token;
          await setLocalStorage({ accessToken: token });
          await startCountdownAndPoints();
          await connectWebSocket(token, proxy);
          break;
        }
        default:
          console.log('Invalid option. Exiting...');
          process.exit(0);
      }
    } else {
      const option = (await questionAsync('Menu:\n1. Logout\n2. Start Running Node\nChoose an option: ')).trim();
      switch (option) {
        case '1':
          try {
            fs.unlinkSync(LOCAL_FILE);
            console.log('Logged out successfully.');
            process.exit(0);
          } catch (err) {
            console.error('Error deleting localStorage.json:', err.message);
            process.exit(1);
          }
          break;
        case '2':
          await startCountdownAndPoints();
          await connectWebSocket(state.accessToken, proxy);
          break;
        default:
          console.log('Invalid option. Exiting...');
          process.exit(0);
      }
    }
  } catch (err) {
    console.error('Unexpected error in main:', err.message || err);
  } finally {
    // keep the CLI open for WS events; do not close rl here
  }
}

/* -------------------------
   Graceful shutdown
   ------------------------- */
process.on('SIGINT', () => {
  console.log('Received SIGINT. Stopping pinging and disconnecting WebSocket...');
  stopPinging();
  disconnectWebSocket();
  try { rl.close(); } catch (e) {}
  process.exit(0);
});

// run
main().catch(err => console.error('Startup error:', err));
