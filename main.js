// main.js - Full optimized script (ready to paste)
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

const auth = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6Imlra25uZ3JneHV4Z2pocGxicGV5Iiwicm9sZSI6ImFub24iLCJpYXQiOjE3MjU0MzgxNTAsImV4cCI6MjA0MTAxNDE1MH0.DRAvf8nH1ojnJBc3rD_Nw6t1AV8X_g6gmY_HByG2Mag";
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
  autoClaim: false,
};

/* -------------------------
   Logging helper
   ------------------------- */
function log(...args) {
  const msg = `[${new Date().toISOString()}] ${args.join(' ')}`;
  console.log(msg);
  try { fs.appendFileSync('run.log', msg + '\n'); } catch (e) {}
}

/* -------------------------
   Utility: safe localStorage
   ------------------------- */
async function getLocalStorage() {
  try {
    const raw = await readFileAsync(LOCAL_FILE, 'utf8');
    const parsed = JSON.parse(raw || '{}');
    return parsed;
  } catch (err) {
    return {};
  }
}

async function setLocalStorage(partial) {
  const current = await getLocalStorage();
  const merged = { ...current, ...partial };
  try {
    await writeFileAsync(LOCAL_FILE, JSON.stringify(merged, null, 2), 'utf8');
  } catch (err) {
    log('Failed to write localStorage:', err.message || err);
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
    log('No token provided to connectWebSocket.');
    return;
  }
  if (socket) {
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
    log('WebSocket creation failed:', err.message || err);
    scheduleReconnect(token, proxy);
    return;
  }

  socket.on('open', async () => {
    reconnectAttempts = 0;
    log('WebSocket connected at', new Date().toISOString());
    state.lastUpdated = new Date().toISOString();
    await setLocalStorage({ lastUpdated: state.lastUpdated });
    startPinging();
    startCountdownAndPoints();
  });

  socket.on('message', async (raw) => {
    try {
      const data = JSON.parse(raw);
      log('Received message from WebSocket:', JSON.stringify(data));
      if (data.pointsTotal !== undefined) state.pointsTotal = data.pointsTotal;
      if (data.pointsToday !== undefined) state.pointsToday = data.pointsToday;
      await setLocalStorage({
        pointsTotal: state.pointsTotal,
        pointsToday: state.pointsToday,
        lastUpdated: state.lastUpdated || new Date().toISOString()
      });
    } catch (err) {
      log('Failed to parse WS message:', err.message || err);
    }
  });

  socket.on('close', (code, reason) => {
    log('WebSocket disconnected', code, reason ? reason.toString() : '');
    stopPinging();
    socket = null;
    scheduleReconnect(token, proxy);
  });

  socket.on('error', (err) => {
    log('WebSocket error:', err.message || err);
  });
}

function scheduleReconnect(token, proxy) {
  if (reconnectTimer) return;
  reconnectAttempts = Math.min(reconnectAttempts + 1, 10);
  const delay = Math.min(1000 * 2 ** (reconnectAttempts - 1), 30000);
  log(`Reconnecting in ${Math.round(delay / 1000)}s... (attempt ${reconnectAttempts})`);
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
        log('Ping failed:', err.message || err);
      }
    }
  }, 10000);
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
    log(`Total Points: ${state.pointsTotal} | Today Points: ${state.pointsToday} | Countdown: ${state.countdown}`);
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

  await setLocalStorage({ potentialPoints: state.potentialPoints, countdown: state.countdown });
  log(`Total Points: ${state.pointsTotal} | Today Points: ${state.pointsToday} | Countdown: ${state.countdown}`);

  const storeAuto = (await getLocalStorage()).autoClaim;
  if (storeAuto && state.potentialPoints >= 25) {
    try {
      await attemptClaim(state.accessToken || (await getLocalStorage()).accessToken);
    } catch (err) {
      log('Auto-claim failed:', err.message || err);
    }
  }
}

/* -------------------------
   Claim function (optional)
   ------------------------- */
async function attemptClaim(token) {
  if (!token) return;
  const claimUrl = 'https://node-b.teneo.pro/api/claim'; // placeholder - replace if you have real endpoint
  try {
    const res = await axios.post(claimUrl, {}, {
      headers: { Authorization: `Bearer ${token}`, 'x-api-key': reffCode }
    });
    log('Claim response:', JSON.stringify(res.data));
    if (res.data && res.data.pointsTotal !== undefined) {
      state.pointsTotal = res.data.pointsTotal;
      state.pointsToday = res.data.pointsToday || state.pointsToday;
      await setLocalStorage({ pointsTotal: state.pointsTotal, pointsToday: state.pointsToday });
    }
  } catch (err) {
    log('Claim request error:', err.response ? JSON.stringify(err.response.data) : err.message || err);
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
    log('Login Error:', error.response ? JSON.stringify(error.response.data) : error.message || error);
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
      log('User already exists, please just login with:', email);
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

    log('Registration successful. Please confirm your email at:', email);
  } catch (error) {
    log('Error during registration:', error.response ? JSON.stringify(error.response.data) : error.message || error);
  }
}

/* -------------------------
   Main CLI flow
   ------------------------- */
async function main() {
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
      while (true) {
        const option = (await questionAsync(
          'User Token not found. Would you like to:\n1. Register an account\n2. Login to your account\n3. Enter Token manually\nChoose an option: '
        )).trim();

        if (option === '1') {
          await registerUser();
          break;
        } else if (option === '2') {
          await getUserId(proxy);
          break;
        } else if (option === '3') {
          const token = (await questionAsync('Please enter your access token: ')).trim();
          if (token) {
            state.accessToken = token;
            await setLocalStorage({ accessToken: token });
            await startCountdownAndPoints();
            await connectWebSocket(token, proxy);
            break;
          } else {
            log('Token cannot be empty. Please try again.');
          }
        } else {
          log('Invalid option. Please enter 1, 2, or 3.');
        }
      }
    } else {
      while (true) {
        const option = (await questionAsync('Menu:\n1. Logout\n2. Start Running Node\nChoose an option: ')).trim();

        if (option === '1') {
          try {
            fs.unlinkSync(LOCAL_FILE);
            log('Logged out successfully.');
          } catch (err) {
            log('Error deleting localStorage.json:', err.message || err);
          }
          process.exit(0);
        } else if (option === '2') {
          await startCountdownAndPoints();
          await connectWebSocket(state.accessToken, proxy);
          break;
        } else {
          log('Invalid option. Please enter 1 or 2.');
        }
      }
    }
  } catch (err) {
    log('Unexpected error in main:', err.message || err);
  } finally {
    // keep CLI open for WS events
  }
}

/* -------------------------
   Graceful shutdown
   ------------------------- */
process.on('SIGINT', () => {
  log('Received SIGINT. Stopping pinging and disconnecting WebSocket...');
  stopPinging();
  disconnectWebSocket();
  try { rl.close(); } catch (e) {}
  process.exit(0);
});

// run
main().catch(err => log('Startup error:', err.message || err));
