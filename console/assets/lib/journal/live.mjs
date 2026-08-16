// Live WebSocket client for the journal feed. Connects to /feed/ws, forwards
// event messages to a callback, and tells the loader worker when a file rolls.
// Auto-reconnects with exponential backoff. Exposes wsConnected state.

const BASE_DELAY_MS = 500;
const MAX_DELAY_MS = 30000;

/**
 * @param {object} opts
 * @param {(event: object) => void} opts.onEvent - called for each live event
 * @param {(rolled: object) => void} opts.onRolled - called when a file rolls
 * @param {(connected: boolean) => void} opts.onStatus - connection status
 */
export function connectJournalWs({ onEvent, onRolled, onStatus }) {
  let ws = null;
  let delay = BASE_DELAY_MS;
  let closed = false;

  function url() {
    const proto = location.protocol === "https:" ? "wss:" : "ws:";
    return `${proto}//${location.host}/feed/ws`;
  }

  function open() {
    if (closed) return;
    try {
      ws = new WebSocket(url());
    } catch (_) {
      scheduleReconnect();
      return;
    }

    ws.onopen = () => {
      delay = BASE_DELAY_MS;
      onStatus(true);
    };

    ws.onmessage = (e) => {
      let msg;
      try {
        msg = JSON.parse(e.data);
      } catch (_) {
        return;
      }
      if (msg.type === "event") {
        onEvent(msg);
      } else if (msg.type === "rolled") {
        onRolled(msg);
      }
    };

    ws.onerror = () => {};

    ws.onclose = () => {
      onStatus(false);
      scheduleReconnect();
    };
  }

  function scheduleReconnect() {
    if (closed) return;
    setTimeout(() => {
      delay = Math.min(delay * 2, MAX_DELAY_MS);
      open();
    }, delay);
  }

  open();

  return {
    close() {
      closed = true;
      if (ws) {
        try { ws.close(); } catch (_) {}
      }
    },
  };
}
