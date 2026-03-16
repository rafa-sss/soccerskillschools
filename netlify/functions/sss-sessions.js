const APPS_SCRIPT_WEBAPP_URL = "https://script.google.com/macros/s/AKfycbxAk8KJ3vDCCVd0Wf3DD7qnyLZWb-WtRD4y2D7hgdzxBGPnJTfFAxAPbhmCBkezY9LgYQ/exec";
const PROXY_SHARED_SECRET = process.env.SSS_PROXY_SECRET || "_005urNTXHuA8_gOIqS6QOYMAKwVPBE3gDwl1Ls_gBM";

function corsHeaders() {
  return {
    "Access-Control-Allow-Origin": "*",
    "Access-Control-Allow-Headers": "Content-Type",
    "Access-Control-Allow-Methods": "GET, POST, OPTIONS",
    "Content-Type": "application/json; charset=utf-8"
  };
}

function jsonResponse(statusCode, payload) {
  return {
    statusCode,
    headers: corsHeaders(),
    body: JSON.stringify(payload)
  };
}

function parseIncoming(event) {
  const method = String(event.httpMethod || 'GET').toUpperCase();
  if (method === 'GET') return event.queryStringParameters || {};
  if (!event.body) return {};

  try {
    return JSON.parse(event.body);
  } catch (err) {
    throw new Error('Invalid JSON body.');
  }
}

exports.handler = async function(event) {
  if (String(event.httpMethod || '').toUpperCase() === 'OPTIONS') {
    return {
      statusCode: 204,
      headers: corsHeaders(),
      body: ''
    };
  }

  try {
    const incoming = parseIncoming(event);
    const fn = String((incoming && incoming.fn) || '').trim();

    if (!fn) {
      return jsonResponse(400, { ok:false, error:'Missing fn.' });
    }

    const payload = { ...incoming, fn };
    if (fn !== 'getSharedSession') {
      payload.proxySecret = PROXY_SHARED_SECRET;
    }

    const upstreamRes = await fetch(APPS_SCRIPT_WEBAPP_URL, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(payload)
    });

    const text = await upstreamRes.text();
    let data = null;

    try {
      data = text ? JSON.parse(text) : null;
    } catch (err) {
      data = null;
    }

    if (!upstreamRes.ok) {
      return jsonResponse(502, {
        ok:false,
        error:(data && data.error) ? data.error : (text || ('Upstream request failed (' + upstreamRes.status + ').'))
      });
    }

    if (!data || typeof data !== 'object') {
      return jsonResponse(502, { ok:false, error:'Invalid response from Apps Script.' });
    }

    return jsonResponse(200, data);
  } catch (err) {
    return jsonResponse(500, {
      ok:false,
      error:(err && err.message) ? err.message : 'Netlify function error.'
    });
  };
};
