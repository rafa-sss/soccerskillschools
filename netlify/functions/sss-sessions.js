const APPS_SCRIPT_WEBAPP_URL = "https://script.google.com/macros/s/AKfycbwrMsZsXqMJzXv_UFYUkbmeM6-7rNLkP_wMQS0Se3nEk7qjdtS1HKyGIth2RxhM2Ro_lQ/exec";
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

function decodeBody(event) {
  if (!event || !event.body) return "";
  if (event.isBase64Encoded) {
    try {
      return Buffer.from(event.body, "base64").toString("utf8");
    } catch (e) {
      return "";
    }
  }
  return String(event.body || "");
}

function parseQueryFromEvent(event) {
  const out = {};

  if (event && event.queryStringParameters && typeof event.queryStringParameters === "object") {
    Object.assign(out, event.queryStringParameters);
  }

  const host =
    (event && event.headers && (event.headers["x-forwarded-host"] || event.headers.host)) ||
    "example.com";

  const proto =
    (event && event.headers && (event.headers["x-forwarded-proto"] || "https")) ||
    "https";

  const rawUrl =
    (event && event.rawUrl) ||
    `${proto}://${host}${event?.path || "/"}${event?.rawQuery ? `?${event.rawQuery}` : ""}`;

  try {
    const url = new URL(rawUrl);
    url.searchParams.forEach((value, key) => {
      out[key] = value;
    });
  } catch (e) {}

  return out;
}

function parseIncoming(event) {
  const method = String(
    event?.httpMethod ||
    event?.requestContext?.http?.method ||
    "GET"
  ).toUpperCase();

  if (method === "GET") {
    return parseQueryFromEvent(event);
  }

  const raw = decodeBody(event).trim();
  if (!raw) return {};

  try {
    return JSON.parse(raw);
  } catch (jsonErr) {
    try {
      const params = new URLSearchParams(raw);
      const out = {};
      for (const [k, v] of params.entries()) out[k] = v;
      return out;
    } catch (formErr) {
      throw new Error("Invalid request body.");
    }
  }
}

exports.handler = async function(event) {
  const method = String(
    event?.httpMethod ||
    event?.requestContext?.http?.method ||
    ""
  ).toUpperCase();

  if (method === "OPTIONS") {
    return {
      statusCode: 204,
      headers: corsHeaders(),
      body: ""
    };
  }

  try {
    const incoming = parseIncoming(event);
    const fn = String((incoming && incoming.fn) || "").trim();

    if (!fn) {
      return jsonResponse(400, {
        ok: false,
        error: "Missing fn.",
        debug: {
          method,
          queryStringParameters: event?.queryStringParameters || null,
          rawQuery: event?.rawQuery || "",
          rawUrl: event?.rawUrl || "",
          parsedKeys: Object.keys(incoming || {})
        }
      });
    }

    const payload = { ...incoming, fn };

    if (fn !== "getSharedSession") {
      payload.proxySecret = PROXY_SHARED_SECRET;
    }

    const upstreamRes = await fetch(APPS_SCRIPT_WEBAPP_URL, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "Accept": "application/json, text/plain, */*"
      },
      body: JSON.stringify(payload),
      redirect: "follow"
    });

    const contentType = upstreamRes.headers.get("content-type") || "";
    const text = await upstreamRes.text();

    let data = null;
    try {
      data = text ? JSON.parse(text) : null;
    } catch (e) {
      data = null;
    }

    if (!upstreamRes.ok) {
      return jsonResponse(502, {
        ok: false,
        error: (data && data.error) ? data.error : `Apps Script HTTP ${upstreamRes.status}`,
        upstreamContentType: contentType,
        upstreamPreview: (text || "").slice(0, 800)
      });
    }

    if (!data || typeof data !== "object") {
      return jsonResponse(502, {
        ok: false,
        error: "Invalid response from Apps Script.",
        upstreamContentType: contentType,
        upstreamPreview: (text || "").slice(0, 800)
      });
    }

    return jsonResponse(200, data);
  } catch (err) {
    return jsonResponse(500, {
      ok: false,
      error: (err && err.message) ? err.message : "Netlify function error."
    });
  }
};
