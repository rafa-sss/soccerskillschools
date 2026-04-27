/**
 * SSS Sessions API (READABLE SESSIONS SHEET + STYLING)
 * - Magic link login
 * - Auto-create users on first login
 * - Save/List/Update/Delete session plans
 * - Public read endpoint for shared viewer links by sessionId
 * - Debug endpoint
 * - Uses LockService for safer concurrent writes
 *
 * Deploy as Web App:
 *   Execute as: Me
 *   Who has access: Anyone (or Anyone with link)
 */

// ===================== CONFIG =====================
const SPREADSHEET_ID = "1HSPyHxZH2jwg8MkapD090fopZJYHvSmjVEi23rJrs4Y";
const DEFAULT_MAGIC_LINK_BASE_URL = "https://www.soccerskillschools.com/staging-test";
const ALLOWED_MAGIC_LINK_RETURN_HOSTS = ["www.soccerskillschools.com", "soccerskillschools.com"];

const MAGIC_TOKEN_TTL_MIN = 15;
const SESSION_TOKEN_TTL_DAYS = 90;
const DEFAULT_CALLBACK = "cb";
const LOCK_TIMEOUT_MS = 10000;
const PROXY_SHARED_SECRET = "_005urNTXHuA8_gOIqS6QOYMAKwVPBE3gDwl1Ls_gBM";

const USERS_HEADERS = ["email","userId","createdAt","lastLoginAt","sessionTokenHash","sessionTokenExpiresAt"];
const LOGIN_TOKENS_HEADERS = ["tokenHash","email","expiresAt","usedAt","createdAt"];
const SESSIONS_HEADERS = [
  "sessionId",
  "userEmail",
  "userId",
  "title",
  "tags",
  "folder",
  "visibility",
  "sessionDate",
  "coach",
  "ageGroup",
  "theme",
  "sessionDrills",
  "notesPreview",
  "payload",
  "createdAt",
  "updatedAt"
];


// ===================== APPROVED MEMBERS SYNC =====================
const APPROVED_MEMBERS_SHEET = "approved_members";
const SQUARESPACE_MEMBERS_RAW_SHEET = "squarespace_orders_raw";
const SYNC_LOG_SHEET = "sync_log";
const AUTH_AUDIT_SHEET = "auth_audit";

// Update these to the exact Squarespace Member Area names you want to allow.
const APPROVED_MEMBER_AREAS = [
  "Staging Test"
];

// Optional manual overrides for your own/admin/coach emails
const MANUAL_ACTIVE_EMAILS = [
  // "you@example.com",
  // "coach@example.com"
];

function setupApprovedMembersSheets_() {
  const ss = SpreadsheetApp.openById(SPREADSHEET_ID);

  ensureSheet_(ss, APPROVED_MEMBERS_SHEET, [
    "email",
    "status",
    "access_source",
    "plan_name",
    "squarespace_customer_id",
    "order_id",
    "access_start",
    "access_end",
    "last_synced_at",
    "notes"
  ]);

  ensureSheet_(ss, SQUARESPACE_MEMBERS_RAW_SHEET, [
    "Email",
    "First Name",
    "Last Name",
    "Created On",
    "Order Count",
    "Last Order Date",
    "Total Spent",
    "Member Since",
    "Subscriber Since",
    "Subscriber Source",
    "Tags",
    "Mailing Lists",
    "Member Areas",
    "Donation Count",
    "Last Donation Date",
    "Total Donation Amount",
    "Has Account",
    "Customer Since",
    "Shipping Name",
    "Shipping Address 1",
    "Shipping Address 2",
    "Shipping City",
    "Shipping Zip",
    "Shipping Province/State",
    "Shipping Country",
    "Shipping Phone Number",
    "Billing Name",
    "Billing Address 1",
    "Billing Address 2",
    "Billing City",
    "Billing Zip",
    "Billing Province/State",
    "Billing Country",
    "Billing Phone Number",
    "Accepts Marketing",
    "Appointment Count"
  ]);

  ensureSheet_(ss, SYNC_LOG_SHEET, [
    "timestamp",
    "action",
    "result",
    "details"
  ]);

  ensureSheet_(ss, AUTH_AUDIT_SHEET, [
    "timestamp",
    "action",
    "email",
    "result",
    "reason"
  ]);

  logSyncEvent_("setupApprovedMembersSheets_", "ok", "Approved-members sheets ensured");
}

function installApprovedMembersSyncTrigger_() {
  const fnName = "syncApprovedMembersFromSquarespaceRaw_";

  ScriptApp.getProjectTriggers()
    .filter(t => t.getHandlerFunction() === fnName)
    .forEach(t => ScriptApp.deleteTrigger(t));

  ScriptApp.newTrigger(fnName)
    .timeBased()
    .everyHours(1)
    .create();

  logSyncEvent_("installApprovedMembersSyncTrigger_", "ok", "Hourly sync trigger created");
}

function syncApprovedMembersFromSquarespaceRaw_() {
  const ss = SpreadsheetApp.openById(SPREADSHEET_ID);
  const rawSh = ensureSheet_(ss, SQUARESPACE_MEMBERS_RAW_SHEET, []);
  const approvedSh = ensureSheet_(ss, APPROVED_MEMBERS_SHEET, [
    "email",
    "status",
    "access_source",
    "plan_name",
    "squarespace_customer_id",
    "order_id",
    "access_start",
    "access_end",
    "last_synced_at",
    "notes"
  ]);

  const rawValues = rawSh.getDataRange().getValues();
  if (rawValues.length < 2) {
    logSyncEvent_("syncApprovedMembersFromSquarespaceRaw_", "ok", "No member rows found in raw sheet");
    syncManualActiveEmails_();
    return;
  }

  const headers = rawValues[0].map(h => String(h || "").trim());
  const rows = rawValues.slice(1);
  const approvedValues = approvedSh.getDataRange().getValues();
  const approvedHeaders = approvedValues[0].map(h => String(h || "").trim());
  const approvedMap = buildApprovedMembersMap_(approvedValues);
  const now = new Date();

  let upserts = 0;
  let skipped = 0;

  rows.forEach(function(row) {
    const record = rowToObject_(headers, row);
    const email = normalizeEmail_(record["Email"] || "");
    if (!email) {
      skipped++;
      return;
    }

    const memberAreasRaw = String(record["Member Areas"] || "").trim();
    const planName = memberAreasRaw;
    const hasMatchingArea = memberHasApprovedArea_(memberAreasRaw);

    // First safe version: only activate rows that clearly match an approved member area.
    // We are not deactivating anyone automatically yet.
    if (!hasMatchingArea) {
      skipped++;
      return;
    }

    const existing = approvedMap[email] || null;

    const next = {
      email: email,
      status: "active",
      access_source: "squarespace_members_export",
      plan_name: planName,
      squarespace_customer_id: "",
      order_id: "",
      access_start: record["Member Since"] || record["Subscriber Since"] || record["Created On"] || "",
      access_end: existing && existing.access_end ? existing.access_end : "",
      last_synced_at: now,
      notes: buildMemberNotes_(record, existing)
    };

    upsertApprovedMember_(approvedSh, approvedHeaders, approvedMap, next);
    upserts++;
  });

  syncManualActiveEmails_();

  logSyncEvent_(
    "syncApprovedMembersFromSquarespaceRaw_",
    "ok",
    "Upserts: " + upserts + ", skipped: " + skipped
  );
}

function syncManualActiveEmails_() {
  const ss = SpreadsheetApp.openById(SPREADSHEET_ID);
  const approvedSh = ensureSheet_(ss, APPROVED_MEMBERS_SHEET, [
    "email",
    "status",
    "access_source",
    "plan_name",
    "squarespace_customer_id",
    "order_id",
    "access_start",
    "access_end",
    "last_synced_at",
    "notes"
  ]);

  const values = approvedSh.getDataRange().getValues();
  const headers = values[0].map(h => String(h || "").trim());
  const approvedMap = buildApprovedMembersMap_(values);
  const now = new Date();

  MANUAL_ACTIVE_EMAILS
    .map(normalizeEmail_)
    .filter(Boolean)
    .forEach(function(email) {
      upsertApprovedMember_(approvedSh, headers, approvedMap, {
        email: email,
        status: "active",
        access_source: "manual",
        plan_name: "Manual Override",
        squarespace_customer_id: "",
        order_id: "",
        access_start: "",
        access_end: "",
        last_synced_at: now,
        notes: "Manual access"
      });
    });
}

function memberHasApprovedArea_(memberAreasRaw) {
  const areas = splitMultiValue_(memberAreasRaw);
  if (!areas.length) return false;

  const normalizedAllowed = APPROVED_MEMBER_AREAS.map(function(v) {
    return String(v || "").trim().toLowerCase();
  }).filter(Boolean);

  return areas.some(function(area) {
    const a = String(area || "").trim().toLowerCase();
    return normalizedAllowed.some(function(allowed) {
      return a.indexOf(allowed) !== -1;
    });
  });
}

function splitMultiValue_(value) {
  const raw = String(value || "").trim();
  if (!raw) return [];

  return raw
    .split(/\n|,|;/)
    .map(function(v) { return String(v || "").trim(); })
    .filter(Boolean);
}

function rowToObject_(headers, row) {
  const out = {};
  headers.forEach(function(h, i) {
    out[String(h || "").trim()] = row[i];
  });
  return out;
}

function buildApprovedMembersMap_(values) {
  const map = {};
  if (!values || values.length < 2) return map;

  const headers = values[0].map(h => String(h || "").trim());
  const emailIdx = headers.indexOf("email");
  if (emailIdx === -1) return map;

  for (let i = 1; i < values.length; i++) {
    const row = values[i];
    const email = normalizeEmail_(row[emailIdx] || "");
    if (!email) continue;

    map[email] = {
      rowIndex: i + 1,
      email: email,
      status: getCellByHeader_(headers, row, "status"),
      access_source: getCellByHeader_(headers, row, "access_source"),
      plan_name: getCellByHeader_(headers, row, "plan_name"),
      squarespace_customer_id: getCellByHeader_(headers, row, "squarespace_customer_id"),
      order_id: getCellByHeader_(headers, row, "order_id"),
      access_start: getCellByHeader_(headers, row, "access_start"),
      access_end: getCellByHeader_(headers, row, "access_end"),
      last_synced_at: getCellByHeader_(headers, row, "last_synced_at"),
      notes: getCellByHeader_(headers, row, "notes")
    };
  }

  return map;
}

function upsertApprovedMember_(sheet, headers, map, member) {
  const rowValues = headers.map(function(h) {
    return member[h] !== undefined ? member[h] : "";
  });

  if (map[member.email] && map[member.email].rowIndex) {
    sheet.getRange(map[member.email].rowIndex, 1, 1, headers.length).setValues([rowValues]);
    map[member.email] = Object.assign({ rowIndex: map[member.email].rowIndex }, member);
  } else {
    sheet.appendRow(rowValues);
    map[member.email] = Object.assign({ rowIndex: sheet.getLastRow() }, member);
  }
}

function getCellByHeader_(headers, row, headerName) {
  const idx = headers.indexOf(headerName);
  return idx === -1 ? "" : row[idx];
}

function buildMemberNotes_(record, existing) {
  const bits = [];

  if (record["First Name"] || record["Last Name"]) {
    bits.push("Name: " + [record["First Name"], record["Last Name"]].filter(Boolean).join(" "));
  }
  if (record["Has Account"] !== "" && record["Has Account"] != null) {
    bits.push("Has Account: " + record["Has Account"]);
  }
  if (record["Subscriber Source"]) {
    bits.push("Subscriber Source: " + record["Subscriber Source"]);
  }
  if (record["Tags"]) {
    bits.push("Tags: " + record["Tags"]);
  }

  const note = bits.join(" | ");
  return note || (existing && existing.notes ? existing.notes : "");
}

function logSyncEvent_(action, result, details) {
  const ss = SpreadsheetApp.openById(SPREADSHEET_ID);
  const sh = ensureSheet_(ss, SYNC_LOG_SHEET, [
    "timestamp",
    "action",
    "result",
    "details"
  ]);
  sh.appendRow([new Date(), action, result, details || ""]);
}

function logAuthAudit_(action, email, result, reason) {
  const ss = SpreadsheetApp.openById(SPREADSHEET_ID);
  const sh = ensureSheet_(ss, AUTH_AUDIT_SHEET, [
    "timestamp",
    "action",
    "email",
    "result",
    "reason"
  ]);
  sh.appendRow([new Date(), normalizeEmail_(email || ""), result || "", reason || ""]);
}

function hasActiveApprovedMember_(email) {
  const normalized = normalizeEmail_(email || "");
  if (!normalized) return false;

  const ss = SpreadsheetApp.openById(SPREADSHEET_ID);
  const sh = ensureSheet_(ss, APPROVED_MEMBERS_SHEET, [
    "email",
    "status",
    "access_source",
    "plan_name",
    "squarespace_customer_id",
    "order_id",
    "access_start",
    "access_end",
    "last_synced_at",
    "notes"
  ]);

  const values = sh.getDataRange().getValues();
  if (values.length < 2) return false;

  const headers = values[0].map(h => String(h || "").trim());
  const emailIdx = headers.indexOf("email");
  const statusIdx = headers.indexOf("status");
  const startIdx = headers.indexOf("access_start");
  const endIdx = headers.indexOf("access_end");

  if (emailIdx === -1 || statusIdx === -1) return false;

  const now = new Date();

  for (let i = 1; i < values.length; i++) {
    const row = values[i];
    const rowEmail = normalizeEmail_(row[emailIdx] || "");
    if (rowEmail !== normalized) continue;

    const status = String(row[statusIdx] || "").trim().toLowerCase();
    if (status !== "active") return false;

    const startVal = startIdx >= 0 ? row[startIdx] : "";
    const endVal = endIdx >= 0 ? row[endIdx] : "";

    const startDate = coerceToDateValue_(startVal);
    const endDate = coerceToDateValue_(endVal);

    if (startDate && now < startDate) return false;
    if (endDate && now > endDate) return false;

    return true;
  }

  return false;
}

// ===================== ROUTER =====================
function doGet(e) {
  const p = (e && e.parameter) ? e.parameter : {};
  const callback = (p.callback || "").trim();

  try {
    const result = routeRequest_(p);
    return callback ? jsonp_(callback, result) : json_(result);
  } catch (err) {
    const payload = {
      ok: false,
      error: (err && err.message) ? err.message : String(err)
    };
    return callback ? jsonp_(callback, payload) : json_(payload);
  }
}

function doPost(e) {
  try {
    const p = parsePostBody_(e);
    const result = routeRequest_(p);
    return json_(result);
  } catch (err) {
    return json_({
      ok: false,
      error: (err && err.message) ? err.message : String(err)
    });
  }
}

function routeRequest_(p) {
  p = p || {};
  const fn = String(p.fn || "").trim();

  const secretCheck = requireProxySecretForFn_(fn, p);
  if (!secretCheck.ok) return secretCheck;

  switch (fn) {
    case "debug":
      return api_debug_(p);
    case "requestMagicLink":
      return api_requestMagicLink_(p);
    case "verifyMagic":
      return api_verifyMagic_(p);
    case "me":
      return api_me_(p);
    case "listSessions":
      return api_listSessions_(p);
    case "saveSession":
      return api_saveSession_(p);
    case "updateSession":
      return api_updateSession_(p);
    case "deleteSession":
      return api_deleteSession_(p);
    case "getSharedSession":
      return api_getSharedSession_(p);
    default:
      return { ok: false, error: "Missing or unknown fn" };
  }
}

// ===================== ONE-TIME AUTHORIZATION HELPER =====================
function authorizeAll() {
  const ss = SpreadsheetApp.openById(SPREADSHEET_ID);
  Logger.log(ss.getName());
  MailApp.getRemainingDailyQuota();
  Logger.log("Authorized Sheets + Mail");
}

// Run this manually after pasting the script if you want to refresh all existing rows.
function rebuildSessionsSheetView() {
  const ss = SpreadsheetApp.openById(SPREADSHEET_ID);
  const sh = ensureSessionsSheet_(ss);
  const values = sh.getDataRange().getValues();
  const map = getHeaderMap_(values[0] || []);

  if (values.length <= 1) {
    applySessionsSheetStyle_(sh);
    styleAllWorkbookSheets_();
    return;
  }

  const rebuilt = [];
  for (let i = 1; i < values.length; i++) {
    const rowObj = getSessionObjectFromRow_(values[i], map);
    const built = buildSessionRow_({
      sessionId: rowObj.sessionId,
      userEmail: rowObj.userEmail,
      userId: rowObj.userId,
      title: rowObj.title,
      tags: rowObj.tags,
      folder: rowObj.folder,
      visibility: rowObj.visibility,
      payload: rowObj.payload,
      createdAt: rowObj.createdAt,
      updatedAt: rowObj.updatedAt
    });
    rebuilt.push(built);
  }

  if (sh.getMaxRows() > 1) {
    sh.getRange(2, 1, sh.getMaxRows() - 1, SESSIONS_HEADERS.length).clearContent();
  }
  if (rebuilt.length) {
    sh.getRange(2, 1, rebuilt.length, SESSIONS_HEADERS.length).setValues(rebuilt);
  }

  trimExtraRows_(sh, rebuilt.length + 1);
  applySessionsSheetStyle_(sh);
  styleAllWorkbookSheets_();
}

// ===================== DEBUG =====================
function api_debug_() {
  const effectiveUser = Session.getEffectiveUser().getEmail();
  const activeUser = Session.getActiveUser().getEmail();

  return {
    ok: true,
    activeUser,
    effectiveUser,
    spreadsheetId: SPREADSHEET_ID
  };
}

// ===================== AUTH: MAGIC LINK =====================
function api_requestMagicLink_(p) {
  const email = normalizeEmail_(p.email || "");
  if (!email) return { ok: false, error: "Missing email" };

  const rawToken = randomToken_(48);
  const tokenHash = sha256_(rawToken);

  const now = new Date();
  const expiresAt = new Date(now.getTime() + MAGIC_TOKEN_TTL_MIN * 60 * 1000);

  withScriptLock_(function () {
    const ss = SpreadsheetApp.openById(SPREADSHEET_ID);
    const sh = ensureSheet_(ss, "login_tokens", LOGIN_TOKENS_HEADERS);
    sh.appendRow([tokenHash, email, expiresAt, "", now]);
    beautifyLoginTokensSheet_(sh);
  });

  const returnTo = normalizeMagicReturnTo_(p.returnTo || DEFAULT_MAGIC_LINK_BASE_URL);
  const url = appendQueryParam_(returnTo, "magic", rawToken);

  const subject = "Your Soccer Skill Schools login link";
  const body =
    "Use this link to log in:\n\n" +
    url + "\n\n" +
    "This link expires in " + MAGIC_TOKEN_TTL_MIN + " minutes.\n\n" +
    "If you didn't request this, you can ignore this email.";

  MailApp.sendEmail({
    to: email,
    subject: subject,
    body: body,
    name: "Soccer Skill Schools",
    replyTo: "info@soccerskillschools.com"
  });

  return { ok: true, message: "Magic link sent" };
}

function api_verifyMagic_(p) {
  const rawToken = String(p.token || "").trim();
  if (!rawToken) return { ok: false, error: "Missing token" };

  return withScriptLock_(function () {
    const tokenHash = sha256_(rawToken);
    const now = new Date();

    const ss = SpreadsheetApp.openById(SPREADSHEET_ID);
    const tokensSh = ensureSheet_(ss, "login_tokens", LOGIN_TOKENS_HEADERS);
    const usersSh = ensureSheet_(ss, "users", USERS_HEADERS);

    const tokens = tokensSh.getDataRange().getValues();

    let rowIndex = -1;
    let email = "";
    let expiresAtIso = "";
    let usedAtIso = "";

    for (let i = 1; i < tokens.length; i++) {
      if (String(tokens[i][0] || "") === tokenHash) {
        rowIndex = i + 1;
        email = normalizeEmail_(tokens[i][1] || "");
        expiresAtIso = String(tokens[i][2] || "");
        usedAtIso = String(tokens[i][3] || "");
        break;
      }
    }

    if (rowIndex === -1) return { ok: false, error: "Invalid token" };
    if (usedAtIso) return { ok: false, error: "Token already used" };

    const exp = new Date(expiresAtIso);
    if (!expiresAtIso || isNaN(exp.getTime()) || now > exp) {
      return { ok: false, error: "Token expired" };
    }

    tokensSh.getRange(rowIndex, 4).setValue(now);

    const users = usersSh.getDataRange().getValues();
    let userRow = -1;
    let userId = "";

    for (let i = 1; i < users.length; i++) {
      if (normalizeEmail_(users[i][0] || "") === email) {
        userRow = i + 1;
        userId = String(users[i][1] || "");
        break;
      }
    }

    if (userRow === -1) {
      userId = Utilities.getUuid();
      usersSh.appendRow([email, userId, now, now, "", ""]);
      userRow = usersSh.getLastRow();
    } else {
      usersSh.getRange(userRow, 4).setValue(now);
    }

    const sessionToken = randomToken_(64);
    const sessionTokenHash = sha256_(sessionToken);
    const sessionExpiresAt = new Date(now.getTime() + SESSION_TOKEN_TTL_DAYS * 24 * 60 * 60 * 1000);

    usersSh.getRange(userRow, 5).setValue(sessionTokenHash);
    usersSh.getRange(userRow, 6).setValue(sessionExpiresAt);

    beautifyUsersSheet_(usersSh);
    beautifyLoginTokensSheet_(tokensSh);

    return {
      ok: true,
      email: email,
      userId: userId,
      sessionToken: sessionToken,
      sessionExpiresAt: sessionExpiresAt.toISOString()
    };
  });
}

function api_me_(p) {
  const auth = requireAuth_(p);
  if (!auth.ok) return auth;

  return {
    ok: true,
    email: auth.email,
    userId: auth.userId,
    sessionExpiresAt: auth.sessionExpiresAt
  };
}

// ===================== SESSIONS CRUD =====================
function api_listSessions_(p) {
  const auth = requireAuth_(p);
  if (!auth.ok) return auth;

  const ss = SpreadsheetApp.openById(SPREADSHEET_ID);
  const sh = ensureSessionsSheet_(ss);
  const values = sh.getDataRange().getValues();
  const map = getHeaderMap_(values[0] || []);

  const sessions = [];
  for (let i = 1; i < values.length; i++) {
    const rowObj = getSessionObjectFromRow_(values[i], map);
    if (rowObj.userId !== auth.userId) continue;

    sessions.push({
      sessionId: rowObj.sessionId,
      userId: rowObj.userId,
      userEmail: rowObj.userEmail,
      title: rowObj.title,
      tags: rowObj.tags,
      folder: rowObj.folder,
      visibility: rowObj.visibility,
      payload: rowObj.payload,
      createdAt: rowObj.createdAt,
      updatedAt: rowObj.updatedAt
    });
  }

  sessions.sort(function(a, b) {
    const aDate = coerceToDateValue_(a.updatedAt);
    const bDate = coerceToDateValue_(b.updatedAt);
    const aTime = aDate ? aDate.getTime() : 0;
    const bTime = bDate ? bDate.getTime() : 0;
    return bTime - aTime;
  });

  return { ok: true, sessions: sessions };
}

function api_saveSession_(p) {
  const auth = requireAuth_(p);
  if (!auth.ok) return auth;

  const title = String(p.title || "Untitled Session").trim();
  const tags = String(p.tags || "").trim();
  const folder = String(p.folder || "").trim();
  const visibility = String(p.visibility || "private").trim();
  const payload = String(p.payload || "").trim();

  if (!payload) return { ok: false, error: "Missing payload" };

  return withScriptLock_(function () {
    const ss = SpreadsheetApp.openById(SPREADSHEET_ID);
    const sh = ensureSessionsSheet_(ss);

    const now = new Date();
    const sessionId = Utilities.getUuid();

    const row = buildSessionRow_({
      sessionId: sessionId,
      userEmail: auth.email,
      userId: auth.userId,
      title: title,
      tags: tags,
      folder: folder,
      visibility: visibility,
      payload: payload,
      createdAt: now,
      updatedAt: now
    });

    sh.appendRow(row);
    applySessionsSheetStyle_(sh);

    return { ok: true, sessionId: sessionId };
  });
}

function api_updateSession_(p) {
  const auth = requireAuth_(p);
  if (!auth.ok) return auth;

  const sessionId = String(p.sessionId || "").trim();
  if (!sessionId) return { ok: false, error: "Missing sessionId" };

  const title = (p.title != null) ? String(p.title).trim() : null;
  const tags = (p.tags != null) ? String(p.tags).trim() : null;
  const folder = (p.folder != null) ? String(p.folder).trim() : null;
  const visibility = (p.visibility != null) ? String(p.visibility).trim() : null;
  const payload = (p.payload != null) ? String(p.payload).trim() : null;

  return withScriptLock_(function () {
    const ss = SpreadsheetApp.openById(SPREADSHEET_ID);
    const sh = ensureSessionsSheet_(ss);
    const values = sh.getDataRange().getValues();
    const map = getHeaderMap_(values[0] || []);

    for (let i = 1; i < values.length; i++) {
      const rowObj = getSessionObjectFromRow_(values[i], map);
      if (rowObj.sessionId !== sessionId) continue;

      if (rowObj.userId !== auth.userId) {
        return { ok: false, error: "Not allowed" };
      }

      const updatedRow = buildSessionRow_({
        sessionId: sessionId,
        userEmail: auth.email,
        userId: auth.userId,
        title: title !== null ? title : rowObj.title,
        tags: tags !== null ? tags : rowObj.tags,
        folder: folder !== null ? folder : rowObj.folder,
        visibility: visibility !== null ? visibility : rowObj.visibility,
        payload: payload !== null ? payload : rowObj.payload,
        createdAt: rowObj.createdAt || new Date(),
        updatedAt: new Date()
      });

      sh.getRange(i + 1, 1, 1, SESSIONS_HEADERS.length).setValues([updatedRow]);
      applySessionsSheetStyle_(sh);
      return { ok: true };
    }

    return { ok: false, error: "Session not found" };
  });
}

function api_deleteSession_(p) {
  const auth = requireAuth_(p);
  if (!auth.ok) return auth;

  const sessionId = String(p.sessionId || "").trim();
  if (!sessionId) return { ok: false, error: "Missing sessionId" };

  return withScriptLock_(function () {
    const ss = SpreadsheetApp.openById(SPREADSHEET_ID);
    const sh = ensureSessionsSheet_(ss);
    const values = sh.getDataRange().getValues();
    const map = getHeaderMap_(values[0] || []);

    for (let i = 1; i < values.length; i++) {
      const rowObj = getSessionObjectFromRow_(values[i], map);
      if (rowObj.sessionId !== sessionId) continue;

      if (rowObj.userId !== auth.userId) {
        return { ok: false, error: "Not allowed" };
      }

      sh.deleteRow(i + 1);
      applySessionsSheetStyle_(sh);
      return { ok: true };
    }

    return { ok: false, error: "Session not found" };
  });
}

function api_getSharedSession_(p) {
  const sessionId = String(p.sessionId || p.s || "").trim();
  if (!sessionId) return { ok: false, error: "Missing sessionId" };

  const ss = SpreadsheetApp.openById(SPREADSHEET_ID);
  const sh = ensureSessionsSheet_(ss);
  const values = sh.getDataRange().getValues();
  const map = getHeaderMap_(values[0] || []);

  for (let i = 1; i < values.length; i++) {
    const rowObj = getSessionObjectFromRow_(values[i], map);
    if (rowObj.sessionId !== sessionId) continue;

    const visibility = String(rowObj.visibility || "private").trim().toLowerCase();
    if (visibility !== "shared" && visibility !== "public") {
      return { ok: false, error: "Session is not shared" };
    }

    return {
      ok: true,
      session: {
        sessionId: rowObj.sessionId,
        title: rowObj.title,
        tags: rowObj.tags,
        folder: rowObj.folder,
        visibility: rowObj.visibility,
        payload: rowObj.payload,
        createdAt: rowObj.createdAt,
        updatedAt: rowObj.updatedAt
      }
    };
  }

  return { ok: false, error: "Session not found" };
}

// ===================== AUTH HELPERS =====================
function requireAuth_(p) {
  const sessionToken = String(p.sessionToken || "").trim();
  if (!sessionToken) return { ok: false, error: "Missing sessionToken" };

  const ss = SpreadsheetApp.openById(SPREADSHEET_ID);
  const usersSh = ensureSheet_(ss, "users", USERS_HEADERS);
  const users = usersSh.getDataRange().getValues();

  const tokenHash = sha256_(sessionToken);
  const now = new Date();

  for (let i = 1; i < users.length; i++) {
    const rowHash = String(users[i][4] || "");
    if (rowHash && rowHash === tokenHash) {
      const expiresIso = String(users[i][5] || "");
      const exp = new Date(expiresIso);

      if (!expiresIso || isNaN(exp.getTime()) || now > exp) {
        return { ok: false, error: "Session expired" };
      }

      return {
        ok: true,
        email: String(users[i][0] || ""),
        userId: String(users[i][1] || ""),
        sessionExpiresAt: expiresIso
      };
    }
  }

  return { ok: false, error: "Invalid session" };
}

// ===================== SESSIONS SHEET HELPERS =====================
function ensureSessionsSheet_(ss) {
  let sh = ss.getSheetByName("sessions");

  if (!sh) {
    sh = ss.insertSheet("sessions");
    sh.getRange(1, 1, 1, SESSIONS_HEADERS.length).setValues([SESSIONS_HEADERS]);
    applySessionsSheetStyle_(sh);
    return sh;
  }

  if (sh.getLastRow() === 0) {
    sh.appendRow(SESSIONS_HEADERS);
    applySessionsSheetStyle_(sh);
    return sh;
  }

  const headerRow = sh.getRange(1, 1, 1, sh.getLastColumn()).getValues()[0];
  const existingHeaders = headerRow.map(function(v) { return String(v || "").trim(); });

  if (arraysEqual_(existingHeaders, SESSIONS_HEADERS)) {
    applySessionsSheetStyle_(sh);
    return sh;
  }

  const data = sh.getDataRange().getValues();
  const oldMap = getHeaderMap_(existingHeaders);
  const emailByUserId = getEmailByUserIdMap_(ss);

  const rebuilt = [];
  for (let i = 1; i < data.length; i++) {
    const oldRow = data[i];
    const sessionId = getValueByHeader_(oldRow, oldMap, "sessionId");
    if (!sessionId) continue;

    const userId = getValueByHeader_(oldRow, oldMap, "userId");
    const userEmail =
      getValueByHeader_(oldRow, oldMap, "userEmail") ||
      emailByUserId[String(userId || "").trim()] ||
      "";

    rebuilt.push(buildSessionRow_({
      sessionId: sessionId,
      userEmail: userEmail,
      userId: userId,
      title: getValueByHeader_(oldRow, oldMap, "title"),
      tags: getValueByHeader_(oldRow, oldMap, "tags"),
      folder: getValueByHeader_(oldRow, oldMap, "folder"),
      visibility: getValueByHeader_(oldRow, oldMap, "visibility") || "private",
      payload: getValueByHeader_(oldRow, oldMap, "payload"),
      createdAt: getValueByHeader_(oldRow, oldMap, "createdAt"),
      updatedAt: getValueByHeader_(oldRow, oldMap, "updatedAt")
    }));
  }

  sh.clearContents();
  sh.clearFormats();
  sh.getRange(1, 1, 1, SESSIONS_HEADERS.length).setValues([SESSIONS_HEADERS]);
  if (rebuilt.length) {
    sh.getRange(2, 1, rebuilt.length, SESSIONS_HEADERS.length).setValues(rebuilt);
  }
  trimExtraColumns_(sh, SESSIONS_HEADERS.length);
  trimExtraRows_(sh, rebuilt.length + 1);
  applySessionsSheetStyle_(sh);
  return sh;
}

function buildSessionRow_(obj) {
  const summary = extractSessionSummary_(obj.payload);

  return [
    String(obj.sessionId || "").trim(),
    normalizeEmail_(obj.userEmail || ""),
    String(obj.userId || "").trim(),
    String(obj.title || "Untitled Session").trim(),
    String(obj.tags || "").trim(),
    String(obj.folder || "").trim(),
    String(obj.visibility || "private").trim(),
    String(summary.sessionDate || ""),
    String(summary.coach || ""),
    String(summary.ageGroup || ""),
    String(summary.theme || ""),
    String(summary.sessionDrills || ""),
    String(summary.notesPreview || ""),
    String(summary.prettyPayload || String(obj.payload || "")),
    coerceToDateValue_(obj.createdAt) || obj.createdAt || "",
    coerceToDateValue_(obj.updatedAt) || obj.updatedAt || ""
  ];
}

function getSessionObjectFromRow_(row, map) {
  return {
    sessionId: getValueByHeader_(row, map, "sessionId"),
    userEmail: getValueByHeader_(row, map, "userEmail"),
    userId: getValueByHeader_(row, map, "userId"),
    title: getValueByHeader_(row, map, "title"),
    tags: getValueByHeader_(row, map, "tags"),
    folder: getValueByHeader_(row, map, "folder"),
    visibility: getValueByHeader_(row, map, "visibility") || "private",
    sessionDate: getValueByHeader_(row, map, "sessionDate"),
    coach: getValueByHeader_(row, map, "coach"),
    ageGroup: getValueByHeader_(row, map, "ageGroup"),
    theme: getValueByHeader_(row, map, "theme"),
    sessionDrills: getValueByHeader_(row, map, "sessionDrills"),
    notesPreview: getValueByHeader_(row, map, "notesPreview"),
    payload: getValueByHeader_(row, map, "payload"),
    createdAt: getValueByHeader_(row, map, "createdAt"),
    updatedAt: getValueByHeader_(row, map, "updatedAt")
  };
}

function extractSessionSummary_(payloadValue) {
  const parsed = safeJsonParse_(payloadValue);
  const payloadObj = parsed.ok ? parsed.value : null;
  const rows = normalizePayloadRows_(payloadObj);

  const sessionDate = payloadObj && payloadObj.date ? String(payloadObj.date) : "";
  const coach = payloadObj && payloadObj.coach ? String(payloadObj.coach) : "";
  const ageGroup = payloadObj && payloadObj.ageGroup ? String(payloadObj.ageGroup) : "";
  const theme = payloadObj && payloadObj.theme ? String(payloadObj.theme) : "";

  const drillLines = [];
  const notesLines = [];

  for (let i = 0; i < rows.length; i++) {
    const item = rows[i] || {};
    const name =
      String(item.drillName || item.name || item.title || item.drill || "").trim() ||
      "Untitled drill";
    const coachNotes = String(item.notes || "").trim();
    const fallbackNotes = String(item.defaultNotes || "").trim();
    const shownNotes = coachNotes || fallbackNotes;

    drillLines.push((i + 1) + ". " + name);
    if (shownNotes) {
      notesLines.push((i + 1) + ". " + clipText_(shownNotes, 120));
    }
  }

  return {
    sessionDate: sessionDate,
    coach: coach,
    ageGroup: ageGroup,
    theme: theme,
    sessionDrills: drillLines.join("\n"),
    notesPreview: notesLines.join("\n"),
    prettyPayload: payloadObj ? JSON.stringify(payloadObj, null, 2) : String(payloadValue || "")
  };
}

function normalizePayloadRows_(payload) {
  if (!payload) return [];
  if (Array.isArray(payload)) return payload;
  if (payload && Array.isArray(payload.rows)) return payload.rows;
  if (payload && Array.isArray(payload.data)) return payload.data;
  if (payload && Array.isArray(payload.items)) return payload.items;
  if (payload && Array.isArray(payload.result)) return payload.result;
  if (payload && payload.payload) return normalizePayloadRows_(payload.payload);
  if (payload && payload.response) return normalizePayloadRows_(payload.response);
  return [];
}

function safeJsonParse_(value) {
  if (value == null) return { ok: false, value: null };
  if (typeof value === "object") return { ok: true, value: value };

  const raw = String(value || "").trim();
  if (!raw) return { ok: false, value: null };

  try {
    return { ok: true, value: JSON.parse(raw) };
  } catch (err) {
    return { ok: false, value: null };
  }
}

function getEmailByUserIdMap_(ss) {
  const usersSh = ensureSheet_(ss, "users", USERS_HEADERS);
  const values = usersSh.getDataRange().getValues();
  const out = {};

  for (let i = 1; i < values.length; i++) {
    const email = normalizeEmail_(values[i][0] || "");
    const userId = String(values[i][1] || "").trim();
    if (userId) out[userId] = email;
  }

  return out;
}

function applySessionsSheetStyle_(sh) {
  const lastRow = Math.max(sh.getLastRow(), 1);
  const lastCol = SESSIONS_HEADERS.length;

  sh.showColumns(1, lastCol);
  sh.setFrozenRows(1);

  const headerRange = sh.getRange(1, 1, 1, lastCol);
  headerRange
    .setBackground("#c83127")
    .setFontColor("#ffffff")
    .setFontWeight("bold")
    .setWrap(true)
    .setVerticalAlignment("middle");

  normalizeDateColumnsAndFormats_(sh, {
    8: 'mmm d, yyyy',
    15: 'mmm d, yyyy "•" h:mm AM/PM',
    16: 'mmm d, yyyy "•" h:mm AM/PM'
  });

  const wholeRange = sh.getRange(1, 1, lastRow, lastCol);
  wholeRange.setWrap(true);
  wholeRange.setVerticalAlignment("top");

  const existingBandings = sh.getBandings();
  existingBandings.forEach(function (banding) {
    banding.remove();
  });

  if (lastRow > 1) {
    sh.getRange(2, 1, lastRow - 1, lastCol).setBackground("#ffffff");
    sh.getRange(2, 1, lastRow - 1, lastCol).applyRowBanding(SpreadsheetApp.BandingTheme.LIGHT_GREY);
    sh.autoResizeRows(2, lastRow - 1);
  }

  setColumnWidthsByHeader_(sh, {
    sessionId: 190,
    userEmail: 220,
    userId: 190,
    title: 220,
    tags: 150,
    folder: 220,
    visibility: 110,
    sessionDate: 110,
    coach: 150,
    ageGroup: 120,
    theme: 160,
    sessionDrills: 460,
    notesPreview: 320,
    payload: 520,
    createdAt: 170,
    updatedAt: 170
  });

  hideColumnIfPresent_(sh, "userId");
  hideColumnIfPresent_(sh, "payload");

  const payloadCol = getHeaderIndexByName_(SESSIONS_HEADERS, "payload");
  const drillsCol = getHeaderIndexByName_(SESSIONS_HEADERS, "sessionDrills");
  const notesCol = getHeaderIndexByName_(SESSIONS_HEADERS, "notesPreview");

  if (payloadCol > 0 && lastRow > 1) {
    sh.getRange(2, payloadCol, lastRow - 1, 1).setWrapStrategy(SpreadsheetApp.WrapStrategy.WRAP);
  }
  if (drillsCol > 0 && lastRow > 1) {
    sh.getRange(2, drillsCol, lastRow - 1, 1)
      .setFontWeight("bold")
      .setWrapStrategy(SpreadsheetApp.WrapStrategy.WRAP);
  }
  if (notesCol > 0 && lastRow > 1) {
    sh.getRange(2, notesCol, lastRow - 1, 1).setFontStyle("italic");
  }

  installFilterIfMissing_(sh, lastRow, lastCol);
}

function styleAllWorkbookSheets_() {
  const ss = SpreadsheetApp.openById(SPREADSHEET_ID);
  const sheets = ss.getSheets();

  sheets.forEach(function (sh) {
    const name = sh.getName();
    if (name === "sessions") {
      sh.setTabColor("#c83127");
      return;
    }
    if (name === "users") {
      beautifyUsersSheet_(sh);
      sh.setTabColor("#111827");
      return;
    }
    if (name === "login_tokens") {
      beautifyLoginTokensSheet_(sh);
      sh.setTabColor("#6b7280");
      return;
    }
    styleGenericDataSheet_(sh, "#374151", sh.getLastColumn(), []);
    sh.setTabColor("#374151");
  });
}

function beautifyUsersSheet_(sh) {
  const prettyHeaders = [
    "User Email",
    "User ID",
    "Created",
    "Last Login",
    "Session Token Hash",
    "Session Expires"
  ];

  ensurePrettyHeaders_(sh, prettyHeaders);
  normalizeDateColumnsAndFormats_(sh, {
    3: 'mmm d, yyyy "•" h:mm AM/PM',
    4: 'mmm d, yyyy "•" h:mm AM/PM',
    6: 'mmm d, yyyy "•" h:mm AM/PM'
  });
  styleGenericDataSheet_(sh, "#111827", sh.getLastColumn(), [2, 5]);

  sh.setColumnWidth(1, 240);
  sh.setColumnWidth(3, 170);
  sh.setColumnWidth(4, 170);
  sh.setColumnWidth(6, 170);
}

function beautifyLoginTokensSheet_(sh) {
  const prettyHeaders = [
    "Token Hash",
    "User Email",
    "Token Expires",
    "Used At",
    "Created"
  ];

  ensurePrettyHeaders_(sh, prettyHeaders);
  normalizeDateColumnsAndFormats_(sh, {
    3: 'mmm d, yyyy "•" h:mm AM/PM',
    4: 'mmm d, yyyy "•" h:mm AM/PM',
    5: 'mmm d, yyyy "•" h:mm AM/PM'
  });
  styleGenericDataSheet_(sh, "#6b7280", sh.getLastColumn(), [1]);

  sh.setColumnWidth(2, 240);
  sh.setColumnWidth(3, 170);
  sh.setColumnWidth(4, 170);
  sh.setColumnWidth(5, 170);
}

function ensurePrettyHeaders_(sh, headers) {
  if (!headers || !headers.length) return;
  sh.getRange(1, 1, 1, headers.length).setValues([headers]);
}

function styleGenericDataSheet_(sh, headerColor, visibleCols, columnsToHide) {
  const lastRow = Math.max(sh.getLastRow(), 1);
  const lastCol = Math.max(sh.getLastColumn(), 1);

  sh.showColumns(1, lastCol);
  sh.setFrozenRows(1);

  const headerRange = sh.getRange(1, 1, 1, lastCol);
  headerRange
    .setBackground(headerColor)
    .setFontColor("#ffffff")
    .setFontWeight("bold")
    .setWrap(true)
    .setVerticalAlignment("middle");

  const wholeRange = sh.getRange(1, 1, lastRow, lastCol);
  wholeRange.setWrap(true);
  wholeRange.setVerticalAlignment("top");

  const existingBandings = sh.getBandings();
  existingBandings.forEach(function (banding) {
    banding.remove();
  });

  if (lastRow > 1) {
    sh.getRange(2, 1, lastRow - 1, lastCol).setBackground("#ffffff");
    sh.getRange(2, 1, lastRow - 1, lastCol).applyRowBanding(SpreadsheetApp.BandingTheme.LIGHT_GREY);
    sh.autoResizeRows(2, lastRow - 1);
  }

  if (visibleCols > 0) {
    for (var col = 1; col <= visibleCols; col++) {
      sh.setColumnWidth(col, 180);
    }
  }

  (columnsToHide || []).forEach(function (colIndex) {
    if (colIndex > 0 && colIndex <= lastCol) {
      sh.hideColumns(colIndex);
    }
  });

  installFilterIfMissing_(sh, lastRow, lastCol);
}

// ===================== UTIL =====================
function parsePostBody_(e) {
  if (!e || !e.postData || !e.postData.contents) return {};

  const raw = String(e.postData.contents || "").trim();
  if (!raw) return {};

  try {
    return JSON.parse(raw);
  } catch (err) {
    throw new Error("Invalid JSON body");
  }
}

function json_(obj) {
  return ContentService
    .createTextOutput(JSON.stringify(obj))
    .setMimeType(ContentService.MimeType.JSON);
}

function jsonp_(callback, obj) {
  const safeCb = callback && /^[a-zA-Z_$][0-9a-zA-Z_$\.]*$/.test(callback)
    ? callback
    : DEFAULT_CALLBACK;

  const out = safeCb + "(" + JSON.stringify(obj) + ");";
  return ContentService.createTextOutput(out).setMimeType(ContentService.MimeType.JAVASCRIPT);
}

function isPublicFn_(fn) {
  return fn === "getSharedSession";
}

function requireProxySecretForFn_(fn, p) {
  if (!fn) return { ok: false, error: "Missing or unknown fn" };
  if (isPublicFn_(fn)) return { ok: true };

  const provided = String((p && p.proxySecret) || "").trim();
  if (!PROXY_SHARED_SECRET) {
    return { ok: false, error: "PROXY_SHARED_SECRET is not configured in Apps Script." };
  }
  if (!provided || provided !== PROXY_SHARED_SECRET) {
    return { ok: false, error: "Proxy secret invalid." };
  }
  return { ok: true };
}

function normalizeMagicReturnTo_(value) {
  const fallback = DEFAULT_MAGIC_LINK_BASE_URL;

  if (value == null) return fallback;

  const raw = String(value || "").trim();
  if (!raw) return fallback;

  const match = raw.match(/^https:\/\/([^\/?#]+)(\/[^?#]*)?(\?[^#]*)?(#.*)?$/i);
  if (!match) return fallback;

  const hostname = String(match[1] || "").toLowerCase();
  const path = match[2] || "/";
  const query = match[3] || "";

  if (ALLOWED_MAGIC_LINK_RETURN_HOSTS.indexOf(hostname) === -1) return fallback;

  const cleanQuery = query
    .replace(/^\?/, "")
    .split("&")
    .filter(Boolean)
    .filter(function (pair) {
      return pair.split("=")[0] !== "magic";
    })
    .join("&");

  return "https://" + hostname + path + (cleanQuery ? "?" + cleanQuery : "");
}

function appendQueryParam_(url, key, value) {
  const base = String(url || DEFAULT_MAGIC_LINK_BASE_URL).trim();
  const k = encodeURIComponent(String(key || ""));
  const v = encodeURIComponent(String(value || ""));

  const parts = base.split("#");
  const beforeHash = parts[0];
  const hash = parts.length > 1 ? "#" + parts.slice(1).join("#") : "";

  const qIndex = beforeHash.indexOf("?");
  const path = qIndex >= 0 ? beforeHash.slice(0, qIndex) : beforeHash;
  const query = qIndex >= 0 ? beforeHash.slice(qIndex + 1) : "";

  const filtered = query
    ? query.split("&").filter(Boolean).filter(function (pair) {
        return decodeURIComponent(pair.split("=")[0] || "") !== String(key || "");
      })
    : [];

  filtered.push(k + "=" + v);

  return path + "?" + filtered.join("&") + hash;
}

function ensureSheet_(ss, name, headers) {
  let sh = ss.getSheetByName(name);

  if (!sh) {
    sh = ss.insertSheet(name);
    sh.appendRow(headers);
  } else if (sh.getLastRow() === 0) {
    sh.appendRow(headers);
  }

  return sh;
}

function getHeaderMap_(headers) {
  const map = {};
  for (let i = 0; i < headers.length; i++) {
    map[String(headers[i] || "").trim()] = i;
  }
  return map;
}

function getValueByHeader_(row, map, key) {
  if (!row || !map || !Object.prototype.hasOwnProperty.call(map, key)) return "";
  return String(row[map[key]] || "").trim();
}

function getHeaderIndexByName_(headers, name) {
  for (let i = 0; i < headers.length; i++) {
    if (String(headers[i] || "").trim() === name) return i + 1;
  }
  return -1;
}

function setColumnWidthsByHeader_(sh, widthsByHeader) {
  Object.keys(widthsByHeader).forEach(function(headerName) {
    const col = getHeaderIndexByName_(SESSIONS_HEADERS, headerName);
    if (col > 0) sh.setColumnWidth(col, widthsByHeader[headerName]);
  });
}

function hideColumnIfPresent_(sh, headerName) {
  const col = getHeaderIndexByName_(SESSIONS_HEADERS, headerName);
  if (col > 0) sh.hideColumns(col);
}

function trimExtraColumns_(sh, keepColumns) {
  const maxCols = sh.getMaxColumns();
  if (maxCols > keepColumns) {
    sh.deleteColumns(keepColumns + 1, maxCols - keepColumns);
  }
}

function trimExtraRows_(sh, keepRows) {
  const maxRows = sh.getMaxRows();
  if (maxRows > keepRows) {
    sh.deleteRows(keepRows + 1, maxRows - keepRows);
  }
}

function arraysEqual_(a, b) {
  if (!a || !b || a.length !== b.length) return false;
  for (let i = 0; i < a.length; i++) {
    if (String(a[i] || "").trim() !== String(b[i] || "").trim()) return false;
  }
  return true;
}

function installFilterIfMissing_(sh, lastRow, lastCol) {
  const existing = sh.getFilter();
  if (existing) return;
  if (lastRow < 1 || lastCol < 1) return;
  sh.getRange(1, 1, lastRow, lastCol).createFilter();
}

function normalizeDateColumnsAndFormats_(sh, formatMap) {
  const lastRow = sh.getLastRow();
  if (lastRow < 2 || !formatMap) return;

  Object.keys(formatMap).forEach(function (colKey) {
    const col = Number(colKey);
    if (!col || col > sh.getLastColumn()) return;

    const range = sh.getRange(2, col, lastRow - 1, 1);
    const values = range.getValues();
    let changed = false;

    for (let i = 0; i < values.length; i++) {
      const current = values[i][0];
      const parsed = coerceToDateValue_(current);
      if (parsed) {
        if (!(current instanceof Date) || current.getTime() !== parsed.getTime()) {
          values[i][0] = parsed;
          changed = true;
        }
      }
    }

    if (changed) {
      range.setValues(values);
    }
    range.setNumberFormat(formatMap[col]);
  });
}

function coerceToDateValue_(value) {
  if (!value) return null;
  if (value instanceof Date && !isNaN(value.getTime())) return value;

  const raw = String(value).trim();
  if (!raw) return null;

  const dateOnly = raw.match(/^(\d{4})-(\d{2})-(\d{2})$/);
  if (dateOnly) {
    return new Date(Number(dateOnly[1]), Number(dateOnly[2]) - 1, Number(dateOnly[3]));
  }

  const parsed = new Date(raw);
  if (!isNaN(parsed.getTime())) return parsed;
  return null;
}

function clipText_(value, maxLen) {
  const raw = String(value || "").trim().replace(/\s+/g, " ");
  if (!raw) return "";
  if (raw.length <= maxLen) return raw;
  return raw.slice(0, Math.max(0, maxLen - 1)).trim() + "…";
}

function normalizeEmail_(email) {
  return String(email || "").trim().toLowerCase();
}

function randomToken_(len) {
  const chars = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
  let s = "";
  for (let i = 0; i < len; i++) {
    s += chars[Math.floor(Math.random() * chars.length)];
  }
  return s;
}

function sha256_(str) {
  const raw = Utilities.computeDigest(
    Utilities.DigestAlgorithm.SHA_256,
    str,
    Utilities.Charset.UTF_8
  );
  return raw.map(function(b) {
    return ("0" + (b & 0xFF).toString(16)).slice(-2);
  }).join("");
}

function withScriptLock_(fn) {
  const lock = LockService.getScriptLock();
  const locked = lock.tryLock(LOCK_TIMEOUT_MS);

  if (!locked) {
    throw new Error("System is busy. Please try again in a few seconds.");
  }

  try {
    return fn();
  } finally {
    lock.releaseLock();
  }
}
