const http = require("http");
const fs = require("fs");
const path = require("path");
const net = require("net");
const crypto = require("crypto");
const { Pool } = require("pg");
const nodemailer = require("nodemailer");


/*
==================================================
GREENAIR TRENDLOG
==================================================

LIVE BMS POLL:
Every 3 seconds

LIVE WEB TREND:
Every 15 seconds

PERMANENT POSTGRES HISTORY:
Every 1 minute

EMAIL REPORT:
Every 8 hours

EMAIL CONTENT:
Previous 8 hours of PostgreSQL history
attached as CSV.

==================================================
*/


/*
==================================================
BMS CONNECTION
==================================================
*/

const BMS_HOST =
  process.env.BMS_HOST ||
  "bms.biancoprecast.com.au";


const BMS_PORT =
  Number(
    process.env.BMS_PORT ||
    502
  );


const UNIT_ID =
  Number(
    process.env.UNIT_ID ||
    69
  );


const PORT =
  Number(
    process.env.PORT ||
    10000
  );


/*
==================================================
DATABASE
==================================================
*/

const DATABASE_URL =
  process.env.DATABASE_URL ||
  "";


let db =
  null;


if (
  DATABASE_URL
) {

  db =
    new Pool({

      connectionString:
        DATABASE_URL,

      max:
        5,

      idleTimeoutMillis:
        30000,

      connectionTimeoutMillis:
        10000

    });

}


/*
==================================================
EMAIL CONFIGURATION
==================================================
*/

const EMAIL_USER =
  process.env.EMAIL_USER ||
  "";


const EMAIL_APP_PASSWORD =
  process.env.EMAIL_APP_PASSWORD ||
  "";


const EMAIL_TO =
  process.env.EMAIL_TO ||
  "greenair.controls@gmail.com";


const EMAIL_REPORT_HOURS =
  8;


const EMAIL_REPORT_MS =
  EMAIL_REPORT_HOURS *
  60 *
  60 *
  1000;


let emailTransporter =
  null;


let emailConfigured =
  false;


let emailConnected =
  false;


let lastEmailAt =
  null;


let lastEmailError =
  null;


let nextEmailAt =
  null;


let emailTimer =
  null;


/*
==================================================
DROPBOX OAUTH CONFIGURATION
==================================================
*/

const DROPBOX_APP_KEY =
  process.env.DROPBOX_APP_KEY ||
  "";


const DROPBOX_APP_SECRET =
  process.env.DROPBOX_APP_SECRET ||
  "";


const DROPBOX_REDIRECT_URI =
  process.env.DROPBOX_REDIRECT_URI ||
  "https://greenair-trendlog.onrender.com/dropbox/callback";


const DROPBOX_REFRESH_TOKEN =
  process.env.DROPBOX_REFRESH_TOKEN ||
  "";


const DROPBOX_BACKUP_HOURS =
  8;


const DROPBOX_BACKUP_MS =
  DROPBOX_BACKUP_HOURS *
  60 *
  60 *
  1000;


let dropboxBackupTimer =
  null;


let nextDropboxBackupAt =
  null;


let lastDropboxBackupAt =
  null;


let lastDropboxBackupError =
  null;


const dropboxOAuthStates =
  new Map();


function createDropboxOAuthState() {

  const state =
    crypto
      .randomBytes(24)
      .toString("hex");


  dropboxOAuthStates.set(
    state,
    Date.now() + 10 * 60 * 1000
  );


  return state;

}


function consumeDropboxOAuthState(
  state
) {

  const expiresAt =
    dropboxOAuthStates.get(
      state
    );


  dropboxOAuthStates.delete(
    state
  );


  if (
    !expiresAt
    ||
    expiresAt <
    Date.now()
  ) {

    return false;

  }


  return true;

}


/*
==================================================
CREATE EMAIL TRANSPORTER
==================================================

if (
  EMAIL_USER &&
  EMAIL_APP_PASSWORD
) {

  emailTransporter =

    nodemailer.createTransport({

      service:
        "gmail",

      auth: {

        user:
          EMAIL_USER,

        pass:
          EMAIL_APP_PASSWORD

      }

    });


  emailConfigured =
    true;

}


/*
==================================================
AUTHENTICATION / USER MANAGEMENT
==================================================
*/

const MASTER_USERNAME =
  (process.env.MASTER_USERNAME || "")
    .trim();


const MASTER_PASSWORD =
  process.env.MASTER_PASSWORD ||
  "";


const SESSION_HOURS =
  12;


const SESSION_MS =
  SESSION_HOURS *
  60 *
  60 *
  1000;


/*
==================================================
NORMALISE USERNAME
==================================================
*/

function normaliseUsername(
  value
) {

  return String(
    value || ""
  ).trim();

}


/*
==================================================
PASSWORD HASHING
==================================================
*/

function hashPassword(
  password,
  saltHex = null
) {

  const salt =
    saltHex

    ?

    Buffer.from(
      saltHex,
      "hex"
    )

    :

    crypto.randomBytes(
      16
    );


  const hash =
    crypto.scryptSync(

      String(password),

      salt,

      64

    );


  return (

    salt.toString("hex")

    +

    ":"

    +

    hash.toString("hex")

  );

}


/*
==================================================
VERIFY PASSWORD
==================================================
*/

function verifyPassword(
  password,
  stored
) {

  try {

    const [
      saltHex,
      hashHex
    ] =

      String(
        stored || ""
      ).split(":");


    if (
      !saltHex ||
      !hashHex
    ) {

      return false;

    }


    const candidate =

      crypto.scryptSync(

        String(password),

        Buffer.from(
          saltHex,
          "hex"
        ),

        64

      );


    const expected =

      Buffer.from(
        hashHex,
        "hex"
      );


    return (

      candidate.length ===
      expected.length

      &&

      crypto.timingSafeEqual(
        candidate,
        expected
      )

    );

  }

  catch {

    return false;

  }

}


/*
==================================================
COOKIE PARSER
==================================================
*/

function parseCookies(
  request
) {

  const result = {};


  for (

    const part

    of

    String(
      request.headers.cookie || ""
    ).split(";")

  ) {

    const i =
      part.indexOf("=");


    if (
      i > 0
    ) {

      result[
        part.slice(
          0,
          i
        ).trim()
      ] =

        decodeURIComponent(

          part.slice(
            i + 1
          ).trim()

        );

    }

  }


  return result;

}
function readJsonBody(request, limit = 32768) {

  return new Promise(
    (resolve, reject) => {

      let body = "";

      request.on(
        "data",
        chunk => {

          body += chunk;

          if (
            body.length > limit
          ) {

            reject(
              new Error("Request too large")
            );

            request.destroy();

          }

        }
      );


      request.on(
        "end",
        () => {

          try {

            resolve(
              body
                ? JSON.parse(body)
                : {}
            );

          }

          catch {

            reject(
              new Error("Invalid JSON")
            );

          }

        }
      );


      request.on(
        "error",
        reject
      );

    }
  );

}


/*
==================================================
SESSION COOKIE
==================================================
*/

function setSessionCookie(
  response,
  token
) {

  response.setHeader(

    "Set-Cookie",

    `greenair_session=${encodeURIComponent(token)}; Path=/; HttpOnly; Secure; SameSite=Lax; Max-Age=${SESSION_HOURS * 3600}`

  );

}


function clearSessionCookie(
  response
) {

  response.setHeader(

    "Set-Cookie",

    "greenair_session=; Path=/; HttpOnly; Secure; SameSite=Lax; Max-Age=0"

  );

}


/*
==================================================
INITIALISE AUTHENTICATION DATABASE
==================================================
*/

async function initialiseAuthentication() {

  if (
    !db
  ) {

    return;

  }


  /*
  USERS
  */

  await db.query(`

    CREATE TABLE IF NOT EXISTS app_users (

      id BIGSERIAL PRIMARY KEY,

      username TEXT UNIQUE NOT NULL,

      password_hash TEXT NOT NULL,

      role TEXT NOT NULL
        CHECK (
          role IN ('master','user')
        ),

      active BOOLEAN NOT NULL
        DEFAULT TRUE,

      created_at TIMESTAMPTZ NOT NULL
        DEFAULT NOW(),

      last_login_at TIMESTAMPTZ

    )

  `);


  /*
  SESSIONS
  */

  await db.query(`

    CREATE TABLE IF NOT EXISTS app_sessions (

      token_hash TEXT PRIMARY KEY,

      user_id BIGINT NOT NULL
        REFERENCES app_users(id)
        ON DELETE CASCADE,

      created_at TIMESTAMPTZ NOT NULL
        DEFAULT NOW(),

      expires_at TIMESTAMPTZ NOT NULL

    )

  `);


  await db.query(`

    CREATE INDEX IF NOT EXISTS
      app_sessions_expires_idx

    ON app_sessions(expires_at)

  `);


  /*
  CREATE MASTER ACCOUNT
  */

  if (
    MASTER_USERNAME &&
    MASTER_PASSWORD
  ) {

    const existing =

      await db.query(

        `

        SELECT id

        FROM app_users

        WHERE
          lower(username) =
          lower($1)

        LIMIT 1

        `,

        [
          MASTER_USERNAME
        ]

      );


    if (
      existing.rowCount === 0
    ) {

      await db.query(

        `

        INSERT INTO app_users (

          username,
          password_hash,
          role,
          active

        )

        VALUES (

          $1,
          $2,
          'master',
          TRUE

        )

        `,

        [

          MASTER_USERNAME,

          hashPassword(
            MASTER_PASSWORD
          )

        ]

      );


      console.log(
        "Master TrendLog account created."
      );

    }

    else {

      await db.query(

        `

        UPDATE app_users

        SET
          role = 'master',
          active = TRUE

        WHERE id = $1

        `,

        [
          existing.rows[0].id
        ]

      );

    }

  }

  else {

    console.warn(

      "MASTER_USERNAME / MASTER_PASSWORD not configured. No automatic master account can be created."

    );

  }


  /*
  REMOVE EXPIRED SESSIONS
  */

  await db.query(`

    DELETE FROM app_sessions

    WHERE expires_at <= NOW()

  `);


  console.log(
    "TrendLog authentication database ready."
  );

}


/*
==================================================
GET AUTHENTICATED USER
==================================================
*/

async function getAuthenticatedUser(
  request
) {

  if (
    !db
  ) {

    return null;

  }


  const token =

    parseCookies(
      request
    ).greenair_session;


  if (
    !token
  ) {

    return null;

  }


  const tokenHash =

    crypto
      .createHash("sha256")
      .update(token)
      .digest("hex");


  const result =

    await db.query(

      `

      SELECT

        u.id,
        u.username,
        u.role,
        u.active,
        s.expires_at

      FROM app_sessions s

      JOIN app_users u
        ON u.id = s.user_id

      WHERE
        s.token_hash = $1

      AND
        s.expires_at > NOW()

      AND
        u.active = TRUE

      LIMIT 1

      `,

      [
        tokenHash
      ]

    );


  return (
    result.rows[0] ||
    null
  );

}


/*
==================================================
CREATE LOGIN SESSION
==================================================
*/

async function createSession(
  response,
  userId
) {

  const token =

    crypto
      .randomBytes(32)
      .toString("hex");


  const tokenHash =

    crypto
      .createHash("sha256")
      .update(token)
      .digest("hex");


  await db.query(

    `

    INSERT INTO app_sessions (

      token_hash,
      user_id,
      expires_at

    )

    VALUES (

      $1,
      $2,
      NOW() +
      ($3 * INTERVAL '1 millisecond')

    )

    `,

    [
      tokenHash,
      userId,
      SESSION_MS
    ]

  );


  setSessionCookie(
    response,
    token
  );

}


/*
==================================================
DELETE LOGIN SESSION
==================================================
*/

async function deleteSession(
  request
) {

  if (
    !db
  ) {

    return;

  }


  const token =

    parseCookies(
      request
    ).greenair_session;


  if (
    !token
  ) {

    return;

  }


  const tokenHash =

    crypto
      .createHash("sha256")
      .update(token)
      .digest("hex");


  await db.query(

    `

    DELETE FROM app_sessions

    WHERE token_hash = $1

    `,

    [
      tokenHash
    ]

  );

}


/*
==================================================
TIMING
==================================================
*/

const POLL_MS =
  3000;


const LIVE_SAMPLE_MS =
  15000;


const HISTORY_INTERVAL_MINUTES =
  1;


const MAX_LIVE_SAMPLES =
  5760;


/*
==================================================
MODBUS POINTS
==================================================
*/

const POINTS = [

  {
    id:
      "in1",

    name:
      "Planks In",

    register:
      7485,

    kind:
      "analog"
  },


  {
    id:
      "in2",

    name:
      "Planks Out",

    register:
      7487,

    kind:
      "analog"
  },


  {
    id:
      "in3",

    name:
      "Ambient",

    register:
      7489,

    kind:
      "analog"
  },


  {
    id:
      "in4",

    name:
      "Planks Concrete",

    register:
      7491,

    kind:
      "analog"
  },


  {
    id:
      "in5",

    name:
      "Planks Tank",

    register:
      7493,

    kind:
      "analog"
  },


  {
    id:
      "diff",

    name:
      "Ambient - Concrete Differential",

    register:
      7503,

    kind:
      "signedAnalog"
  }

];


/*
==================================================
STATE
==================================================
*/

let transactionId =
  1;


let polling =
  false;


let liveHistory =
  [];


const streamClients =
  new Set();


let latest = {

  ok:
    false,

  status:
    "starting",

  error:
    null,

  host:
    BMS_HOST,

  port:
    BMS_PORT,

  unitId:
    UNIT_ID,

  function:
    3,

  results:
    [],

  timestamp:
    null

};


/*
==================================================
LOGGER STATUS
==================================================
*/

let databaseConnected =
  false;


let lastArchiveAt =
  null;


let lastArchiveError =
  null;


let nextArchiveAt =
  null;


let archiveTimer =
  null;
/*
==================================================
DATABASE SETUP
==================================================
*/

async function initialiseDatabase() {

  if (
    !db
  ) {

    databaseConnected =
      false;


    console.error(
      "DATABASE_URL is not configured."
    );


    return;

  }


  try {

    /*
    TREND HISTORY TABLE
    */

    await db.query(`

      CREATE TABLE IF NOT EXISTS trend_history (

        id BIGSERIAL PRIMARY KEY,

        recorded_at TIMESTAMPTZ NOT NULL,

        planks_in DOUBLE PRECISION NOT NULL,

        planks_out DOUBLE PRECISION NOT NULL,

        ambient DOUBLE PRECISION NOT NULL,

        planks_concrete DOUBLE PRECISION NOT NULL,

        planks_tank DOUBLE PRECISION NOT NULL,

        ambient_concrete_diff DOUBLE PRECISION NOT NULL

      )

    `);


    await db.query(`

      CREATE INDEX IF NOT EXISTS
      trend_history_recorded_at_idx

      ON trend_history(recorded_at)

    `);


    /*
    EMAIL REPORT LOG TABLE
    */

    await db.query(`

      CREATE TABLE IF NOT EXISTS trend_email_reports (

        id BIGSERIAL PRIMARY KEY,

        report_key TEXT UNIQUE NOT NULL,

        period_from TIMESTAMPTZ NOT NULL,

        period_to TIMESTAMPTZ NOT NULL,

        sent_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

        recipient TEXT NOT NULL,

        row_count INTEGER NOT NULL DEFAULT 0

      )

    `);


    /*
    RECOVER LAST ARCHIVE
    */

    const previousArchive =

      await db.query(`

        SELECT

          MAX(recorded_at)
          AS last_archive

        FROM trend_history

      `);


    if (

      previousArchive.rows[0]

      &&

      previousArchive.rows[0]
        .last_archive

    ) {

      lastArchiveAt =

        new Date(

          previousArchive.rows[0]
            .last_archive

        );

    }


    /*
    RECOVER LAST EMAIL TIME
    */

    const previousEmail =

      await db.query(`

        SELECT

          MAX(sent_at)
          AS last_email

        FROM trend_email_reports

      `);


    if (

      previousEmail.rows[0]

      &&

      previousEmail.rows[0]
        .last_email

    ) {

      lastEmailAt =

        new Date(

          previousEmail.rows[0]
            .last_email

        );

    }


    databaseConnected =
      true;


    lastArchiveError =
      null;


    console.log(
      "PostgreSQL history database ready."
    );

  }


  catch (
    error
  ) {

    databaseConnected =
      false;


    lastArchiveError =
      error.message;


    console.error(

      "Database setup failed:",

      error.message

    );

  }

}


/*
==================================================
EMAIL CONNECTION TEST
==================================================
*/

async function initialiseEmail() {

  if (
    !emailTransporter
  ) {

    emailConfigured =
      false;


    emailConnected =
      false;


    console.log(
      "Email is not configured yet."
    );


    return;

  }


  try {

    await emailTransporter.verify();


    emailConnected =
      true;


    lastEmailError =
      null;


    console.log(
      "Email SMTP connection ready."
    );

  }


  catch (
    error
  ) {

    emailConnected =
      false;


    lastEmailError =
      error.message;


    console.error(

      "Email SMTP connection failed:",

      error.message

    );

  }

}


/*
==================================================
TRANSACTION ID
==================================================
*/

function nextTransactionId() {

  const current =
    transactionId;


  transactionId++;


  if (
    transactionId >
    65535
  ) {

    transactionId =
      1;

  }


  return current;

}


/*
==================================================
MODBUS REQUEST
==================================================
*/

function buildReadRequest(
  transaction,
  register
) {

  const request =
    Buffer.alloc(12);


  /*
  TRANSACTION ID
  */

  request.writeUInt16BE(
    transaction,
    0
  );


  /*
  PROTOCOL ID
  */

  request.writeUInt16BE(
    0,
    2
  );


  /*
  LENGTH
  */

  request.writeUInt16BE(
    6,
    4
  );


  /*
  UNIT ID
  */

  request[6] =
    UNIT_ID;


  /*
  FUNCTION 03
  */

  request[7] =
    3;


  /*
  REGISTER
  */

  request.writeUInt16BE(
    register,
    8
  );


  /*
  QUANTITY
  */

  request.writeUInt16BE(
    1,
    10
  );


  return request;

}
/*
==================================================
READ MODBUS REGISTER
==================================================
*/

function readRegister(
  register
) {

  return new Promise(

    (
      resolve,
      reject
    ) => {


      const transaction =
        nextTransactionId();


      const socket =
        new net.Socket();


      let completed =
        false;


      let responseBuffer =
        Buffer.alloc(0);


      let connectTimer =
        null;


      let responseTimer =
        null;


      function fail(
        error
      ) {

        if (
          completed
        ) {

          return;

        }


        completed =
          true;


        clearTimeout(
          connectTimer
        );


        clearTimeout(
          responseTimer
        );


        socket.destroy();


        reject(

          error instanceof Error

          ?

          error

          :

          new Error(
            String(error)
          )

        );

      }


      function succeed(
        value
      ) {

        if (
          completed
        ) {

          return;

        }


        completed =
          true;


        clearTimeout(
          connectTimer
        );


        clearTimeout(
          responseTimer
        );


        socket.end();


        resolve(
          value
        );

      }


      connectTimer =

        setTimeout(

          () => {

            fail(

              new Error(

                `TCP connect timeout to ${BMS_HOST}:${BMS_PORT}`

              )

            );

          },

          7000

        );


      socket.setNoDelay(
        true
      );


      socket.once(

        "connect",

        () => {


          clearTimeout(
            connectTimer
          );


          responseTimer =

            setTimeout(

              () => {

                fail(

                  new Error(

                    `Modbus response timeout Unit ${UNIT_ID} Register ${register}`

                  )

                );

              },

              5000

            );


          socket.write(

            buildReadRequest(

              transaction,

              register

            )

          );

        }

      );


      socket.on(

        "data",

        chunk => {


          responseBuffer =

            Buffer.concat(

              [
                responseBuffer,
                chunk
              ]

            );


          if (
            responseBuffer.length <
            7
          ) {

            return;

          }


          const responseTransaction =

            responseBuffer
              .readUInt16BE(0);


          const protocolId =

            responseBuffer
              .readUInt16BE(2);


          const length =

            responseBuffer
              .readUInt16BE(4);


          const responseUnit =

            responseBuffer[6];


          if (
            length < 2 ||
            length > 254
          ) {

            return fail(

              new Error(

                `Invalid Modbus response length ${length}`

              )

            );

          }


          const completeLength =

            6 +
            length;


          if (
            responseBuffer.length <
            completeLength
          ) {

            return;

          }


          if (
            responseTransaction !==
            transaction
          ) {

            return fail(

              new Error(
                "Transaction ID mismatch"
              )

            );

          }


          if (
            protocolId !==
            0
          ) {

            return fail(

              new Error(
                "Protocol ID mismatch"
              )

            );

          }


          if (
            responseUnit !==
            UNIT_ID
          ) {

            return fail(

              new Error(
                "Unit ID mismatch"
              )

            );

          }


          const pdu =

            responseBuffer.subarray(

              7,

              completeLength

            );


          if (
            pdu.length <
            2
          ) {

            return fail(

              new Error(
                "Short Modbus response"
              )

            );

          }


          const functionCode =
            pdu[0];


          if (

            (
              functionCode &
              0x80
            ) !== 0

          ) {

            return fail(

              new Error(

                `Modbus exception ${pdu[1]}`

              )

            );

          }


          if (
            functionCode !==
            3
          ) {

            return fail(

              new Error(
                "Unexpected Modbus function"
              )

            );

          }


          if (

            pdu.length !==
            4

            ||

            pdu[1] !==
            2

          ) {

            return fail(

              new Error(
                "Unexpected FC03 response"
              )

            );

          }


          const value =

            pdu.readUInt16BE(
              2
            );


          succeed(
            value
          );

        }

      );


      socket.on(

        "error",

        error => {

          fail(

            new Error(

              `TCP/Modbus error: ${error.message}`

            )

          );

        }

      );


      socket.on(

        "close",

        () => {

          if (
            !completed
          ) {

            fail(

              new Error(
                "Connection closed before complete response"
              )

            );

          }

        }

      );


      socket.connect(

        BMS_PORT,

        BMS_HOST

      );

    }

  );

}


/*
==================================================
SIGNED 16 BIT
==================================================
*/

function signed16(
  raw
) {

  if (
    raw >=
    32768
  ) {

    return raw -
      65536;

  }


  return raw;

}


/*
==================================================
SCALE VALUE
==================================================
*/

function scaleValue(
  point,
  raw
) {

  if (
    point.kind ===
    "signedAnalog"
  ) {

    return (

      signed16(
        raw
      )

      /

      1000

    );

  }


  return (

    raw /
    1000

  );

}


/*
==================================================
POLL BMS
==================================================
*/

async function pollBms() {

  if (
    polling
  ) {

    return;

  }


  polling =
    true;


  try {

    const results =
      [];


    for (
      const point
      of POINTS
    ) {

      const raw =

        await readRegister(

          point.register

        );


      results.push({

        ...point,

        raw,

        value:

          scaleValue(

            point,

            raw

          )

      });

    }


    latest = {

      ok:
        true,

      status:
        "online",

      error:
        null,

      host:
        BMS_HOST,

      port:
        BMS_PORT,

      unitId:
        UNIT_ID,

      function:
        3,

      results,

      timestamp:

        new Date()
          .toISOString()

    };

  }


  catch (
    error
  ) {

    latest = {

      ...latest,

      ok:
        false,

      status:
        "offline",

      error:
        error.message,

      timestamp:

        new Date()
          .toISOString()

    };

  }


  finally {

    polling =
      false;


    broadcast(
      latest
    );

  }

}


/*
==================================================
LATEST POINT VALUE
==================================================
*/

function getLatestValue(
  id
) {

  if (
    !latest.ok
  ) {

    return null;

  }


  const result =

    latest.results.find(

      point =>
        point.id === id

    );


  if (
    !result
  ) {

    return null;

  }


  const value =
    Number(
      result.value
    );


  return Number.isFinite(
    value
  )

  ?

  value

  :

  null;

}


/*
==================================================
LIVE 15 SECOND SAMPLE
==================================================
*/

function saveLiveSample() {

  if (
    !latest.ok
  ) {

    return;

  }


  const values = {

    in1:
      getLatestValue("in1"),

    in2:
      getLatestValue("in2"),

    in3:
      getLatestValue("in3"),

    in4:
      getLatestValue("in4"),

    in5:
      getLatestValue("in5"),

    diff:
      getLatestValue("diff")

  };


  if (

    Object.values(
      values
    ).some(

      value =>
        value === null

    )

  ) {

    return;

  }


  liveHistory.push({

    timestamp:

      new Date()
        .toISOString(),

    ...values

  });


  if (
    liveHistory.length >
    MAX_LIVE_SAMPLES
  ) {

    liveHistory =

      liveHistory.slice(

        -MAX_LIVE_SAMPLES

      );

  }

}
/*
==================================================
NEXT MINUTE
==================================================
*/

function getNextMinuteBoundary() {

  const next =
    new Date();


  next.setSeconds(
    0,
    0
  );


  next.setMinutes(

    next.getMinutes() +
    1

  );


  return next;

}


/*
==================================================
ARCHIVE ONE-MINUTE HISTORY
==================================================
*/

async function archiveTrendSample(
  recordedAt
) {

  if (
    !db
  ) {

    lastArchiveError =
      "DATABASE_URL not configured";


    return false;

  }


  if (
    !latest.ok
  ) {

    lastArchiveError =
      "BMS offline at archive time";


    return false;

  }


  const values = {

    in1:
      getLatestValue("in1"),

    in2:
      getLatestValue("in2"),

    in3:
      getLatestValue("in3"),

    in4:
      getLatestValue("in4"),

    in5:
      getLatestValue("in5"),

    diff:
      getLatestValue("diff")

  };


  if (

    Object.values(
      values
    ).some(

      value =>
        value === null

    )

  ) {

    lastArchiveError =
      "BMS values unavailable";


    return false;

  }


  try {

    const existing =

      await db.query(

        `

        SELECT id

        FROM trend_history

        WHERE recorded_at = $1

        LIMIT 1

        `,

        [
          recordedAt
        ]

      );


    if (
      existing.rowCount ===
      0
    ) {

      await db.query(

        `

        INSERT INTO trend_history (

          recorded_at,

          planks_in,

          planks_out,

          ambient,

          planks_concrete,

          planks_tank,

          ambient_concrete_diff

        )

        VALUES (

          $1,
          $2,
          $3,
          $4,
          $5,
          $6,
          $7

        )

        `,

        [

          recordedAt,

          values.in1,

          values.in2,

          values.in3,

          values.in4,

          values.in5,

          values.diff

        ]

      );

    }


    databaseConnected =
      true;


    lastArchiveAt =
      recordedAt;


    lastArchiveError =
      null;


    console.log(

      "1-minute trend history saved:",

      recordedAt.toISOString()

    );


    return true;

  }


  catch (
    error
  ) {

    databaseConnected =
      false;


    lastArchiveError =
      error.message;


    console.error(

      "History save failed:",

      error.message

    );


    return false;

  }

}


/*
==================================================
SCHEDULE 1-MINUTE LOGGER
==================================================
*/

function scheduleNextArchive() {

  if (
    archiveTimer
  ) {

    clearTimeout(
      archiveTimer
    );

  }


  const next =
    getNextMinuteBoundary();


  nextArchiveAt =
    next;


  archiveTimer =

    setTimeout(

      async () => {

        await archiveTrendSample(
          next
        );


        scheduleNextArchive();

      },

      Math.max(

        1000,

        next.getTime() -
        Date.now()

      )

    );

}


/*
==================================================
QUERY HISTORY
==================================================
*/

async function queryHistory(
  from,
  to
) {

  if (
    !db
  ) {

    throw new Error(
      "Database not configured"
    );

  }


  const result =

    await db.query(

      `

      SELECT

        recorded_at,

        planks_in,

        planks_out,

        ambient,

        planks_concrete,

        planks_tank,

        ambient_concrete_diff

      FROM trend_history

      WHERE

        recorded_at >= $1

      AND

        recorded_at <= $2

      ORDER BY

        recorded_at ASC

      `,

      [
        from,
        to
      ]

    );


  return result.rows;

}


/*
==================================================
FORMAT ADELAIDE DATE
==================================================
*/

function formatAdelaideDateTime(
  date
) {

  return new Intl.DateTimeFormat(

    "en-AU",

    {

      timeZone:
        "Australia/Adelaide",

      year:
        "numeric",

      month:
        "2-digit",

      day:
        "2-digit",

      hour:
        "2-digit",

      minute:
        "2-digit",

      second:
        "2-digit",

      hour12:
        false

    }

  ).format(
    date
  );

}


/*
==================================================
CSV ESCAPE
==================================================
*/

function csvValue(
  value
) {

  return (

    '"'

    +

    String(
      value ?? ""
    )
      .replace(
        /"/g,
        '""'
      )

    +

    '"'

  );

}


/*
==================================================
BUILD 8-HOUR CSV
==================================================
*/

function buildEmailCsv(
  rows
) {

  const lines = [

    [

      "Date / Time",

      "Planks In Deg.C",

      "Planks Out Deg.C",

      "Ambient Deg.C",

      "Planks Concrete Deg.C",

      "Planks Tank Deg.C",

      "Ambient - Concrete Differential Deg.C"

    ]
      .map(
        csvValue
      )
      .join(",")

  ];


  for (
    const row
    of rows
  ) {

    lines.push(

      [

        formatAdelaideDateTime(

          new Date(
            row.recorded_at
          )

        ),

        row.planks_in,

        row.planks_out,

        row.ambient,

        row.planks_concrete,

        row.planks_tank,

        row.ambient_concrete_diff

      ]
        .map(
          csvValue
        )
        .join(",")

    );

  }


  return lines.join(
    "\r\n"
  );

}
/*
==================================================
8-HOUR REPORT SLOT
==================================================
*/

function getCompletedEightHourPeriod() {

  const now =
    Date.now();


  const periodEndMs =

    Math.floor(

      now /
      EMAIL_REPORT_MS

    )

    *

    EMAIL_REPORT_MS;


  const periodStartMs =

    periodEndMs -
    EMAIL_REPORT_MS;


  return {

    from:
      new Date(
        periodStartMs
      ),

    to:
      new Date(
        periodEndMs
      )

  };

}


/*
==================================================
REPORT KEY
==================================================
*/

function makeReportKey(
  from,
  to
) {

  return (

    from.toISOString()

    +

    "__"

    +

    to.toISOString()

  );

}


/*
==================================================
CHECK IF REPORT ALREADY SENT
==================================================
*/

async function reportAlreadySent(
  reportKey
) {

  const result =

    await db.query(

      `

      SELECT id

      FROM trend_email_reports

      WHERE report_key = $1

      LIMIT 1

      `,

      [
        reportKey
      ]

    );


  return (
    result.rowCount >
    0
  );

}


/*
==================================================
SAVE EMAIL REPORT LOG
==================================================
*/

async function saveEmailReportLog(
  reportKey,
  from,
  to,
  count
) {

  await db.query(

    `

    INSERT INTO trend_email_reports (

      report_key,

      period_from,

      period_to,

      sent_at,

      recipient,

      row_count

    )

    VALUES (

      $1,
      $2,
      $3,
      NOW(),
      $4,
      $5

    )

    ON CONFLICT (report_key)
    DO NOTHING

    `,

    [

      reportKey,

      from,

      to,

      EMAIL_TO,

      count

    ]

  );

}


/*
==================================================
SEND 8-HOUR REPORT
==================================================
*/

async function sendEightHourReport() {

  if (
    !emailTransporter
  ) {

    lastEmailError =
      "Email is not configured";


    return false;

  }


  if (
    !db
  ) {

    lastEmailError =
      "Database is not configured";


    return false;

  }


  const period =

    getCompletedEightHourPeriod();


  const reportKey =

    makeReportKey(

      period.from,

      period.to

    );


  try {

    if (

      await reportAlreadySent(
        reportKey
      )

    ) {

      console.log(

        "8-hour email already sent:",

        reportKey

      );


      return true;

    }


    const rows =

      await queryHistory(

        period.from,

        period.to

      );


    const csv =

      buildEmailCsv(
        rows
      );


    const fromText =

      formatAdelaideDateTime(
        period.from
      );


    const toText =

      formatAdelaideDateTime(
        period.to
      );


    const fileDate =

      period.to
        .toISOString()
        .replace(
          /[:.]/g,
          "-"
        );


    const subject =

      `Greenair TrendLog - 8 Hour History - ${fromText} to ${toText}`;


    const text =

      `BIANCO PRECAST - GREENAIR TRENDLOG

Automatic 8-hour TrendLog report.

Period:
${fromText}
to
${toText}

History records:
${rows.length}

Logging interval:
1 minute

The stored TrendLog data is attached as a CSV file.

Greenair Controls
`;


    await emailTransporter.sendMail({

      from:
        EMAIL_USER,

      to:
        EMAIL_TO,

      subject,

      text,

      attachments: [

        {

          filename:

            `Greenair_TrendLog_8Hour_${fileDate}.csv`,

          content:
            csv,

          contentType:
            "text/csv"

        }

      ]

    });


    await saveEmailReportLog(

      reportKey,

      period.from,

      period.to,

      rows.length

    );


    emailConnected =
      true;


    lastEmailAt =
      new Date();


    lastEmailError =
      null;


    console.log(

      "8-hour TrendLog email sent to:",

      EMAIL_TO

    );


    console.log(

      "Rows emailed:",

      rows.length

    );


    return true;

  }


  catch (
    error
  ) {

    emailConnected =
      false;


    lastEmailError =
      error.message;


    console.error(

      "8-hour email failed:",

      error.message

    );


    return false;

  }

}


/*
==================================================
DROPBOX ACCESS TOKEN
==================================================
*/

async function getDropboxAccessToken() {

  if (
    !DROPBOX_APP_KEY
    ||
    !DROPBOX_APP_SECRET
    ||
    !DROPBOX_REFRESH_TOKEN
  ) {

    throw new Error(
      "Dropbox backup credentials are not fully configured"
    );

  }


  const body =
    new URLSearchParams();


  body.set(
    "grant_type",
    "refresh_token"
  );


  body.set(
    "refresh_token",
    DROPBOX_REFRESH_TOKEN
  );


  body.set(
    "client_id",
    DROPBOX_APP_KEY
  );


  body.set(
    "client_secret",
    DROPBOX_APP_SECRET
  );


  const response =
    await fetch(
      "https://api.dropboxapi.com/oauth2/token",
      {
        method:
          "POST",

        headers:
          {
            "Content-Type":
              "application/x-www-form-urlencoded"
          },

        body:
          body.toString()
      }
    );


  const data =
    await response.json();


  if (
    !response.ok
    ||
    !data.access_token
  ) {

    throw new Error(
      data.error_description
      ||
      data.error
      ||
      "Dropbox access-token refresh failed"
    );

  }


  return data.access_token;

}


/*
==================================================
DROPBOX API
==================================================
*/

async function dropboxApi(
  accessToken,
  endpoint,
  body
) {

  const response =
    await fetch(
      `https://api.dropboxapi.com/2/${endpoint}`,
      {
        method:
          "POST",

        headers:
          {
            "Authorization":
              `Bearer ${accessToken}`,

            "Content-Type":
              "application/json"
          },

        body:
          JSON.stringify(
            body
          )
      }
    );


  const text =
    await response.text();


  let data =
    {};


  if (
    text
  ) {

    try {

      data =
        JSON.parse(
          text
        );

    }

    catch {

      data =
        {
          raw:
            text
        };

    }

  }


  if (
    !response.ok
  ) {

    const summary =
      data.error_summary
      ||
      data.error_description
      ||
      data.error
      ||
      text
      ||
      `Dropbox API error ${response.status}`;


    throw new Error(
      typeof summary === "string"
      ?
      summary
      :
      JSON.stringify(summary)
    );

  }


  return data;

}


/*
==================================================
ENSURE DROPBOX FOLDER
==================================================
*/

async function ensureDropboxFolder(
  accessToken,
  folderPath
) {

  try {

    await dropboxApi(
      accessToken,
      "files/create_folder_v2",
      {
        path:
          folderPath,

        autorename:
          false
      }
    );

  }

  catch (
    error
  ) {

    if (
      !String(
        error.message
      ).includes(
        "path/conflict/folder"
      )
    ) {

      throw error;

    }

  }

}


/*
==================================================
UPLOAD DROPBOX FILE
==================================================
*/

async function uploadDropboxFile(
  accessToken,
  dropboxPath,
  content
) {

  const response =
    await fetch(
      "https://content.dropboxapi.com/2/files/upload",
      {
        method:
          "POST",

        headers:
          {
            "Authorization":
              `Bearer ${accessToken}`,

            "Dropbox-API-Arg":
              JSON.stringify(
                {
                  path:
                    dropboxPath,

                  mode:
                    "overwrite",

                  autorename:
                    false,

                  mute:
                    true
                }
              ),

            "Content-Type":
              "application/octet-stream"
          },

        body:
          content
      }
    );


  const text =
    await response.text();


  if (
    !response.ok
  ) {

    let message =
      text;


    try {

      const data =
        JSON.parse(
          text
        );


      message =
        data.error_summary
        ||
        data.error
        ||
        text;

    }

    catch {}


    throw new Error(
      typeof message === "string"
      ?
      message
      :
      JSON.stringify(message)
    );

  }


  return text
    ?
    JSON.parse(
      text
    )
    :
    {};

}


/*
==================================================
BUILD TREND GRAPH SVG
==================================================
*/

function buildTrendGraphSvg(
  rows,
  from,
  to
) {

  const width =
    1400;

  const height =
    820;

  const left =
    90;

  const right =
    40;

  const top =
    100;

  const bottom =
    90;

  const plotWidth =
    width -
    left -
    right;

  const plotHeight =
    height -
    top -
    bottom;


  const series = [

    {
      key:
        "planks_in",

      label:
        "Planks In"
    },

    {
      key:
        "planks_out",

      label:
        "Planks Out"
    },

    {
      key:
        "ambient",

      label:
        "Ambient"
    },

    {
      key:
        "planks_concrete",

      label:
        "Planks Concrete"
    },

    {
      key:
        "planks_tank",

      label:
        "Planks Tank"
    },

    {
      key:
        "ambient_concrete_diff",

      label:
        "Ambient - Concrete Differential"
    }

  ];


  const colours = [
    "#1565c0",
    "#2e7d32",
    "#ef6c00",
    "#6a1b9a",
    "#00838f",
    "#c62828"
  ];


  const values =
    [];


  for (
    const row
    of rows
  ) {

    for (
      const item
      of series
    ) {

      const value =
        Number(
          row[item.key]
        );


      if (
        Number.isFinite(
          value
        )
      ) {

        values.push(
          value
        );

      }

    }

  }


  let minValue =
    values.length
    ?
    Math.min(
      ...values
    )
    :
    0;


  let maxValue =
    values.length
    ?
    Math.max(
      ...values
    )
    :
    1;


  if (
    minValue ===
    maxValue
  ) {

    minValue -=
      1;

    maxValue +=
      1;

  }


  const padding =
    Math.max(
      0.5,
      (
        maxValue -
        minValue
      )
      *
      0.08
    );


  minValue -=
    padding;

  maxValue +=
    padding;


  const xFor =
    index =>

      left

      +

      (
        rows.length <= 1
        ?
        0
        :
        index /
        (
          rows.length -
          1
        )
      )

      *
      plotWidth;


  const yFor =
    value =>

      top

      +

      (
        maxValue -
        value
      )

      /
      (
        maxValue -
        minValue
      )

      *
      plotHeight;


  const escapeXml =
    value =>

      String(
        value
      )
        .replaceAll(
          "&",
          "&amp;"
        )
        .replaceAll(
          "<",
          "&lt;"
        )
        .replaceAll(
          ">",
          "&gt;"
        );


  const svg =
    [];


  svg.push(
    `<?xml version="1.0" encoding="UTF-8"?>`
  );


  svg.push(
    `<svg xmlns="http://www.w3.org/2000/svg" width="${width}" height="${height}" viewBox="0 0 ${width} ${height}">`
  );


  svg.push(
    `<rect width="100%" height="100%" fill="white"/>`
  );


  svg.push(
    `<text x="${left}" y="42" font-family="Arial, sans-serif" font-size="28" font-weight="700">BIANCO PRECAST - GREENAIR TRENDLOG</text>`
  );


  svg.push(
    `<text x="${left}" y="72" font-family="Arial, sans-serif" font-size="16">8 Hour Trend Graph: ${escapeXml(formatAdelaideDateTime(from))} to ${escapeXml(formatAdelaideDateTime(to))}</text>`
  );


  for (
    let i = 0;
    i <= 6;
    i += 1
  ) {

    const y =
      top +
      (
        i /
        6
      )
      *
      plotHeight;


    const value =
      maxValue -
      (
        i /
        6
      )
      *
      (
        maxValue -
        minValue
      );


    svg.push(
      `<line x1="${left}" y1="${y.toFixed(1)}" x2="${left + plotWidth}" y2="${y.toFixed(1)}" stroke="#d0d0d0" stroke-width="1"/>`
    );


    svg.push(
      `<text x="${left - 12}" y="${(y + 5).toFixed(1)}" text-anchor="end" font-family="Arial, sans-serif" font-size="13">${value.toFixed(1)} °C</text>`
    );

  }


  svg.push(
    `<line x1="${left}" y1="${top}" x2="${left}" y2="${top + plotHeight}" stroke="#333" stroke-width="2"/>`
  );


  svg.push(
    `<line x1="${left}" y1="${top + plotHeight}" x2="${left + plotWidth}" y2="${top + plotHeight}" stroke="#333" stroke-width="2"/>`
  );


  series.forEach(
    (
      item,
      seriesIndex
    ) => {

      const points =
        [];


      rows.forEach(
        (
          row,
          index
        ) => {

          const value =
            Number(
              row[item.key]
            );


          if (
            Number.isFinite(
              value
            )
          ) {

            points.push(
              `${xFor(index).toFixed(1)},${yFor(value).toFixed(1)}`
            );

          }

        }
      );


      if (
        points.length
      ) {

        svg.push(
          `<polyline fill="none" stroke="${colours[seriesIndex]}" stroke-width="2.5" points="${points.join(" ")}"/>`
        );

      }


      const legendX =
        left +
        (
          seriesIndex % 3
        )
        *
        390;

      const legendY =
        height -
        55 +
        (
          seriesIndex >= 3
          ?
          24
          :
          0
        );


      svg.push(
        `<line x1="${legendX}" y1="${legendY - 5}" x2="${legendX + 28}" y2="${legendY - 5}" stroke="${colours[seriesIndex]}" stroke-width="4"/>`
      );


      svg.push(
        `<text x="${legendX + 38}" y="${legendY}" font-family="Arial, sans-serif" font-size="14">${escapeXml(item.label)}</text>`
      );

    }
  );


  if (
    !rows.length
  ) {

    svg.push(
      `<text x="${width / 2}" y="${height / 2}" text-anchor="middle" font-family="Arial, sans-serif" font-size="28">No stored trend records for this period</text>`
    );

  }


  svg.push(
    `</svg>`
  );


  return svg.join(
    "\n"
  );

}


/*
==================================================
DROPBOX 8-HOUR BACKUP
==================================================
*/

async function backupEightHoursToDropbox(
  to = new Date()
) {

  const from =
    new Date(
      to.getTime()
      -
      DROPBOX_BACKUP_MS
    );


  try {

    const rows =
      await queryHistory(
        from,
        to
      );


    const csv =
      buildEmailCsv(
        rows
      );


    const graph =
      buildTrendGraphSvg(
        rows,
        from,
        to
      );


    const accessToken =
      await getDropboxAccessToken();


    const parts =
      new Intl.DateTimeFormat(
        "en-AU",
        {
          timeZone:
            "Australia/Adelaide",

          year:
            "numeric",

          month:
            "2-digit",

          day:
            "2-digit",

          hour:
            "2-digit",

          minute:
            "2-digit",

          hour12:
            false
        }
      )
        .formatToParts(
          to
        )
        .reduce(
          (
            result,
            part
          ) => {

            if (
              part.type !==
              "literal"
            ) {

              result[part.type] =
                part.value;

            }


            return result;

          },
          {}
        );


    const year =
      parts.year;

    const month =
      parts.month;

    const stamp =
      `${parts.year}-${parts.month}-${parts.day}_${parts.hour}-${parts.minute}`;


    const base =
      `/Bianco Precast/${year}/${month}`;


    const folders = [
      "/Bianco Precast",
      `/Bianco Precast/${year}`,
      base,
      `${base}/Trend Data`,
      `${base}/Graphs`
    ];


    for (
      const folder
      of folders
    ) {

      await ensureDropboxFolder(
        accessToken,
        folder
      );

    }


    const csvPath =
      `${base}/Trend Data/Greenair_TrendLog_8Hour_${stamp}.csv`;


    const graphPath =
      `${base}/Graphs/Greenair_TrendLog_8Hour_${stamp}.svg`;


    await uploadDropboxFile(
      accessToken,
      csvPath,
      csv
    );


    await uploadDropboxFile(
      accessToken,
      graphPath,
      graph
    );


    lastDropboxBackupAt =
      new Date();


    lastDropboxBackupError =
      null;


    console.log(
      "Dropbox 8-hour backup complete."
    );


    console.log(
      "Dropbox CSV:",
      csvPath
    );


    console.log(
      "Dropbox graph:",
      graphPath
    );


    console.log(
      "Dropbox rows:",
      rows.length
    );


    return {
      ok:
        true,

      rows:
        rows.length,

      from:
        from.toISOString(),

      to:
        to.toISOString(),

      csvPath,

      graphPath
    };

  }

  catch (
    error
  ) {

    lastDropboxBackupError =
      error.message;


    console.error(
      "Dropbox 8-hour backup failed:",
      error.message
    );


    throw error;

  }

}


/*
==================================================
SCHEDULE DROPBOX BACKUP
==================================================
*/

function scheduleNextDropboxBackup() {

  if (
    dropboxBackupTimer
  ) {

    clearTimeout(
      dropboxBackupTimer
    );

  }


  const next =
    getNextEightHourBoundary();


  nextDropboxBackupAt =
    next;


  const delay =
    Math.max(
      1000,
      next.getTime() -
      Date.now()
    );


  console.log(
    "Next 8-hour Dropbox backup:",
    next.toISOString()
  );


  dropboxBackupTimer =
    setTimeout(
      async () => {

        try {

          await backupEightHoursToDropbox(
            next
          );

        }

        catch {}


        scheduleNextDropboxBackup();

      },
      delay
    );

}


/*
==================================================
NEXT 8-HOUR BOUNDARY
==================================================
*/

function getNextEightHourBoundary() {

  const now =
    Date.now();


  const next =

    (

      Math.floor(

        now /
        EMAIL_REPORT_MS

      )

      +

      1

    )

    *

    EMAIL_REPORT_MS;


  return new Date(
    next
  );

}


/*
==================================================
SCHEDULE EMAIL
==================================================
*/

function scheduleNextEmail() {

  if (
    emailTimer
  ) {

    clearTimeout(
      emailTimer
    );

  }


  const next =

    getNextEightHourBoundary();


  nextEmailAt =
    next;


  const delay =

    Math.max(

      1000,

      next.getTime() -
      Date.now()

    );


  console.log(

    "Next 8-hour email:",

    next.toISOString()

  );


  emailTimer =

    setTimeout(

      async () => {

        await sendEightHourReport();


        scheduleNextEmail();

      },

      delay

    );

}


/*
==================================================
HISTORY RANGE
==================================================
*/

async function getHistoryRange() {

  if (
    !db
  ) {

    return {

      first:
        null,

      last:
        null,

      count:
        0

    };

  }


  const result =

    await db.query(`

      SELECT

        MIN(recorded_at) AS first,

        MAX(recorded_at) AS last,

        COUNT(*)::bigint AS count

      FROM trend_history

    `);


  const row =
    result.rows[0];


  return {

    first:

      row.first

      ?

      new Date(
        row.first
      ).toISOString()

      :

      null,


    last:

      row.last

      ?

      new Date(
        row.last
      ).toISOString()

      :

      null,


    count:

      Number(
        row.count
      )

  };

}
/*
==================================================
LOGIN USER
==================================================
*/

async function loginUser(
  request,
  response
) {

  if (
    !db
  ) {

    return sendJson(

      response,

      {
        ok: false,
        error: "Database unavailable"
      },

      503

    );

  }


  try {

    const body =
      await readJsonBody(
        request
      );


    const username =
      normaliseUsername(
        body.username
      );


    const password =
      String(
        body.password || ""
      );


    if (
      !username ||
      !password
    ) {

      return sendJson(

        response,

        {
          ok: false,
          error: "Username and password are required"
        },

        400

      );

    }


    const result =

      await db.query(

        `

        SELECT

          id,
          username,
          password_hash,
          role,
          active

        FROM app_users

        WHERE
          lower(username) =
          lower($1)

        LIMIT 1

        `,

        [
          username
        ]

      );


    if (
      result.rowCount === 0
    ) {

      return sendJson(

        response,

        {
          ok: false,
          error: "Incorrect username or password"
        },

        401

      );

    }


    const user =
      result.rows[0];


    if (
      !user.active
    ) {

      return sendJson(

        response,

        {
          ok: false,
          error: "This account has been disabled"
        },

        403

      );

    }


    if (

      !verifyPassword(

        password,

        user.password_hash

      )

    ) {

      return sendJson(

        response,

        {
          ok: false,
          error: "Incorrect username or password"
        },

        401

      );

    }


    /*
    REMOVE ANY EXPIRED SESSIONS
    */

    await db.query(`

      DELETE FROM app_sessions

      WHERE expires_at <= NOW()

    `);


    /*
    CREATE NEW SESSION
    */

    await createSession(

      response,

      user.id

    );


    /*
    RECORD LOGIN TIME
    */

    await db.query(

      `

      UPDATE app_users

      SET
        last_login_at = NOW()

      WHERE id = $1

      `,

      [
        user.id
      ]

    );


    return sendJson(

      response,

      {

        ok:
          true,

        user: {

          id:
            user.id,

          username:
            user.username,

          role:
            user.role

        }

      }

    );

  }


  catch (
    error
  ) {

    console.error(

      "Login failed:",

      error.message

    );


    return sendJson(

      response,

      {
        ok: false,
        error: "Unable to log in"
      },

      500

    );

  }

}


/*
==================================================
LOGOUT USER
==================================================
*/

async function logoutUser(
  request,
  response
) {

  try {

    await deleteSession(
      request
    );

  }

  catch (
    error
  ) {

    console.error(

      "Logout session cleanup failed:",

      error.message

    );

  }


  clearSessionCookie(
    response
  );


  return sendJson(

    response,

    {
      ok: true
    }

  );

}


/*
==================================================
CURRENT USER
==================================================
*/

async function currentUserApi(
  request,
  response
) {

  try {

    const user =

      await getAuthenticatedUser(
        request
      );


    if (
      !user
    ) {

      return sendJson(

        response,

        {
          ok: false,
          authenticated: false
        },

        401

      );

    }


    return sendJson(

      response,

      {

        ok:
          true,

        authenticated:
          true,

        user: {

          id:
            user.id,

          username:
            user.username,

          role:
            user.role

        }

      }

    );

  }


  catch (
    error
  ) {

    return sendJson(

      response,

      {
        ok: false,
        error: error.message
      },

      500

    );

  }

}


/*
==================================================
REQUIRE LOGIN
==================================================
*/

async function requireAuthenticatedUser(
  request,
  response
) {

  try {

    const user =

      await getAuthenticatedUser(
        request
      );


    if (
      !user
    ) {

      sendJson(

        response,

        {
          ok:
            false,

          error:
            "Authentication required"

        },

        401

      );


      return null;

    }


    return user;

  }


  catch (
    error
  ) {

    sendJson(

      response,

      {
        ok: false,
        error: "Authentication check failed"
      },

      500

    );


    return null;

  }

}


/*
==================================================
REQUIRE MASTER
==================================================
*/

async function requireMasterUser(
  request,
  response
) {

  const user =

    await requireAuthenticatedUser(

      request,

      response

    );


  if (
    !user
  ) {

    return null;

  }


  if (
    user.role !==
    "master"
  ) {

    sendJson(

      response,

      {
        ok: false,
        error: "Master access required"
      },

      403

    );


    return null;

  }


  return user;

}


/*
==================================================
LIST USERS
MASTER ONLY
==================================================
*/

async function listUsersApi(
  request,
  response
) {

  const master =

    await requireMasterUser(

      request,

      response

    );


  if (
    !master
  ) {

    return;

  }


  try {

    const result =

      await db.query(`

        SELECT

          id,
          username,
          role,
          active,
          created_at,
          last_login_at

        FROM app_users

        ORDER BY

          CASE
            WHEN role = 'master'
            THEN 0
            ELSE 1
          END,

          lower(username)

      `);


    return sendJson(

      response,

      {

        ok:
          true,

        users:

          result.rows.map(

            user => ({

              id:
                user.id,

              username:
                user.username,

              role:
                user.role,

              active:
                user.active,

              createdAt:

                user.created_at

                ?

                new Date(
                  user.created_at
                ).toISOString()

                :

                null,

              lastLoginAt:

                user.last_login_at

                ?

                new Date(
                  user.last_login_at
                ).toISOString()

                :

                null

            })

          )

      }

    );

  }


  catch (
    error
  ) {

    return sendJson(

      response,

      {
        ok: false,
        error: error.message
      },

      500

    );

  }

}


/*
==================================================
CREATE USER
MASTER ONLY
==================================================
*/

async function createUserApi(
  request,
  response
) {

  const master =

    await requireMasterUser(

      request,

      response

    );


  if (
    !master
  ) {

    return;

  }


  try {

    const body =
      await readJsonBody(
        request
      );


    const username =

      normaliseUsername(
        body.username
      );


    const password =

      String(
        body.password || ""
      );


    /*
    USERNAME RULES
    */

    if (
      username.length < 3
    ) {

      return sendJson(

        response,

        {
          ok: false,
          error: "Username must be at least 3 characters"
        },

        400

      );

    }


    if (

      !/^[A-Za-z0-9._-]+$/.test(
        username
      )

    ) {

      return sendJson(

        response,

        {
          ok: false,
          error: "Username can only contain letters, numbers, dot, dash and underscore"
        },

        400

      );

    }


    /*
    PASSWORD RULES
    */

    if (
      password.length < 8
    ) {

      return sendJson(

        response,

        {
          ok: false,
          error: "Password must be at least 8 characters"
        },

        400

      );

    }


    /*
    CHECK DUPLICATE USERNAME
    */

    const existing =

      await db.query(

        `

        SELECT id

        FROM app_users

        WHERE
          lower(username) =
          lower($1)

        LIMIT 1

        `,

        [
          username
        ]

      );


    if (
      existing.rowCount > 0
    ) {

      return sendJson(

        response,

        {
          ok: false,
          error: "That username already exists"
        },

        409

      );

    }


    /*
    NORMAL ACCOUNTS ARE ALWAYS
    CREATED AS ROLE = USER
    */

    const created =

      await db.query(

        `

        INSERT INTO app_users (

          username,
          password_hash,
          role,
          active

        )

        VALUES (

          $1,
          $2,
          'user',
          TRUE

        )

        RETURNING

          id,
          username,
          role,
          active,
          created_at

        `,

        [

          username,

          hashPassword(
            password
          )

        ]

      );


    console.log(

      "TrendLog user created:",

      username,

      "by",

      master.username

    );


    return sendJson(

      response,

      {

        ok:
          true,

        user:
          created.rows[0]

      },

      201

    );

  }


  catch (
    error
  ) {

    console.error(

      "Create user failed:",

      error.message

    );


    return sendJson(

      response,

      {
        ok: false,
        error: error.message
      },

      500

    );

  }

}


/*
==================================================
RESET USER PASSWORD
MASTER ONLY
==================================================
*/

async function resetUserPasswordApi(
  request,
  response
) {

  const master =

    await requireMasterUser(

      request,

      response

    );


  if (
    !master
  ) {

    return;

  }


  try {

    const body =

      await readJsonBody(
        request
      );


    const userId =

      Number(
        body.userId
      );


    const password =

      String(
        body.password || ""
      );


    if (
      !Number.isInteger(
        userId
      )
    ) {

      return sendJson(

        response,

        {
          ok: false,
          error: "Invalid user"
        },

        400

      );

    }


    if (
      password.length < 8
    ) {

      return sendJson(

        response,

        {
          ok: false,
          error: "Password must be at least 8 characters"
        },

        400

      );

    }


    const userResult =

      await db.query(

        `

        SELECT

          id,
          username,
          role

        FROM app_users

        WHERE id = $1

        LIMIT 1

        `,

        [
          userId
        ]

      );


    if (
      userResult.rowCount === 0
    ) {

      return sendJson(

        response,

        {
          ok: false,
          error: "User not found"
        },

        404

      );

    }


    const target =
      userResult.rows[0];


    /*
    DON'T RESET A MASTER ACCOUNT
    FROM USER MANAGEMENT.

    THE MASTER PASSWORD IS MANAGED
    THROUGH THE MASTER LOGIN SETUP.
    */

    if (
      target.role ===
      "master"
    ) {

      return sendJson(

        response,

        {
          ok: false,
          error: "Master password cannot be reset from User Management"
        },

        403

      );

    }


    await db.query(

      `

      UPDATE app_users

      SET
        password_hash = $1

      WHERE id = $2

      `,

      [

        hashPassword(
          password
        ),

        userId

      ]

    );


    /*
    LOG THEM OUT OF ALL DEVICES
    AFTER PASSWORD RESET
    */

    await db.query(

      `

      DELETE FROM app_sessions

      WHERE user_id = $1

      `,

      [
        userId
      ]

    );


    console.log(

      "Password reset for:",

      target.username,

      "by",

      master.username

    );


    return sendJson(

      response,

      {
        ok: true
      }

    );

  }


  catch (
    error
  ) {

    return sendJson(

      response,

      {
        ok: false,
        error: error.message
      },

      500

    );

  }

}


/*
==================================================
ENABLE / DISABLE USER
MASTER ONLY
==================================================
*/

async function setUserActiveApi(
  request,
  response
) {

  const master =

    await requireMasterUser(

      request,

      response

    );


  if (
    !master
  ) {

    return;

  }


  try {

    const body =

      await readJsonBody(
        request
      );


    const userId =

      Number(
        body.userId
      );


    const active =

      Boolean(
        body.active
      );


    const userResult =

      await db.query(

        `

        SELECT

          id,
          username,
          role

        FROM app_users

        WHERE id = $1

        LIMIT 1

        `,

        [
          userId
        ]

      );


    if (
      userResult.rowCount === 0
    ) {

      return sendJson(

        response,

        {
          ok: false,
          error: "User not found"
        },

        404

      );

    }


    const target =
      userResult.rows[0];


    /*
    MASTER CANNOT BE DISABLED
    */

    if (
      target.role ===
      "master"
    ) {

      return sendJson(

        response,

        {
          ok: false,
          error: "The Master account cannot be disabled"
        },

        403

      );

    }


    await db.query(

      `

      UPDATE app_users

      SET
        active = $1

      WHERE id = $2

      `,

      [
        active,
        userId
      ]

    );


    /*
    IF DISABLED,
    REMOVE ALL SESSIONS
    */

    if (
      !active
    ) {

      await db.query(

        `

        DELETE FROM app_sessions

        WHERE user_id = $1

        `,

        [
          userId
        ]

      );

    }


    console.log(

      active
        ? "User enabled:"
        : "User disabled:",

      target.username,

      "by",

      master.username

    );


    return sendJson(

      response,

      {
        ok: true,
        active
      }

    );

  }


  catch (
    error
  ) {

    return sendJson(

      response,

      {
        ok: false,
        error: error.message
      },

      500

    );

  }

}


/*
==================================================
DELETE USER
MASTER ONLY
==================================================
*/

async function deleteUserApi(
  request,
  response
) {

  const master =

    await requireMasterUser(

      request,

      response

    );


  if (
    !master
  ) {

    return;

  }


  try {

    const body =

      await readJsonBody(
        request
      );


    const userId =

      Number(
        body.userId
      );


    const userResult =

      await db.query(

        `

        SELECT

          id,
          username,
          role

        FROM app_users

        WHERE id = $1

        LIMIT 1

        `,

        [
          userId
        ]

      );


    if (
      userResult.rowCount === 0
    ) {

      return sendJson(

        response,

        {
          ok: false,
          error: "User not found"
        },

        404

      );

    }


    const target =
      userResult.rows[0];


    /*
    MASTER ACCOUNT CANNOT BE DELETED
    */

    if (
      target.role ===
      "master"
    ) {

      return sendJson(

        response,

        {
          ok: false,
          error: "The Master account cannot be deleted"
        },

        403

      );

    }


    await db.query(

      `

      DELETE FROM app_users

      WHERE id = $1

      `,

      [
        userId
      ]

    );


    console.log(

      "TrendLog user deleted:",

      target.username,

      "by",

      master.username

    );


    return sendJson(

      response,

      {
        ok: true
      }

    );

  }


  catch (
    error
  ) {

    return sendJson(

      response,

      {
        ok: false,
        error: error.message
      },

      500

    );

  }

}
/*
==================================================
SERVER SENT EVENTS
==================================================
*/

function broadcast(
  state
) {

  const payload =

    `event: state\n`

    +

    `data: ${JSON.stringify(state)}\n\n`;


  for (
    const response
    of streamClients
  ) {

    try {

      response.write(
        payload
      );

    }

    catch {

      streamClients.delete(
        response
      );

    }

  }

}


/*
==================================================
SEND JSON
==================================================
*/

function sendJson(
  response,
  object,
  status = 200
) {

  const data =

    Buffer.from(

      JSON.stringify(
        object
      )

    );


  response.writeHead(

    status,

    {

      "Content-Type":
        "application/json; charset=utf-8",

      "Content-Length":
        data.length,

      "Cache-Control":
        "no-store",

      "X-Content-Type-Options":
        "nosniff",

      "X-Frame-Options":
        "DENY"

    }

  );


  response.end(
    data
  );

}


/*
==================================================
REDIRECT
==================================================
*/

function redirect(
  response,
  location
) {

  response.writeHead(

    302,

    {

      "Location":
        location,

      "Cache-Control":
        "no-store"

    }

  );


  response.end();

}


/*
==================================================
CONTENT TYPES
==================================================
*/

function getContentType(
  filePath
) {

  if (
    filePath.endsWith(
      ".html"
    )
  ) {

    return "text/html; charset=utf-8";

  }


  if (
    filePath.endsWith(
      ".css"
    )
  ) {

    return "text/css; charset=utf-8";

  }


  if (
    filePath.endsWith(
      ".js"
    )
  ) {

    return "application/javascript; charset=utf-8";

  }


  if (
    filePath.endsWith(
      ".json"
    )
  ) {

    return "application/json; charset=utf-8";

  }


  if (
    filePath.endsWith(
      ".png"
    )
  ) {

    return "image/png";

  }


  if (

    filePath.endsWith(
      ".jpg"
    )

    ||

    filePath.endsWith(
      ".jpeg"
    )

  ) {

    return "image/jpeg";

  }


  if (
    filePath.endsWith(
      ".svg"
    )
  ) {

    return "image/svg+xml";

  }


  return "application/octet-stream";

}


/*
==================================================
SERVE ONE PUBLIC FILE
==================================================
*/

function servePublicFile(
  response,
  relativePath
) {

  const publicRoot =

    path.resolve(

      __dirname,

      "public"

    );


  const filePath =

    path.resolve(

      publicRoot,

      relativePath

    );


  /*
  STOP PATH TRAVERSAL
  */

  if (

    !filePath.startsWith(
      publicRoot
    )

  ) {

    response.writeHead(
      403
    );


    response.end(
      "Forbidden"
    );


    return;

  }


  fs.readFile(

    filePath,

    (
      error,
      data
    ) => {


      if (
        error
      ) {

        response.writeHead(
          404
        );


        response.end(
          "Not found"
        );


        return;

      }


      response.writeHead(

        200,

        {

          "Content-Type":
            getContentType(
              filePath
            ),

          "Cache-Control":
            "no-store",

          "X-Content-Type-Options":
            "nosniff",

          "X-Frame-Options":
            "DENY"

        }

      );


      response.end(
        data
      );

    }

  );

}


/*
==================================================
SERVE PROTECTED STATIC FILE
==================================================
*/

function serveProtectedStatic(
  request,
  response
) {

  const url =

    new URL(

      request.url,

      `http://${request.headers.host || "localhost"}`

    );


  let requested =

    url.pathname === "/"

    ?

    "/index.html"

    :

    url.pathname;


  /*
  NEVER LET STATIC ACCESS
  SERVE LOGIN OR ADMIN VIA
  RANDOM PATH TRICKS
  */

  if (
    requested ===
    "/login.html"
  ) {

    return servePublicFile(

      response,

      "login.html"

    );

  }


  if (
    requested ===
    "/admin.html"
  ) {

    return servePublicFile(

      response,

      "admin.html"

    );

  }


  requested =

    requested.replace(

      /^\/+/,

      ""

    );


  servePublicFile(

    response,

    requested

  );

}


/*
==================================================
HTTP SERVER
==================================================
*/

const server =

  http.createServer(

    async (
      request,
      response
    ) => {


      const url =

        new URL(

          request.url,

          `http://${request.headers.host || "localhost"}`

        );


      /*
      BASIC SECURITY HEADERS
      */

      response.setHeader(

        "Referrer-Policy",

        "same-origin"

      );


      response.setHeader(

        "X-Content-Type-Options",

        "nosniff"

      );


      response.setHeader(

        "X-Frame-Options",

        "DENY"

      );


      /*
      ================================================
      PUBLIC LOGIN PAGE
      ================================================
      */

      if (

        url.pathname ===
        "/login"

        ||

        url.pathname ===
        "/login.html"

      ) {

        try {

          const existingUser =

            await getAuthenticatedUser(
              request
            );


          if (
            existingUser
          ) {

            return redirect(

              response,

              "/"

            );

          }

        }

        catch {}


        return servePublicFile(

          response,

          "login.html"

        );

      }


      /*
      ================================================
      LOGIN API
      PUBLIC
      ================================================
      */

      if (

        url.pathname ===
        "/api/auth/login"

        &&

        request.method ===
        "POST"

      ) {

        return loginUser(

          request,

          response

        );

      }


      /*
      ================================================
      EVERYTHING BELOW REQUIRES LOGIN
      ================================================
      */

      let authUser =
        null;


      try {

        authUser =

          await getAuthenticatedUser(
            request
          );

      }


      catch (
        error
      ) {

        console.error(

          "Authentication lookup failed:",

          error.message

        );


        if (
          url.pathname.startsWith(
            "/api/"
          )
        ) {

          return sendJson(

            response,

            {

              ok:
                false,

              error:
                "Authentication database error"

            },

            500

          );

        }


        return redirect(

          response,

          "/login"

        );

      }


      if (
        !authUser
      ) {

        if (
          url.pathname.startsWith(
            "/api/"
          )
        ) {

          return sendJson(

            response,

            {

              ok:
                false,

              error:
                "Login required"

            },

            401

          );

        }


        return redirect(

          response,

          "/login"

        );

      }


      /*
      ================================================
      DROPBOX CONNECT
      MASTER ONLY
      ================================================
      */

      if (
        url.pathname ===
        "/dropbox/connect"
      ) {

        if (
          authUser.role !==
          "master"
        ) {

          response.writeHead(
            403,
            {
              "Content-Type":
                "text/plain; charset=utf-8"
            }
          );

          response.end(
            "Master access required."
          );

          return;

        }


        if (
          !DROPBOX_APP_KEY
          ||
          !DROPBOX_APP_SECRET
        ) {

          response.writeHead(
            500,
            {
              "Content-Type":
                "text/plain; charset=utf-8"
            }
          );

          response.end(
            "Dropbox is not configured in Render."
          );

          return;

        }


        const state =
          createDropboxOAuthState();


        const authorizeUrl =
          new URL(
            "https://www.dropbox.com/oauth2/authorize"
          );


        authorizeUrl.searchParams.set(
          "client_id",
          DROPBOX_APP_KEY
        );


        authorizeUrl.searchParams.set(
          "response_type",
          "code"
        );


        authorizeUrl.searchParams.set(
          "token_access_type",
          "offline"
        );


        authorizeUrl.searchParams.set(
          "redirect_uri",
          DROPBOX_REDIRECT_URI
        );


        authorizeUrl.searchParams.set(
          "state",
          state
        );


        return redirect(
          response,
          authorizeUrl.toString()
        );

      }


      /*
      ================================================
      DROPBOX CALLBACK
      MASTER ONLY
      ================================================
      */

      if (
        url.pathname ===
        "/dropbox/callback"
      ) {

        if (
          authUser.role !==
          "master"
        ) {

          response.writeHead(
            403,
            {
              "Content-Type":
                "text/plain; charset=utf-8"
            }
          );

          response.end(
            "Master access required."
          );

          return;

        }


        const errorText =
          url.searchParams.get(
            "error_description"
          )
          ||
          url.searchParams.get(
            "error"
          );


        if (
          errorText
        ) {

          response.writeHead(
            400,
            {
              "Content-Type":
                "text/plain; charset=utf-8"
            }
          );

          response.end(
            `Dropbox authorization failed: ${errorText}`
          );

          return;

        }


        const code =
          url.searchParams.get(
            "code"
          );


        const state =
          url.searchParams.get(
            "state"
          );


        if (
          !code
          ||
          !state
          ||
          !consumeDropboxOAuthState(
            state
          )
        ) {

          response.writeHead(
            400,
            {
              "Content-Type":
                "text/plain; charset=utf-8"
            }
          );

          response.end(
            "Invalid or expired Dropbox authorization request."
          );

          return;

        }


        try {

          const tokenBody =
            new URLSearchParams();


          tokenBody.set(
            "code",
            code
          );


          tokenBody.set(
            "grant_type",
            "authorization_code"
          );


          tokenBody.set(
            "client_id",
            DROPBOX_APP_KEY
          );


          tokenBody.set(
            "client_secret",
            DROPBOX_APP_SECRET
          );


          tokenBody.set(
            "redirect_uri",
            DROPBOX_REDIRECT_URI
          );


          const tokenResponse =
            await fetch(
              "https://api.dropboxapi.com/oauth2/token",
              {
                method:
                  "POST",

                headers:
                  {
                    "Content-Type":
                      "application/x-www-form-urlencoded"
                  },

                body:
                  tokenBody.toString()
              }
            );


          const tokenData =
            await tokenResponse.json();


          if (
            !tokenResponse.ok
          ) {

            throw new Error(
              tokenData.error_description
              ||
              tokenData.error
              ||
              "Dropbox token exchange failed"
            );

          }


          if (
            !tokenData.refresh_token
          ) {

            throw new Error(
              "Dropbox did not return a refresh token. Reconnect using offline access."
            );

          }


          const safeToken =
            String(
              tokenData.refresh_token
            )
              .replaceAll(
                "&",
                "&amp;"
              )
              .replaceAll(
                "<",
                "&lt;"
              )
              .replaceAll(
                ">",
                "&gt;"
              )
              .replaceAll(
                '"',
                "&quot;"
              );


          response.writeHead(
            200,
            {
              "Content-Type":
                "text/html; charset=utf-8",

              "Cache-Control":
                "no-store"
            }
          );


          response.end(
            `<!doctype html>
<html>
<head>
<meta charset="utf-8">
<title>Dropbox Connected</title>
<style>
body{font-family:Arial,Helvetica,sans-serif;background:#f3f5f4;padding:30px;color:#222}
.card{max-width:760px;margin:auto;background:#fff;border:1px solid #ddd;border-radius:10px;padding:24px;box-shadow:0 6px 24px rgba(0,0,0,.08)}
h1{color:#1b5e20;margin-top:0}
.token{word-break:break-all;background:#f5f5f5;border:1px solid #ccc;border-radius:6px;padding:14px;font-family:monospace;font-size:13px}
.warning{color:#b00020;font-weight:700}
</style>
</head>
<body>
<div class="card">
<h1>Dropbox connected successfully</h1>
<p>Your Dropbox refresh token has been created.</p>
<p class="warning">Keep this token private. Do not paste it into ChatGPT.</p>
<p>Copy the token below and add it to Render as <strong>DROPBOX_REFRESH_TOKEN</strong>.</p>
<div class="token">${safeToken}</div>
</div>
</body>
</html>`
          );


          return;

        }


        catch (
          error
        ) {

          response.writeHead(
            500,
            {
              "Content-Type":
                "text/plain; charset=utf-8",

              "Cache-Control":
                "no-store"
            }
          );


          response.end(
            `Dropbox connection failed: ${error.message}`
          );


          return;

        }

      }


      /*
      ================================================
      DROPBOX BACKUP STATUS
      MASTER ONLY
      ================================================
      */

      if (
        url.pathname ===
        "/api/dropbox/status"
      ) {

        if (
          authUser.role !==
          "master"
        ) {

          return sendJson(
            response,
            {
              ok:
                false,

              error:
                "Master access required"
            },
            403
          );

        }


        return sendJson(
          response,
          {
            ok:
              true,

            configured:
              Boolean(
                DROPBOX_APP_KEY
                &&
                DROPBOX_APP_SECRET
                &&
                DROPBOX_REFRESH_TOKEN
              ),

            everyHours:
              DROPBOX_BACKUP_HOURS,

            lastBackupAt:
              lastDropboxBackupAt
              ?
              lastDropboxBackupAt.toISOString()
              :
              null,

            nextBackupAt:
              nextDropboxBackupAt
              ?
              nextDropboxBackupAt.toISOString()
              :
              null,

            lastError:
              lastDropboxBackupError
          }
        );

      }


      /*
      ================================================
      DROPBOX BACKUP TEST
      MASTER ONLY
      ================================================
      */

      if (
        url.pathname ===
        "/api/dropbox/test"
      ) {

        if (
          authUser.role !==
          "master"
        ) {

          return sendJson(
            response,
            {
              ok:
                false,

              error:
                "Master access required"
            },
            403
          );

        }


        try {

          const result =
            await backupEightHoursToDropbox(
              new Date()
            );


          return sendJson(
            response,
            result
          );

        }

        catch (
          error
        ) {

          return sendJson(
            response,
            {
              ok:
                false,

              error:
                error.message
            },
            500
          );

        }

      }


      /*
      ================================================
      CURRENT USER
      ================================================
      */

      if (

        url.pathname ===
        "/api/auth/me"

        &&

        request.method ===
        "GET"

      ) {

        return currentUserApi(

          request,

          response

        );

      }


      /*
      ================================================
      LOGOUT
      ================================================
      */

      if (

        url.pathname ===
        "/api/auth/logout"

        &&

        request.method ===
        "POST"

      ) {

        return logoutUser(

          request,

          response

        );

      }


      /*
      ================================================
      MASTER ADMIN PAGE
      ================================================
      */

      if (

        url.pathname ===
        "/admin"

        ||

        url.pathname ===
        "/admin.html"

      ) {

        if (
          authUser.role !==
          "master"
        ) {

          response.writeHead(

            403,

            {

              "Content-Type":
                "text/plain; charset=utf-8",

              "Cache-Control":
                "no-store"

            }

          );


          return response.end(

            "Master access required"

          );

        }


        return servePublicFile(

          response,

          "admin.html"

        );

      }


      /*
      ================================================
      LIST USERS
      MASTER ONLY
      ================================================
      */

      if (

        url.pathname ===
        "/api/users"

        &&

        request.method ===
        "GET"

      ) {

        return listUsersApi(

          request,

          response

        );

      }


      /*
      ================================================
      CREATE USER
      MASTER ONLY
      ================================================
      */

      if (

        url.pathname ===
        "/api/users"

        &&

        request.method ===
        "POST"

      ) {

        return createUserApi(

          request,

          response

        );

      }


      /*
      ================================================
      RESET USER PASSWORD
      ================================================
      */

      if (

        url.pathname ===
        "/api/users/password"

        &&

        request.method ===
        "POST"

      ) {

        return resetUserPasswordApi(

          request,

          response

        );

      }


      /*
      ================================================
      ENABLE / DISABLE USER
      ================================================
      */

      if (

        url.pathname ===
        "/api/users/active"

        &&

        request.method ===
        "POST"

      ) {

        return setUserActiveApi(

          request,

          response

        );

      }


      /*
      ================================================
      DELETE USER
      ================================================
      */

      if (

        url.pathname ===
        "/api/users/delete"

        &&

        request.method ===
        "POST"

      ) {

        return deleteUserApi(

          request,

          response

        );

      }


      /*
      ================================================
      CURRENT BMS STATE
      ================================================
      */

      if (
        url.pathname ===
        "/api/state"
      ) {

        return sendJson(

          response,

          latest,

          latest.ok

          ?

          200

          :

          503

        );

      }


      /*
      ================================================
      LIVE TREND
      ================================================
      */

      if (
        url.pathname ===
        "/api/trend"
      ) {

        return sendJson(

          response,

          {

            ok:
              true,

            sampleIntervalMs:
              LIVE_SAMPLE_MS,

            count:
              liveHistory.length,

            samples:
              liveHistory

          }

        );

      }
            /*
      ================================================
      PERMANENT HISTORY
      ================================================
      */

      if (
        url.pathname ===
        "/api/history"
      ) {

        try {

          const fromText =

            url.searchParams.get(
              "from"
            );


          const toText =

            url.searchParams.get(
              "to"
            );


          if (
            !fromText ||
            !toText
          ) {

            return sendJson(

              response,

              {

                ok:
                  false,

                error:
                  "from and to are required"

              },

              400

            );

          }


          const from =

            new Date(
              fromText
            );


          const to =

            new Date(
              toText
            );


          if (

            Number.isNaN(
              from.getTime()
            )

            ||

            Number.isNaN(
              to.getTime()
            )

          ) {

            return sendJson(

              response,

              {

                ok:
                  false,

                error:
                  "Invalid date range"

              },

              400

            );

          }


          if (
            from >
            to
          ) {

            return sendJson(

              response,

              {

                ok:
                  false,

                error:
                  "From must be before To"

              },

              400

            );

          }


          const rows =

            await queryHistory(

              from,

              to

            );


          const samples =

            rows.map(

              row => ({

                timestamp:

                  new Date(
                    row.recorded_at
                  ).toISOString(),

                in1:

                  Number(
                    row.planks_in
                  ),

                in2:

                  Number(
                    row.planks_out
                  ),

                in3:

                  Number(
                    row.ambient
                  ),

                in4:

                  Number(
                    row.planks_concrete
                  ),

                in5:

                  Number(
                    row.planks_tank
                  ),

                diff:

                  Number(
                    row.ambient_concrete_diff
                  )

              })

            );


          return sendJson(

            response,

            {

              ok:
                true,

              count:
                samples.length,

              from:
                from.toISOString(),

              to:
                to.toISOString(),

              samples

            }

          );

        }


        catch (
          error
        ) {

          return sendJson(

            response,

            {

              ok:
                false,

              error:
                error.message

            },

            500

          );

        }

      }


      /*
      ================================================
      HISTORY RANGE
      ================================================
      */

      if (
        url.pathname ===
        "/api/history/range"
      ) {

        try {

          const range =

            await getHistoryRange();


          return sendJson(

            response,

            {

              ok:
                true,

              ...range

            }

          );

        }


        catch (
          error
        ) {

          return sendJson(

            response,

            {

              ok:
                false,

              error:
                error.message

            },

            500

          );

        }

      }


      /*
      ================================================
      LOGGER STATUS
      ================================================
      */

      if (
        url.pathname ===
        "/api/logger/status"
      ) {

        return sendJson(

          response,

          {

            ok:
              true,

            databaseConfigured:
              Boolean(db),

            databaseConnected,

            archiveIntervalMinutes:
              HISTORY_INTERVAL_MINUTES,

            lastArchiveAt:

              lastArchiveAt

              ?

              lastArchiveAt.toISOString()

              :

              null,

            nextArchiveAt:

              nextArchiveAt

              ?

              nextArchiveAt.toISOString()

              :

              null,

            lastArchiveError,

            emailConfigured,

            emailConnected,

            emailTo:
              EMAIL_TO,

            emailReportHours:
              EMAIL_REPORT_HOURS,

            lastEmailAt:

              lastEmailAt

              ?

              lastEmailAt.toISOString()

              :

              null,

            nextEmailAt:

              nextEmailAt

              ?

              nextEmailAt.toISOString()

              :

              null,

            lastEmailError

          }

        );

      }


      /*
      ================================================
      EMAIL STATUS
      ================================================
      */

      if (
        url.pathname ===
        "/api/email/status"
      ) {

        return sendJson(

          response,

          {

            ok:
              true,

            configured:
              emailConfigured,

            connected:
              emailConnected,

            recipient:
              EMAIL_TO,

            everyHours:
              EMAIL_REPORT_HOURS,

            lastEmailAt:

              lastEmailAt

              ?

              lastEmailAt.toISOString()

              :

              null,

            nextEmailAt:

              nextEmailAt

              ?

              nextEmailAt.toISOString()

              :

              null,

            lastEmailError

          }

        );

      }


      /*
      ================================================
      MANUAL TEST EMAIL
      ================================================
      */

      if (
        url.pathname ===
        "/api/email/test"
      ) {

        try {

          if (
            !emailTransporter
          ) {

            throw new Error(
              "Email is not configured"
            );

          }


          const to =
            new Date();


          const from =

            new Date(

              to.getTime()

              -

              EMAIL_REPORT_MS

            );


          const rows =

            await queryHistory(

              from,

              to

            );


          const csv =

            buildEmailCsv(
              rows
            );


          await emailTransporter.sendMail({

            from:
              EMAIL_USER,

            to:
              EMAIL_TO,

            subject:
              "Greenair TrendLog - Test Email",

            text:

              `Greenair TrendLog test email.

Stored records attached: ${rows.length}

If you received this email, the automatic 8-hour reporting system is configured correctly.
`,

            attachments: [

              {

                filename:
                  "Greenair_TrendLog_Test.csv",

                content:
                  csv,

                contentType:
                  "text/csv"

              }

            ]

          });


          emailConnected =
            true;


          lastEmailError =
            null;


          return sendJson(

            response,

            {

              ok:
                true,

              message:
                "Test email sent",

              recipient:
                EMAIL_TO,

              rows:
                rows.length

            }

          );

        }


        catch (
          error
        ) {

          emailConnected =
            false;


          lastEmailError =
            error.message;


          return sendJson(

            response,

            {

              ok:
                false,

              error:
                error.message

            },

            500

          );

        }

      }


      /*
      ================================================
      LIVE STATE STREAM
      ================================================
      */

      if (
        url.pathname ===
        "/api/stream"
      ) {

        response.writeHead(

          200,

          {

            "Content-Type":
              "text/event-stream",

            "Cache-Control":
              "no-cache, no-transform",

            "Connection":
              "keep-alive",

            "X-Accel-Buffering":
              "no"

          }

        );


        response.write(

          `event: state\n`

          +

          `data: ${JSON.stringify(latest)}\n\n`

        );


        streamClients.add(
          response
        );


        const keepAlive =

          setInterval(

            () => {

              try {

                response.write(
                  ": keepalive\n\n"
                );

              }

              catch {}

            },

            15000

          );


        request.on(

          "close",

          () => {

            clearInterval(
              keepAlive
            );


            streamClients.delete(
              response
            );

          }

        );


        return;

      }


      /*
      ================================================
      MAIN TRENDLOG PAGE / STATIC FILES
      ================================================
      */

      return serveProtectedStatic(

        request,

        response

      );

    }

  );
/*
==================================================
START SERVER
==================================================
*/

async function startServer() {

  /*
  INITIALISE DATABASE
  */

  await initialiseDatabase();


  /*
  INITIALISE USER / SESSION TABLES
  AND CREATE MASTER ACCOUNT
  */

  await initialiseAuthentication();


  /*
  INITIALISE EMAIL
  */

  await initialiseEmail();


  /*
  START HTTP SERVER
  */

  server.listen(

    PORT,

    "0.0.0.0",

    () => {


      console.log(
        ""
      );


      console.log(
        "GREENAIR TRENDLOG"
      );


      console.log(
        "--------------------------------"
      );


      console.log(

        `BMS: ${BMS_HOST}:${BMS_PORT}`

      );


      console.log(

        `Unit ID: ${UNIT_ID}`

      );


      console.log(
        "Function: FC03 / READ ONLY"
      );


      console.log(
        "Live BMS poll: 3 seconds"
      );


      console.log(
        "Live trend sample: 15 seconds"
      );


      console.log(
        "Permanent database archive: 1 minute"
      );


      console.log(

        `8-hour email recipient: ${EMAIL_TO}`

      );


      console.log(

        `Email configured: ${emailConfigured}`

      );


      console.log(

        `Dropbox app configured: ${Boolean(DROPBOX_APP_KEY && DROPBOX_APP_SECRET)}`

      );


      console.log(

        `Dropbox backup token configured: ${Boolean(DROPBOX_REFRESH_TOKEN)}`

      );


      console.log(

        `Master username configured: ${Boolean(MASTER_USERNAME)}`

      );


      console.log(

        `Master password configured: ${Boolean(MASTER_PASSWORD)}`

      );


      console.log(

        `Login session duration: ${SESSION_HOURS} hours`

      );


      console.log(
        ""
      );

    }

  );


  /*
  INITIAL BMS POLL
  */

  await pollBms();


  /*
  CONTINUOUS BMS POLLING
  */

  setInterval(

    pollBms,

    POLL_MS

  );


  /*
  FIRST LIVE SAMPLE
  */

  setTimeout(

    saveLiveSample,

    5000

  );


  /*
  LIVE 15 SECOND TREND
  */

  setInterval(

    saveLiveSample,

    LIVE_SAMPLE_MS

  );


  /*
  PERMANENT ONE-MINUTE HISTORY
  */

  scheduleNextArchive();


  /*
  AUTOMATIC 8-HOUR EMAIL
  */

  scheduleNextEmail();


  /*
  AUTOMATIC 8-HOUR DROPBOX BACKUP
  */

  scheduleNextDropboxBackup();


  /*
  PERIODIC SESSION CLEANUP
  EVERY 30 MINUTES
  */

  setInterval(

    async () => {

      try {

        if (
          db
        ) {

          await db.query(`

            DELETE FROM app_sessions

            WHERE expires_at <= NOW()

          `);

        }

      }

      catch (
        error
      ) {

        console.error(

          "Session cleanup failed:",

          error.message

        );

      }

    },

    30 *
    60 *
    1000

  );

}


/*
==================================================
START APPLICATION
==================================================
*/

startServer()
  .catch(

    error => {

      console.error(

        "Server startup failed:",

        error

      );


      process.exit(
        1
      );

    }

  );


/*
==================================================
GRACEFUL SHUTDOWN
==================================================
*/

let shuttingDown =
  false;


async function shutdown(
  signal
) {

  if (
    shuttingDown
  ) {

    return;

  }


  shuttingDown =
    true;


  console.log(

    `Greenair TrendLog shutting down (${signal}).`

  );


  /*
  STOP ARCHIVE TIMER
  */

  if (
    archiveTimer
  ) {

    clearTimeout(
      archiveTimer
    );

  }


  /*
  STOP EMAIL TIMER
  */

  if (
    emailTimer
  ) {

    clearTimeout(
      emailTimer
    );

  }


  /*
  CLOSE EMAIL TRANSPORT
  */

  try {

    if (
      emailTransporter
    ) {

      emailTransporter.close();

    }

  }

  catch {}


  /*
  CLOSE HTTP SERVER
  */

  try {

    await new Promise(

      resolve => {

        server.close(
          resolve
        );

      }

    );

  }

  catch {}


  /*
  CLOSE POSTGRES
  */

  try {

    if (
      db
    ) {

      await db.end();

    }

  }

  catch {}


  process.exit(
    0
  );

}


/*
==================================================
PROCESS SIGNALS
==================================================
*/

process.on(

  "SIGTERM",

  () =>
    shutdown(
      "SIGTERM"
    )

);


process.on(

  "SIGINT",

  () =>
    shutdown(
      "SIGINT"
    )

);


/*
==================================================
UNHANDLED ERRORS
==================================================
*/

process.on(

  "unhandledRejection",

  reason => {

    console.error(

      "Unhandled promise rejection:",

      reason

    );

  }

);


process.on(

  "uncaughtException",

  error => {

    console.error(

      "Uncaught exception:",

      error

    );

  }

);
