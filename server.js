const http = require("http");
const fs = require("fs");
const path = require("path");
const net = require("net");
const { Pool } = require("pg");


/*
==================================================
GREENAIR TRENDLOG
==================================================

LIVE:
BMS polled every 3 seconds
Live graph sample every 15 seconds

PERMANENT HISTORY:
PostgreSQL sample every 30 minutes

The permanent logger runs on the SERVER.
The web page does NOT need to be open.

==================================================
*/


/*
==================================================
MODBUS CONNECTION
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


let db = null;


if (DATABASE_URL) {

  db = new Pool({

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
TIMING
==================================================
*/

const POLL_MS =
  3000;

const LIVE_SAMPLE_MS =
  15000;

const HISTORY_SAMPLE_MS =
  30 * 60 * 1000;


/*
24 hours of 15-second
live samples in RAM
*/

const MAX_LIVE_SAMPLES =
  5760;


/*
==================================================
MODBUS POINTS
==================================================
*/

const POINTS = [

  {
    id: "in1",
    name: "Planks In",
    register: 7485,
    kind: "analog"
  },

  {
    id: "in2",
    name: "Planks Out",
    register: 7487,
    kind: "analog"
  },

  {
    id: "in3",
    name: "Ambient",
    register: 7489,
    kind: "analog"
  },

  {
    id: "in4",
    name: "Planks Concrete",
    register: 7491,
    kind: "analog"
  },

  {
    id: "in5",
    name: "Planks Tank",
    register: 7493,
    kind: "analog"
  },

  {
    id: "diff",
    name: "Ambient - Concrete Differential",
    register: 7503,
    kind: "signedAnalog"
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

  ok: false,

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
HISTORY LOGGER STATUS
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


/*
==================================================
DATABASE SETUP
==================================================
*/

async function initialiseDatabase() {

  if (!db) {

    console.error(
      "DATABASE_URL is not configured."
    );

    databaseConnected =
      false;

    return;

  }


  try {

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


    databaseConnected =
      true;


    lastArchiveError =
      null;


    console.log(
      "PostgreSQL history database ready."
    );

  }


  catch (error) {

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
TRANSACTION ID
==================================================
*/

function nextTransactionId() {

  const current =
    transactionId;


  transactionId++;


  if (
    transactionId > 65535
  ) {

    transactionId =
      1;

  }


  return current;

}


/*
==================================================
BUILD MODBUS FC03 REQUEST
==================================================
*/

function buildReadRequest(
  transaction,
  register
) {

  const request =
    Buffer.alloc(12);


  request.writeUInt16BE(
    transaction,
    0
  );


  request.writeUInt16BE(
    0,
    2
  );


  request.writeUInt16BE(
    6,
    4
  );


  request[6] =
    UNIT_ID;


  request[7] =
    3;


  request.writeUInt16BE(
    register,
    8
  );


  request.writeUInt16BE(
    1,
    10
  );


  return request;

}


/*
==================================================
READ ONE MODBUS REGISTER
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

        if (completed) {
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

          ? error

          : new Error(
              String(error)
            )

        );

      }


      function succeed(
        value
      ) {

        if (completed) {
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
            responseBuffer.length < 7
          ) {

            return;

          }


          const responseTransaction =

            responseBuffer.readUInt16BE(
              0
            );


          const protocolId =

            responseBuffer.readUInt16BE(
              2
            );


          const length =

            responseBuffer.readUInt16BE(
              4
            );


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
            protocolId !== 0
          ) {

            return fail(

              new Error(
                "Protocol ID mismatch"
              )

            );

          }


          if (
            responseUnit !== UNIT_ID
          ) {

            return fail(

              new Error(

                `Unit ID mismatch: expected ${UNIT_ID}, received ${responseUnit}`

              )

            );

          }


          const pdu =

            responseBuffer.subarray(

              7,

              completeLength

            );


          if (
            pdu.length < 2
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
            (functionCode & 0x80) !== 0
          ) {

            return fail(

              new Error(

                `Modbus exception ${pdu[1]} Register ${register}`

              )

            );

          }


          if (
            functionCode !== 3
          ) {

            return fail(

              new Error(

                `Unexpected Modbus function ${functionCode}`

              )

            );

          }


          if (
            pdu.length !== 4 ||
            pdu[1] !== 2
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

          if (!completed) {

            fail(

              new Error(

                "Connection closed before complete Modbus response"

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
SIGNED 16-BIT
==================================================
*/

function signed16(
  raw
) {

  if (
    raw >= 32768
  ) {

    return raw -
      65536;

  }


  return raw;

}


/*
==================================================
SCALE MODBUS VALUE
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

      signed16(raw) /
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

  if (polling) {

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
GET CURRENT POINT VALUE
==================================================
*/

function getLatestValue(
  id
) {

  if (
    !latest.ok ||
    !Array.isArray(
      latest.results
    )
  ) {

    return null;

  }


  const point =

    latest.results.find(

      result =>
        result.id === id

    );


  if (!point) {

    return null;

  }


  return Number(
    point.value
  );

}


/*
==================================================
LIVE 15 SECOND TREND
==================================================
*/

function saveLiveSample() {

  if (!latest.ok) {

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

    Object
      .values(values)
      .some(
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
PERMANENT DATABASE ARCHIVE
==================================================
*/

async function archiveTrendSample() {

  nextArchiveAt =
    null;


  if (!db) {

    lastArchiveError =
      "DATABASE_URL not configured";

    return;

  }


  if (!latest.ok) {

    lastArchiveError =
      "BMS offline at archive time";

    console.error(
      "30-minute archive skipped: BMS offline."
    );

    return;

  }


  const planksIn =
    getLatestValue("in1");

  const planksOut =
    getLatestValue("in2");

  const ambient =
    getLatestValue("in3");

  const concrete =
    getLatestValue("in4");

  const tank =
    getLatestValue("in5");

  const differential =
    getLatestValue("diff");


  const values = [

    planksIn,
    planksOut,
    ambient,
    concrete,
    tank,
    differential

  ];


  if (

    values.some(
      value =>
        value === null
    )

  ) {

    lastArchiveError =
      "One or more BMS values unavailable";

    return;

  }


  try {


    const recordedAt =
      new Date();


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
        planksIn,
        planksOut,
        ambient,
        concrete,
        tank,
        differential
      ]

    );


    databaseConnected =
      true;


    lastArchiveAt =
      recordedAt;


    lastArchiveError =
      null;


    console.log(

      "30-minute trend history saved:",

      recordedAt.toISOString()

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

      "Trend history database save failed:",

      error.message

    );

  }

}


/*
==================================================
ALIGN ARCHIVE TO :00 AND :30
==================================================
*/

function millisecondsUntilNextHalfHour() {

  const now =
    new Date();


  const next =
    new Date(
      now
    );


  next.setSeconds(
    0,
    0
  );


  if (
    now.getMinutes() < 30
  ) {

    next.setMinutes(
      30
    );

  }

  else {

    next.setHours(
      next.getHours() + 1
    );


    next.setMinutes(
      0
    );

  }


  return Math.max(

    1000,

    next.getTime() -
    now.getTime()

  );

}


/*
==================================================
START PERMANENT LOGGER
==================================================
*/

function schedulePermanentLogger() {

  const wait =
    millisecondsUntilNextHalfHour();


  nextArchiveAt =

    new Date(

      Date.now() +
      wait

    );


  console.log(

    "Next permanent archive:",

    nextArchiveAt.toISOString()

  );


  setTimeout(

    async () => {


      await archiveTrendSample();


      setInterval(

        archiveTrendSample,

        HISTORY_SAMPLE_MS

      );

    },

    wait

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

  if (!db) {

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


  databaseConnected =
    true;


  return result.rows.map(

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

}


/*
==================================================
HISTORY RANGE
==================================================
*/

async function getHistoryRange() {

  if (!db) {

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

    await db.query(

      `
      SELECT
        MIN(recorded_at) AS first,
        MAX(recorded_at) AS last,
        COUNT(*)::bigint AS count
      FROM trend_history
      `

    );


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
JSON RESPONSE
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

      "Access-Control-Allow-Origin":
        "*"

    }

  );


  response.end(
    data
  );

}


/*
==================================================
STATIC FILE SERVER
==================================================
*/

function serveStatic(
  request,
  response
) {

  const url =

    new URL(

      request.url,

      `http://${request.headers.host || "localhost"}`

    );


  const requested =

    url.pathname === "/"

    ? "/index.html"

    : url.pathname;


  const publicRoot =

    path.join(

      __dirname,

      "public"

    );


  const filePath =

    path.resolve(

      publicRoot,

      "." +
      requested

    );


  if (

    !filePath.startsWith(
      publicRoot
    )

  ) {

    response.writeHead(
      403
    );


    return response.end(
      "Forbidden"
    );

  }


  fs.readFile(

    filePath,

    (
      error,
      data
    ) => {


      if (error) {

        response.writeHead(
          404
        );


        return response.end(
          "Not found"
        );

      }


      let contentType =
        "application/octet-stream";


      if (
        filePath.endsWith(
          ".html"
        )
      ) {

        contentType =
          "text/html; charset=utf-8";

      }


      response.writeHead(

        200,

        {

          "Content-Type":
            contentType,

          "Cache-Control":
            "no-store"

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
      CURRENT BMS STATE
      */

      if (
        url.pathname ===
        "/api/state"
      ) {

        return sendJson(

          response,

          latest,

          latest.ok
          ? 200
          : 503

        );

      }


      /*
      15 SECOND LIVE TREND
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
      PERMANENT HISTORY
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
            ) ||

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
            from > to
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


          const samples =

            await queryHistory(

              from,

              to

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
      HISTORY RANGE
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
      LOGGER STATUS
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
              30,

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

            lastArchiveError

          }

        );

      }


      /*
      HEALTH
      */

      if (
        url.pathname ===
        "/api/health"
      ) {

        return sendJson(

          response,

          {

            ok:
              true,

            service:
              "greenair-trendlog",

            bmsOnline:
              latest.ok,

            databaseConfigured:
              Boolean(db),

            databaseConnected,

            liveTrendSamples:
              liveHistory.length,

            permanentIntervalMinutes:
              30

          }

        );

      }


      /*
      LIVE STATE STREAM
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
      STATIC PAGE
      */

      serveStatic(

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


  await initialiseDatabase();


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
        "Live trend: 15 seconds"
      );


      console.log(
        "Permanent archive: 30 minutes"
      );


      console.log(

        `Database configured: ${Boolean(db)}`

      );


      console.log(
        ""
      );

    }

  );


  /*
  MODBUS
  */

  pollBms();


  setInterval(

    pollBms,

    POLL_MS

  );


  /*
  LIVE 15 SECOND LOGGER
  */

  setTimeout(

    saveLiveSample,

    5000

  );


  setInterval(

    saveLiveSample,

    LIVE_SAMPLE_MS

  );


  /*
  PERMANENT 30 MINUTE LOGGER
  */

  schedulePermanentLogger();

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

      process.exit(1);

    }

  );


/*
==================================================
GRACEFUL SHUTDOWN
==================================================
*/

async function shutdown() {

  console.log(
    "Greenair TrendLog shutting down."
  );


  try {

    if (db) {

      await db.end();

    }

  }

  catch {}


  process.exit(0);

}


process.on(
  "SIGTERM",
  shutdown
);


process.on(
  "SIGINT",
  shutdown
);
