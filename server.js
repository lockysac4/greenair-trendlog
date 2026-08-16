const http = require("http");
const fs = require("fs");
const path = require("path");
const net = require("net");
const { Pool } = require("pg");


/*
==================================================
GREENAIR TRENDLOG
==================================================

LIVE BMS POLL:
Every 3 seconds

LIVE WEB TREND:
Every 15 seconds

PERMANENT POSTGRES HISTORY:
Every 5 minutes

Permanent logging is SERVER SIDE.
The webpage does NOT need to be open.

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
TIMING
==================================================
*/

const POLL_MS =
  3000;


const LIVE_SAMPLE_MS =
  15000;


/*
Permanent history every 5 minutes.
*/

const HISTORY_INTERVAL_MINUTES =
  5;


const HISTORY_SAMPLE_MS =
  HISTORY_INTERVAL_MINUTES *
  60 *
  1000;


/*
24 hours of 15-second
live samples held in RAM.
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


    /*
    Recover last archive time from PostgreSQL.

    This fixes the old issue where lastArchiveAt
    returned to null after every Render redeploy.
    */

    const previous =

      await db.query(`

        SELECT
          MAX(recorded_at) AS last_archive

        FROM trend_history

      `);


    if (
      previous.rows[0] &&
      previous.rows[0].last_archive
    ) {

      lastArchiveAt =

        new Date(

          previous.rows[0]
            .last_archive

        );

    }


    databaseConnected =
      true;


    lastArchiveError =
      null;


    console.log(
      "PostgreSQL history database ready."
    );


    if (
      lastArchiveAt
    ) {

      console.log(

        "Last permanent archive:",

        lastArchiveAt.toISOString()

      );

    }

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
BUILD MODBUS FC03 REQUEST
==================================================
*/

function buildReadRequest(
  transaction,
  register
) {

  const request =
    Buffer.alloc(12);


  /*
  Transaction ID
  */

  request.writeUInt16BE(
    transaction,
    0
  );


  /*
  Protocol ID
  */

  request.writeUInt16BE(
    0,
    2
  );


  /*
  Remaining packet length
  */

  request.writeUInt16BE(
    6,
    4
  );


  /*
  Unit ID
  */

  request[6] =
    UNIT_ID;


  /*
  Function code 03
  */

  request[7] =
    3;


  /*
  Starting register
  */

  request.writeUInt16BE(
    register,
    8
  );


  /*
  Quantity
  */

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


      /*
      TCP connect timeout
      */

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


      /*
      CONNECTED
      */

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


      /*
      RECEIVE MODBUS RESPONSE
      */

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


          /*
          Modbus exception
          */

          if (
            (
              functionCode &
              0x80
            ) !== 0
          ) {

            return fail(

              new Error(

                `Modbus exception ${pdu[1]} Register ${register}`

              )

            );

          }


          if (
            functionCode !==
            3
          ) {

            return fail(

              new Error(

                `Unexpected Modbus function ${functionCode}`

              )

            );

          }


          if (
            pdu.length !==
            4 ||
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
SCALE MODBUS VALUES
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


  if (
    !point
  ) {

    return null;

  }


  const value =

    Number(
      point.value
    );


  if (
    !Number.isFinite(
      value
    )
  ) {

    return null;

  }


  return value;

}


/*
==================================================
LIVE 15 SECOND HISTORY
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
      getLatestValue(
        "in1"
      ),

    in2:
      getLatestValue(
        "in2"
      ),

    in3:
      getLatestValue(
        "in3"
      ),

    in4:
      getLatestValue(
        "in4"
      ),

    in5:
      getLatestValue(
        "in5"
      ),

    diff:
      getLatestValue(
        "diff"
      )

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
GET CURRENT 5 MINUTE SLOT
==================================================
*/

function getCurrentFiveMinuteSlot() {

  const now =
    new Date();


  const slot =
    new Date(
      now
    );


  slot.setSeconds(
    0,
    0
  );


  const minute =

    Math.floor(

      slot.getMinutes() /
      HISTORY_INTERVAL_MINUTES

    )

    *

    HISTORY_INTERVAL_MINUTES;


  slot.setMinutes(
    minute
  );


  return slot;

}


/*
==================================================
GET NEXT 5 MINUTE BOUNDARY
==================================================
*/

function getNextFiveMinuteBoundary() {

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


  const currentMinute =
    now.getMinutes();


  const nextMinute =

    (
      Math.floor(

        currentMinute /
        HISTORY_INTERVAL_MINUTES

      )

      +

      1
    )

    *

    HISTORY_INTERVAL_MINUTES;


  if (
    nextMinute >=
    60
  ) {

    next.setHours(

      next.getHours() +
      1

    );


    next.setMinutes(
      0
    );

  }

  else {

    next.setMinutes(
      nextMinute
    );

  }


  return next;

}


/*
==================================================
PERMANENT DATABASE ARCHIVE
==================================================
*/

async function archiveTrendSample(
  recordedAt = null
) {

  if (
    !db
  ) {

    lastArchiveError =
      "DATABASE_URL not configured";


    databaseConnected =
      false;


    return false;

  }


  if (
    !latest.ok
  ) {

    lastArchiveError =
      "BMS offline at archive time";


    console.error(

      "5-minute archive skipped: BMS offline."

    );


    return false;

  }


  const planksIn =
    getLatestValue(
      "in1"
    );


  const planksOut =
    getLatestValue(
      "in2"
    );


  const ambient =
    getLatestValue(
      "in3"
    );


  const concrete =
    getLatestValue(
      "in4"
    );


  const tank =
    getLatestValue(
      "in5"
    );


  const differential =
    getLatestValue(
      "diff"
    );


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


    console.error(

      "5-minute archive skipped: BMS values unavailable."

    );


    return false;

  }


  /*
  Archive timestamp is the exact
  5 minute boundary.

  Example:
  20:00
  20:05
  20:10
  20:15
  */


  const archiveTime =

    recordedAt instanceof Date

    ?

    recordedAt

    :

    getCurrentFiveMinuteSlot();


  try {

    /*
    Prevent duplicate records for the exact same
    5-minute timestamp.

    Useful if Render restarts close to a boundary.
    */

    const existing =

      await db.query(

        `

        SELECT id

        FROM trend_history

        WHERE recorded_at = $1

        LIMIT 1

        `,

        [
          archiveTime
        ]

      );


    if (
      existing.rowCount >
      0
    ) {

      databaseConnected =
        true;


      lastArchiveAt =
        archiveTime;


      lastArchiveError =
        null;


      console.log(

        "5-minute archive already exists:",

        archiveTime.toISOString()

      );


      return true;

    }


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

        archiveTime,

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
      archiveTime;


    lastArchiveError =
      null;


    console.log(

      "5-minute trend history saved:",

      archiveTime.toISOString()

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

      "Trend history database save failed:",

      error.message

    );


    return false;

  }

}


/*
==================================================
SCHEDULE NEXT 5 MINUTE ARCHIVE
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
    getNextFiveMinuteBoundary();


  nextArchiveAt =
    next;


  const delay =

    Math.max(

      1000,

      next.getTime() -
      Date.now()

    );


  console.log(

    "Next permanent archive:",

    next.toISOString()

  );


  archiveTimer =

    setTimeout(

      async () => {

        /*
        Use the scheduled boundary,
        not Date.now(), as the timestamp.
        */

        await archiveTrendSample(
          next
        );


        /*
        Calculate the next boundary again.

        Recursive scheduling avoids interval drift.
        */

        scheduleNextArchive();

      },

      delay

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

    await db.query(

      `

      SELECT

        MIN(recorded_at) AS first,

        MAX(recorded_at) AS last,

        COUNT(*)::bigint AS count

      FROM trend_history

      `

    );


  databaseConnected =
    true;


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

    ?

    "/index.html"

    :

    url.pathname;


  const publicRoot =

    path.resolve(

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


      else if (
        filePath.endsWith(
          ".css"
        )
      ) {

        contentType =
          "text/css; charset=utf-8";

      }


      else if (
        filePath.endsWith(
          ".js"
        )
      ) {

        contentType =
          "application/javascript; charset=utf-8";

      }


      else if (
        filePath.endsWith(
          ".png"
        )
      ) {

        contentType =
          "image/png";

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

          ?

          200

          :

          503

        );

      }


      /*
      LIVE 15 SECOND TREND
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
      PERMANENT POSTGRES HISTORY
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

              null

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

  /*
  DATABASE
  */

  await initialiseDatabase();


  /*
  HTTP SERVER
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

        `Permanent archive: ${HISTORY_INTERVAL_MINUTES} minutes`

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
  START MODBUS POLL
  */

  await pollBms();


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
  LIVE 15 SECOND LOGGER
  */

  setInterval(

    saveLiveSample,

    LIVE_SAMPLE_MS

  );


  /*
  5 MINUTE PERMANENT LOGGER

  Records:
  :00
  :05
  :10
  :15
  :20
  :25
  :30
  :35
  :40
  :45
  :50
  :55
  */

  scheduleNextArchive();

}


/*
==================================================
START
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

async function shutdown() {

  console.log(
    "Greenair TrendLog shutting down."
  );


  if (
    archiveTimer
  ) {

    clearTimeout(
      archiveTimer
    );

  }


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


process.on(
  "SIGTERM",
  shutdown
);


process.on(
  "SIGINT",
  shutdown
);
