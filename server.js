const http = require("http");
const fs = require("fs");
const path = require("path");
const net = require("net");
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
CREATE EMAIL TRANSPORTER
==================================================
*/

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

    Prevents the same 8-hour report
    being emailed twice after a restart.
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
      LIVE TREND
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
      HISTORY
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
      EMAIL STATUS
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
      MANUAL TEST EMAIL

      Opens:
      /api/email/test

      This sends an immediate CSV
      so we can verify the setup.
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
      LIVE STREAM
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
      STATIC FILE
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


  await initialiseEmail();


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
        ""
      );

    }

  );


  /*
  INITIAL BMS POLL
  */

  await pollBms();


  setInterval(

    pollBms,

    POLL_MS

  );


  /*
  LIVE TREND
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
  1-MINUTE PERMANENT HISTORY
  */

  scheduleNextArchive();


  /*
  8-HOUR EMAIL REPORT
  */

  scheduleNextEmail();

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
SHUTDOWN
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


  if (
    emailTimer
  ) {

    clearTimeout(
      emailTimer
    );

  }


  try {

    if (
      emailTransporter
    ) {

      emailTransporter.close();

    }

  }

  catch {}


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
