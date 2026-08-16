const http = require("http");
const fs = require("fs");
const path = require("path");
const net = require("net");


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
SETTINGS
==================================================
*/

const POLL_MS =
  3000;

const TREND_SAMPLE_MS =
  15000;

const MAX_TREND_SAMPLES =
  5760;


/*
==================================================
TREND POINTS
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

let trendHistory =
  [];

const clients =
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
FC03 REQUEST
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
READ REGISTER
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

          ? error

          : new Error(
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

                    `Modbus response timeout at register ${register}`

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

            length <
            2 ||

            length >
            254

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


          if (

            (
              functionCode &
              0x80
            ) !==
            0

          ) {

            return fail(

              new Error(

                `Modbus exception ${pdu[1]} at register ${register}`

              )

            );

          }


          if (

            functionCode !==
            3

          ) {

            return fail(

              new Error(

                `Unexpected function ${functionCode}`

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
TREND LOGGER
==================================================
*/

function saveTrendSample() {

  if (

    !latest.ok ||

    !Array.isArray(
      latest.results
    )

  ) {

    return;

  }


  function value(
    id
  ) {

    const point =
      latest.results.find(

        item =>
          item.id === id

      );


    return point
      ? point.value
      : null;

  }


  trendHistory.push({

    timestamp:

      new Date()
      .toISOString(),

    in1:
      value("in1"),

    in2:
      value("in2"),

    in3:
      value("in3"),

    in4:
      value("in4"),

    in5:
      value("in5"),

    diff:
      value("diff")

  });


  if (

    trendHistory.length >
    MAX_TREND_SAMPLES

  ) {

    trendHistory =

      trendHistory.slice(

        -MAX_TREND_SAMPLES

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
    of clients
  ) {

    try {

      response.write(
        payload
      );

    }

    catch {

      clients.delete(
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


      if (
        error
      ) {

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

    (
      request,
      response
    ) => {


      const url =

        new URL(

          request.url,

          `http://${request.headers.host || "localhost"}`

        );


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
              TREND_SAMPLE_MS,

            count:
              trendHistory.length,

            samples:
              trendHistory

          }

        );

      }


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

            trendSamples:
              trendHistory.length

          }

        );

      }


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


        clients.add(
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


            clients.delete(
              response
            );

          }

        );


        return;

      }


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
      "------------------------------"
    );

    console.log(

      `BMS ${BMS_HOST}:${BMS_PORT}`

    );

    console.log(

      `Unit ID ${UNIT_ID}`

    );

    console.log(
      "FC03 / READ ONLY"
    );

    console.log(
      "Trend interval: 15 seconds"
    );

    console.log(
      "Trend history: 24 hours"
    );

    console.log(
      ""
    );

  }

);


/*
==================================================
START
==================================================
*/

pollBms();


setInterval(

  pollBms,

  POLL_MS

);


setTimeout(

  saveTrendSample,

  5000

);


setInterval(

  saveTrendSample,

  TREND_SAMPLE_MS

);
