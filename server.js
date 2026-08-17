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
