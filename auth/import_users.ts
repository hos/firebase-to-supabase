import * as fs from "fs";
import * as moment from "moment";
import { Client } from "pg";
import * as StreamArray from "stream-json/streamers/StreamArray";

const args = process.argv.slice(2);
let filename;
let client: Client;

const DEFAULT_BATCH_SIZE = 100;

if (args.length < 1) {
  console.log("Usage: node import_users.js <path_to_json_file> [<batch_size>]");
  console.log(
    "  path_to_json_file: full local path and filename of .json input file (of users)"
  );
  console.log(
    `  batch_size: number of users to process in a batch (defaults to ${DEFAULT_BATCH_SIZE})`
  );
  process.exit(1);
} else {
  filename = args[0];
}
const BATCH_SIZE = parseInt(args[1], 10) || DEFAULT_BATCH_SIZE;
if (!BATCH_SIZE || typeof BATCH_SIZE !== "number" || BATCH_SIZE < 1) {
  console.log("invalid batch_size");
  process.exit(1);
}

let pgCreds;
try {
  pgCreds = JSON.parse(fs.readFileSync("./supabase-service.json", "utf8"));
  if (
    typeof pgCreds.user === "string" &&
    typeof pgCreds.password === "string" &&
    typeof pgCreds.host === "string" &&
    typeof pgCreds.port === "number" &&
    typeof pgCreds.database === "string"
  ) {
  } else {
    console.log("supabase-service.json must contain the following fields:");
    console.log("   user: string");
    console.log("   password: string");
    console.log("   host: string");
    console.log("   port: number");
    console.log("   database: string");
    process.exit(1);
  }
} catch (err) {
  console.log("error reading supabase-service.json", err);
  process.exit(1);
}

async function main(filename: string) {
  client = await new Client({
    user: pgCreds.user,
    host: pgCreds.host,
    database: pgCreds.database,
    password: pgCreds.password,
    port: pgCreds.port,
  });
  client.connect();

  console.log(`loading users from ${filename}`);
  await loadUsers(filename);
  console.log(`done processing ${filename}`);
  quit();
}
function quit() {
  client.end();
  process.exit(1);
}

async function loadUsers(filename: string): Promise<any> {
  return new Promise((resolve, reject) => {
    const Batch = require("stream-json/utils/Batch");
    let insertRows = [];
    let insertValues = [];

    const StreamArray = require("stream-json/streamers/StreamArray");
    const { chain } = require("stream-chain");
    const fs = require("fs");

    const pipeline = chain([
      fs.createReadStream(filename),
      StreamArray.withParser(),
      new Batch({ batchSize: BATCH_SIZE }),
    ]);

    // count all odd values from a huge array

    let oddCounter = 0;
    pipeline.on("data", async (data) => {
      data.forEach((item, idx) => {
        const user = item.value;
        const [sql, values] = createUser(user, idx);
        insertRows.push(sql);
        insertValues.push(...values);
      });
      console.log("insertUsers:", insertRows.length);
      pipeline.pause();
      const result = await insertUsers(insertRows, insertValues);
      insertRows = [];
      insertValues = [];
      pipeline.resume();
    });
    pipeline.on("end", () => {
      console.log("finished");
      resolve("");
    });
  });
}

async function insertUsers(rows: any[], params: any[]): Promise<any> {
  const sql = createUserHeader() + rows.join(",\n") + "ON CONFLICT DO NOTHING;";
  const result = await runSQL(sql, params);
  return result;
}

function formatDate(date: string) {
  return moment.utc(date).toISOString();
}
async function runSQL(sql: string, values: any[]): Promise<any> {
  return new Promise(async (resolve, reject) => {
    // fs.writeFileSync(`temp.sql`, sql, 'utf-8');
    client.query(sql, values, (err, res) => {
      if (err) {
        console.log("runSQL error:", err);
        console.log("sql was: ");
        console.log(sql);
        quit();
        reject(err);
      } else {
        resolve(res);
      }
    });
  });
}

main(filename);

function createUserHeader() {
  return `INSERT INTO auth.users (
        instance_id,
        id,
        aud,
        role,
        email,
        encrypted_password,
        email_confirmed_at,
        invited_at,
        confirmation_token,
        confirmation_sent_at,
        recovery_token,
        recovery_sent_at,
        email_change_token_new,
        email_change,
        email_change_sent_at,
        last_sign_in_at,
        raw_app_meta_data,
        raw_user_meta_data,
        is_super_admin,
        created_at,
        updated_at,
        phone,
        phone_confirmed_at,
        phone_change,
        phone_change_token,
        phone_change_sent_at,
        email_change_token_current,
        email_change_confirm_status    
    ) VALUES `;
}
function createUser(user: any, index: number): [string, any[]] {
  const params = [
    user.email,
    formatDate(user.metadata.creationTime),
    getProviderString(user.providerData),
    JSON.stringify({ fbuser: user }),
  ];

  let paramNum = index * params.length;
  const next = () => ++paramNum;

  const sql = `(
        '00000000-0000-0000-0000-000000000000', /* instance_id */
        uuid_generate_v4(), /* id */
        'authenticated', /* aud character varying(255),*/
        'authenticated', /* role character varying(255),*/
        $${next()}, /* email character varying(255),*/
        '', /* encrypted_password character varying(255),*/
        ${
          user.emailVerified ? "NOW()" : "null"
        }, /* email_confirmed_at timestamp with time zone,*/
        $${next()}::timestamptz, /* invited_at timestamp with time zone, */
        '', /* confirmation_token character varying(255), */
        null, /* confirmation_sent_at timestamp with time zone, */
        '', /* recovery_token character varying(255), */
        null, /* recovery_sent_at timestamp with time zone, */
        '', /* email_change_token_new character varying(255), */
        '', /* email_change character varying(255), */
        null, /* email_change_sent_at timestamp with time zone, */
        null, /* last_sign_in_at timestamp with time zone, */
        $${next()}::JSONB, /* raw_app_meta_data jsonb,*/
        $${next()}::JSONB, /* raw_user_meta_data jsonb,*/
        false, /* is_super_admin boolean, */
        NOW(), /* created_at timestamp with time zone, */
        NOW(), /* updated_at timestamp with time zone, */
        null, /* phone character varying(15) DEFAULT NULL::character varying, */
        null, /* phone_confirmed_at timestamp with time zone, */
        '', /* phone_change character varying(15) DEFAULT ''::character varying, */
        '', /* phone_change_token character varying(255) DEFAULT ''::character varying, */
        null, /* phone_change_sent_at timestamp with time zone, */
        '', /* email_change_token_current character varying(255) DEFAULT ''::character varying, */
        0 /*email_change_confirm_status smallint DEFAULT 0 */   
    )`;

  return [sql, params];
}

function getProviderString(providerData: any[]) {
  const providers = [];
  for (let i = 0; i < providerData.length; i++) {
    const p = providerData[i].providerId.toLowerCase().replace(".com", "");
    let provider = "email";
    switch (p) {
      case "password":
        provider = "email";
        break;
      case "google":
        provider = "google";
        break;
      case "facebook":
        provider = "facebook";
        break;
    }
    providers.push(provider);
  }
  const providerString = `{"provider": "${
    providers[0]
  }","providers":["${providers.join('","')}"]}`;
  return providerString;
}
