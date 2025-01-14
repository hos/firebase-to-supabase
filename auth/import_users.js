"use strict";
var __awaiter = (this && this.__awaiter) || function (thisArg, _arguments, P, generator) {
    function adopt(value) { return value instanceof P ? value : new P(function (resolve) { resolve(value); }); }
    return new (P || (P = Promise))(function (resolve, reject) {
        function fulfilled(value) { try { step(generator.next(value)); } catch (e) { reject(e); } }
        function rejected(value) { try { step(generator["throw"](value)); } catch (e) { reject(e); } }
        function step(result) { result.done ? resolve(result.value) : adopt(result.value).then(fulfilled, rejected); }
        step((generator = generator.apply(thisArg, _arguments || [])).next());
    });
};
var __generator = (this && this.__generator) || function (thisArg, body) {
    var _ = { label: 0, sent: function() { if (t[0] & 1) throw t[1]; return t[1]; }, trys: [], ops: [] }, f, y, t, g;
    return g = { next: verb(0), "throw": verb(1), "return": verb(2) }, typeof Symbol === "function" && (g[Symbol.iterator] = function() { return this; }), g;
    function verb(n) { return function (v) { return step([n, v]); }; }
    function step(op) {
        if (f) throw new TypeError("Generator is already executing.");
        while (_) try {
            if (f = 1, y && (t = op[0] & 2 ? y["return"] : op[0] ? y["throw"] || ((t = y["return"]) && t.call(y), 0) : y.next) && !(t = t.call(y, op[1])).done) return t;
            if (y = 0, t) op = [op[0] & 2, t.value];
            switch (op[0]) {
                case 0: case 1: t = op; break;
                case 4: _.label++; return { value: op[1], done: false };
                case 5: _.label++; y = op[1]; op = [0]; continue;
                case 7: op = _.ops.pop(); _.trys.pop(); continue;
                default:
                    if (!(t = _.trys, t = t.length > 0 && t[t.length - 1]) && (op[0] === 6 || op[0] === 2)) { _ = 0; continue; }
                    if (op[0] === 3 && (!t || (op[1] > t[0] && op[1] < t[3]))) { _.label = op[1]; break; }
                    if (op[0] === 6 && _.label < t[1]) { _.label = t[1]; t = op; break; }
                    if (t && _.label < t[2]) { _.label = t[2]; _.ops.push(op); break; }
                    if (t[2]) _.ops.pop();
                    _.trys.pop(); continue;
            }
            op = body.call(thisArg, _);
        } catch (e) { op = [6, e]; y = 0; } finally { f = t = 0; }
        if (op[0] & 5) throw op[1]; return { value: op[0] ? op[1] : void 0, done: true };
    }
};
exports.__esModule = true;
var fs = require("fs");
var moment = require("moment");
var pg_1 = require("pg");
var args = process.argv.slice(2);
var filename;
var client;
var DEFAULT_BATCH_SIZE = 100;
if (args.length < 1) {
    console.log("Usage: node import_users.js <path_to_json_file> [<batch_size>]");
    console.log("  path_to_json_file: full local path and filename of .json input file (of users)");
    console.log("  batch_size: number of users to process in a batch (defaults to ".concat(DEFAULT_BATCH_SIZE, ")"));
    process.exit(1);
}
else {
    filename = args[0];
}
var BATCH_SIZE = parseInt(args[1], 10) || DEFAULT_BATCH_SIZE;
if (!BATCH_SIZE || typeof BATCH_SIZE !== "number" || BATCH_SIZE < 1) {
    console.log("invalid batch_size");
    process.exit(1);
}
var pgCreds;
try {
    pgCreds = JSON.parse(fs.readFileSync("./supabase-service.json", "utf8"));
    if (typeof pgCreds.user === "string" &&
        typeof pgCreds.password === "string" &&
        typeof pgCreds.host === "string" &&
        typeof pgCreds.port === "number" &&
        typeof pgCreds.database === "string") {
    }
    else {
        console.log("supabase-service.json must contain the following fields:");
        console.log("   user: string");
        console.log("   password: string");
        console.log("   host: string");
        console.log("   port: number");
        console.log("   database: string");
        process.exit(1);
    }
}
catch (err) {
    console.log("error reading supabase-service.json", err);
    process.exit(1);
}
function main(filename) {
    return __awaiter(this, void 0, void 0, function () {
        return __generator(this, function (_a) {
            switch (_a.label) {
                case 0: return [4 /*yield*/, new pg_1.Client({
                        user: pgCreds.user,
                        host: pgCreds.host,
                        database: pgCreds.database,
                        password: pgCreds.password,
                        port: pgCreds.port
                    })];
                case 1:
                    client = _a.sent();
                    client.connect();
                    console.log("loading users from ".concat(filename));
                    return [4 /*yield*/, loadUsers(filename)];
                case 2:
                    _a.sent();
                    console.log("done processing ".concat(filename));
                    quit();
                    return [2 /*return*/];
            }
        });
    });
}
function quit() {
    client.end();
    process.exit(1);
}
function loadUsers(filename) {
    return __awaiter(this, void 0, void 0, function () {
        var _this = this;
        return __generator(this, function (_a) {
            return [2 /*return*/, new Promise(function (resolve, reject) {
                    var Batch = require("stream-json/utils/Batch");
                    var insertRows = [];
                    var insertValues = [];
                    var StreamArray = require("stream-json/streamers/StreamArray");
                    var chain = require("stream-chain").chain;
                    var fs = require("fs");
                    var pipeline = chain([
                        fs.createReadStream(filename),
                        StreamArray.withParser(),
                        new Batch({ batchSize: BATCH_SIZE }),
                    ]);
                    // count all odd values from a huge array
                    var oddCounter = 0;
                    pipeline.on("data", function (data) { return __awaiter(_this, void 0, void 0, function () {
                        var result;
                        return __generator(this, function (_a) {
                            switch (_a.label) {
                                case 0:
                                    data.forEach(function (item, idx) {
                                        var user = item.value;
                                        var _a = createUser(user, idx), sql = _a[0], values = _a[1];
                                        insertRows.push(sql);
                                        insertValues.push.apply(insertValues, values);
                                    });
                                    console.log("insertUsers:", insertRows.length);
                                    pipeline.pause();
                                    return [4 /*yield*/, insertUsers(insertRows, insertValues)];
                                case 1:
                                    result = _a.sent();
                                    insertRows = [];
                                    insertValues = [];
                                    pipeline.resume();
                                    return [2 /*return*/];
                            }
                        });
                    }); });
                    pipeline.on("end", function () {
                        console.log("finished");
                        resolve("");
                    });
                })];
        });
    });
}
function insertUsers(rows, params) {
    return __awaiter(this, void 0, void 0, function () {
        var sql, result;
        return __generator(this, function (_a) {
            switch (_a.label) {
                case 0:
                    sql = createUserHeader() + rows.join(",\n") + "ON CONFLICT DO NOTHING;";
                    return [4 /*yield*/, runSQL(sql, params)];
                case 1:
                    result = _a.sent();
                    return [2 /*return*/, result];
            }
        });
    });
}
function formatDate(date) {
    return moment.utc(date).toISOString();
}
function runSQL(sql, values) {
    return __awaiter(this, void 0, void 0, function () {
        var _this = this;
        return __generator(this, function (_a) {
            return [2 /*return*/, new Promise(function (resolve, reject) { return __awaiter(_this, void 0, void 0, function () {
                    return __generator(this, function (_a) {
                        // fs.writeFileSync(`temp.sql`, sql, 'utf-8');
                        client.query(sql, values, function (err, res) {
                            if (err) {
                                console.log("runSQL error:", err);
                                console.log("sql was: ");
                                console.log(sql);
                                quit();
                                reject(err);
                            }
                            else {
                                resolve(res);
                            }
                        });
                        return [2 /*return*/];
                    });
                }); })];
        });
    });
}
main(filename);
function createUserHeader() {
    return "INSERT INTO auth.users (\n        instance_id,\n        id,\n        aud,\n        role,\n        email,\n        encrypted_password,\n        email_confirmed_at,\n        invited_at,\n        confirmation_token,\n        confirmation_sent_at,\n        recovery_token,\n        recovery_sent_at,\n        email_change_token_new,\n        email_change,\n        email_change_sent_at,\n        last_sign_in_at,\n        raw_app_meta_data,\n        raw_user_meta_data,\n        is_super_admin,\n        created_at,\n        updated_at,\n        phone,\n        phone_confirmed_at,\n        phone_change,\n        phone_change_token,\n        phone_change_sent_at,\n        email_change_token_current,\n        email_change_confirm_status    \n    ) VALUES ";
}
function createUser(user, index) {
    var _a = user.metadata, lastSignInTime = _a.lastSignInTime, creationTime = _a.creationTime;
    var params = [
        user.email,
        user.emailVerified ? "NOW()" : null,
        creationTime ? formatDate(creationTime) : null,
        lastSignInTime ? formatDate(lastSignInTime) : null,
        getProviderString(user.providerData),
        JSON.stringify({ fbuser: user }),
    ];
    var paramNum = index * params.length;
    var next = function () { return "$".concat(++paramNum); };
    var sql = "(\n        '00000000-0000-0000-0000-000000000000', /* instance_id */\n        uuid_generate_v4(), /* id */\n        'authenticated', /* aud character varying(255),*/\n        'authenticated', /* role character varying(255),*/\n        ".concat(next(), ", /* email character varying(255),*/\n        '', /* encrypted_password character varying(255),*/\n        ").concat(next(), ", /* email_confirmed_at timestamp with time zone,*/\n        ").concat(next(), "::timestamptz, /* invited_at timestamp with time zone, */\n        '', /* confirmation_token character varying(255), */\n        null, /* confirmation_sent_at timestamp with time zone, */\n        '', /* recovery_token character varying(255), */\n        null, /* recovery_sent_at timestamp with time zone, */\n        '', /* email_change_token_new character varying(255), */\n        '', /* email_change character varying(255), */\n        null, /* email_change_sent_at timestamp with time zone, */\n        ").concat(next(), "::timestamptz, /* last_sign_in_at timestamp with time zone, */\n        ").concat(next(), "::JSONB, /* raw_app_meta_data jsonb,*/\n        ").concat(next(), "::JSONB, /* raw_user_meta_data jsonb,*/\n        false, /* is_super_admin boolean, */\n        NOW(), /* created_at timestamp with time zone, */\n        NOW(), /* updated_at timestamp with time zone, */\n        null, /* phone character varying(15) DEFAULT NULL::character varying, */\n        null, /* phone_confirmed_at timestamp with time zone, */\n        '', /* phone_change character varying(15) DEFAULT ''::character varying, */\n        '', /* phone_change_token character varying(255) DEFAULT ''::character varying, */\n        null, /* phone_change_sent_at timestamp with time zone, */\n        '', /* email_change_token_current character varying(255) DEFAULT ''::character varying, */\n        0 /*email_change_confirm_status smallint DEFAULT 0 */   \n    )");
    return [sql, params];
}
function getProviderString(providerData) {
    var providers = [];
    for (var i = 0; i < providerData.length; i++) {
        var p = providerData[i].providerId.toLowerCase().replace(".com", "");
        var provider = "email";
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
    var providerString = "{\"provider\": \"".concat(providers[0], "\",\"providers\":[\"").concat(providers.join('","'), "\"]}");
    return providerString;
}
