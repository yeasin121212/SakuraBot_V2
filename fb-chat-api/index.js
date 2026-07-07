"use strict";

if (typeof WebSocket === "undefined") {
    global.WebSocket = require("ws");
}

const cheerio = require("cheerio");
const path = require("path");
const fs = require("fs");
const utils = require("./utils");
const logger = require("./logger");
const { initAutoUpdate, checkForFCAUpdate } = require("./checkUpdate");

const PKG = require("./package.json");
const VERSION = PKG.version;
const NAME = "SHADOWX";

// Initialize auto-update on startup
initAutoUpdate();

// Check for updates immediately on require
checkForFCAUpdate().catch(() => {
    // Silently fail - updates are non-critical
});

const BOOLEAN_OPTIONS = [
    'online', 'selfListen', 'listenEvents', 'updatePresence', 'forceLogin',
    'autoMarkDelivery', 'autoMarkRead', 'listenTyping', 'autoReconnect', 'emitReady'
];

function setOptions(globalOptions, options) {
    Object.keys(options || {}).forEach(key => {
        if (BOOLEAN_OPTIONS.includes(key)) {
            globalOptions[key] = Boolean(options[key]);
        } else {
            switch (key) {
                case 'pauseLog':
                    if (options.pauseLog) log.pause();
                    else log.resume();
                    break;
                case 'logLevel':
                    globalOptions.logLevel = options.logLevel;
                    break;
                case 'logRecordSize':
                    globalOptions.logRecordSize = options.logRecordSize;
                    break;
                case 'pageID':
                    globalOptions.pageID = String(options.pageID);
                    break;
                case 'userAgent':
                    globalOptions.userAgent = options.userAgent || globalOptions.userAgent;
                    break;
                case 'proxy':
                    if (typeof options.proxy !== "string") {
                        delete globalOptions.proxy;
                        utils.setProxy();
                    } else {
                        globalOptions.proxy = options.proxy;
                        utils.setProxy(globalOptions.proxy);
                    }
                    break;
                case 'autoUpdate':
                    globalOptions.autoUpdate = Boolean(options.autoUpdate);
                    if (globalOptions.autoUpdate) {
                        process.env.AUTO_UPDATE = 'true';
                    }
                    break;
                default:
                    logger.warn("setOptions", "Unrecognized option given to setOptions: " + key);
                    break;
            }
        }
    });
}

function extractFbDtsg(html) {
    // Ensure html is a string
    let htmlString = html;
    if (Buffer.isBuffer(html)) {
        htmlString = html.toString('utf8');
    } else if (typeof html !== 'string') {
        htmlString = String(html);
    }
    
    // Try multiple patterns for extracting fb_dtsg
    const patterns = [
        /\["DTSGInitialData",\[\],{"token":"([^"]+)"}]/,
        /\["DTSGInitData",\[\],{"token":"([^"]+)"/,
        /{"dtsg":{"token":"([^"]+)"/,
        /"token":"([^"]+)"/,
        /name="fb_dtsg" value="([^"]+)"/
    ];
    
    for (const pat of patterns) {
        const m = htmlString.match(pat);
        if (m && m[1]) return m[1];
    }

    // Try cheerio as fallback
    try {
        const $ = cheerio.load(htmlString);
        const dtsgInput = $('input[name="fb_dtsg"]').val();
        if (dtsgInput) return dtsgInput;
    } catch (_) {}

    return null;
}

function buildAPI(globalOptions, html, jar) {
    // Ensure html is a string
    let htmlString = html;
    if (Buffer.isBuffer(html)) {
        htmlString = html.toString('utf8');
    } else if (typeof html !== 'string') {
        htmlString = String(html);
    }
    
    const fb_dtsg = extractFbDtsg(htmlString);
    const irisSeqMatch = htmlString.match(/irisSeqID":"([^"]+)"/);
    const irisSeqID = irisSeqMatch ? irisSeqMatch[1] : null;

    // Collect cookies across all Facebook-related domains
    const FB_DOMAINS = [
        "https://www.facebook.com",
        "https://facebook.com",
        "https://mbasic.facebook.com",
        "https://web.facebook.com",
    ];
    const allCookies = [];
    const seenKeys = new Set();
    FB_DOMAINS.forEach(domain => {
        try {
            jar.getCookies(domain).forEach(c => {
                if (!seenKeys.has(c.key)) {
                    seenKeys.add(c.key);
                    allCookies.push(c);
                }
            });
        } catch (_) {}
    });

    const userCookie = allCookies.find(c => c.key === "c_user");
    const i_userCookie = allCookies.find(c => c.key === "i_user");

    if (!userCookie && !i_userCookie) {
        const presentKeys = allCookies.map(c => c.key).join(", ") || "(none)";
        throw new Error(
            "AppState is invalid or expired — no c_user/i_user cookie found.\n" +
            "  Cookies present: " + presentKeys + "\n" +
            "  Make sure your appstate.json was exported from an active Facebook session."
        );
    }
    if (htmlString.includes("/checkpoint/block/?next")) {
        throw new Error("This account has been checkpointed by Facebook.");
    }

    const userID = (i_userCookie || userCookie).value;
    const clientID = (Math.random() * 2147483648 | 0).toString(16);

    let mqttEndpoint = "wss://edge-chat.facebook.com/chat?region=pnb";
    let region = "PNB";
    try {
        const epMatch = htmlString.match(/"endpoint":"([^"]+)"/);
        if (epMatch) {
            let ep = epMatch[1].replace(/\\\//g, '/');
            try {
                const epUrl = new URL(ep);
                epUrl.searchParams.delete('sid');
                epUrl.searchParams.delete('cid');
                region = (epUrl.searchParams.get('region') || "PNB").toUpperCase();
                mqttEndpoint = epUrl.toString();
            } catch (_) {
                mqttEndpoint = ep.replace(/[?&]sid=[^&]*/g, '').replace(/[?&]cid=[^&]*/g, '');
                region = (mqttEndpoint.match(/region=([^&]+)/) || [])[1]?.toUpperCase() || "PNB";
            }
        }
    } catch (_) {}

    // Load config for typing indicator
    let config = { enableTypingIndicator: false, typingDuration: 4000 };
    try {
        const configPaths = [
            path.join(process.cwd(), 'config.json'),
            path.join(__dirname, 'config.json')
        ];
        for (const configPath of configPaths) {
            if (fs.existsSync(configPath)) {
                const fileConfig = JSON.parse(fs.readFileSync(configPath, 'utf8'));
                if (fileConfig && typeof fileConfig === 'object') {
                    if (typeof fileConfig.enableTypingIndicator !== 'undefined') 
                        config.enableTypingIndicator = fileConfig.enableTypingIndicator;
                    if (typeof fileConfig.typingDuration !== 'undefined') 
                        config.typingDuration = fileConfig.typingDuration;
                }
            }
        }
        if (global.GoatBot && global.GoatBot.config) {
            if (typeof global.GoatBot.config.enableTypingIndicator !== 'undefined')
                config.enableTypingIndicator = global.GoatBot.config.enableTypingIndicator;
            if (typeof global.GoatBot.config.typingDuration !== 'undefined')
                config.typingDuration = global.GoatBot.config.typingDuration;
        }
    } catch (e) {
        logger.debug('CONFIG', 'Error loading config.json');
    }

    const refreshFcaConfig = () => {
        try {
            const updatedConfig = { enableTypingIndicator: false, typingDuration: 4000 };
            const configPaths = [
                path.join(process.cwd(), 'config.json'),
                path.join(__dirname, 'config.json')
            ];
            for (const configPath of configPaths) {
                if (fs.existsSync(configPath)) {
                    const fileConfig = JSON.parse(fs.readFileSync(configPath, 'utf8'));
                    if (fileConfig && typeof fileConfig === 'object') {
                        if (typeof fileConfig.enableTypingIndicator !== 'undefined')
                            updatedConfig.enableTypingIndicator = fileConfig.enableTypingIndicator;
                        if (typeof fileConfig.typingDuration !== 'undefined')
                            updatedConfig.typingDuration = fileConfig.typingDuration;
                    }
                }
            }
            if (global.GoatBot && global.GoatBot.config) {
                if (typeof global.GoatBot.config.enableTypingIndicator !== 'undefined')
                    updatedConfig.enableTypingIndicator = global.GoatBot.config.enableTypingIndicator;
                if (typeof global.GoatBot.config.typingDuration !== 'undefined')
                    updatedConfig.typingDuration = global.GoatBot.config.typingDuration;
            }
            ctx.config = updatedConfig;
        } catch (e) {
            logger.debug('CONFIG', 'Failed to refresh fca config');
        }
    };

    const ctx = {
        userID,
        i_userID: i_userCookie ? i_userCookie.value : userID,
        jar,
        clientID,
        globalOptions,
        loggedIn: true,
        access_token: 'NONE',
        clientMutationId: 0,
        mqttClient: undefined,
        lastSeqId: irisSeqID,
        syncToken: undefined,
        mqttEndpoint,
        region,
        firstListen: true,
        fb_dtsg,
        req_ID: 0,
        callback_Task: {},
        wsReqNumber: 0,
        wsTaskNumber: 0,
        reqCallbacks: {},
        threadTypes: {},
        config,
        refreshFcaConfig
    };

    if (global.GoatBot) {
        global.GoatBot.refreshFcaConfig = refreshFcaConfig;
    }

    const defaultFuncs = utils.makeDefaults(htmlString, userID, ctx);

    const api = {
        setOptions: setOptions.bind(null, globalOptions),
        getAppState: () => utils.getAppState(jar),
        getCurrentUserID: () => userID,
        getCtx: () => ctx,
        postFormData: (url, body) => defaultFuncs.postFormData(url, ctx.jar, body),
        checkForUpdate: () => checkForFCAUpdate(),
    };

    // Load core modules
    api.httpPost = require("./src/httpPost")(defaultFuncs, api, ctx);
    api.httpGet = require("./src/httpGet")(defaultFuncs, api, ctx);
    api.httpPostFormData = require("./src/httpPostFormData")(defaultFuncs, api, ctx);
    api.uploadImageToImgbb = require("./src/uploadImageToImgbb")(defaultFuncs, api, ctx);
    api.getFreshDtsg = require("./src/refreshFb_dtsg")(defaultFuncs, api, ctx);
    api.addExternalModule = require("./src/addExternalModule")(defaultFuncs, api, ctx);

    // Load all other modules
    const SRC = path.join(__dirname, "src");
    const exclude = new Set([
        "listenMqtt.js", "sendMessage.js", "OldMessage.js", "e2ee.js",
        "httpPost.js", "httpGet.js", "httpPostFormData.js",
        "uploadImageToImgbb.js", "refreshFb_dtsg.js", "addExternalModule.js"
    ]);

    fs.readdirSync(SRC)
        .filter(f => f.endsWith('.js') && !exclude.has(f))
        .forEach(f => {
            const name = f.replace('.js', '');
            try {
                api[name] = require(path.join(SRC, f))(defaultFuncs, api, ctx);
            } catch (e) {
                logger.warn("SHADOWX", `Failed to load module '${name}': ${e.message}`);
            }
        });

    // Set up messaging with fallback
    const originalSendMessage = require("./src/sendMessage")(defaultFuncs, api, ctx);
    const originalOldMessage = require("./src/OldMessage")(defaultFuncs, api, ctx);

    api.sendMessage = async function(msg, threadID, callback, replyToMessage, isSingleUser) {
        try {
            return await originalSendMessage(msg, threadID, callback, replyToMessage, isSingleUser);
        } catch (error) {
            logger.warn('SEND_MSG', 'sendMessage failed, using OldMessage fallback');
            return originalOldMessage(msg, threadID, callback, replyToMessage, isSingleUser);
        }
    };

    api.OldMessage = originalOldMessage;
    api.sendMessageDM = (msg, threadID, cb, replyTo) => originalOldMessage(msg, threadID, cb, replyTo, true);
    api.sendMessageMqtt = require("./src/sendMessageMqtt")(defaultFuncs, api, ctx);
    
    // Create wrapped MQTT listener to filter binary messages
    const originalListenMqtt = require("./src/listenMqtt")(defaultFuncs, api, ctx);
    api.listenMqtt = function(callback) {
        return originalListenMqtt((err, event) => {
            if (err) {
                // Filter out binary / JSON parse transient errors (safe string check)
                var errMsg = (typeof err.error === 'string') ? err.error
                           : (err.message ? String(err.message) : '');
                if (errMsg.includes('JSON.parse')) return;
                if (err.isBinaryResponse === true) return;
                if (err.res && Buffer.isBuffer(err.res)) return;
                // Also suppress parse_error type events — these are recoverable
                if (err.type === 'parse_error') return;
            }
            callback(err, event);
        });
    };
    
    api.listen = api.listenMqtt;

    // E2EE support
    const e2eeModule = require("./src/e2ee");
    api.e2ee = new e2eeModule.E2EEBridge(ctx, api, defaultFuncs);
    ctx.e2ee = api.e2ee;
    api.connectE2EE = (deviceStorePath) => api.e2ee.connect(deviceStorePath, userID);
    api.listenE2EE = require("./src/listenE2EE")(defaultFuncs, api, ctx);
    api.sendBroadcast = require("./src/sendBroadcast")(defaultFuncs, api, ctx);
    api.sessionGuard = require("./src/sessionGuard")(defaultFuncs, api, ctx);

    // Image upload helper
    Object.defineProperty(api, '_imgUpload', {
        value: async (imageUrl) => {
            try {
                const r = await api.uploadImageToImgbb(imageUrl);
                if (r && r.data) return r.data.url || r.data.display_url;
            } catch (_) { }
            return null;
        },
        enumerable: false,
        writable: true
    });
    Object.defineProperty(ctx, '_imgUpload', {
        value: api._imgUpload,
        enumerable: false,
        writable: true
    });

    return { ctx, defaultFuncs, api };
}

function makeLoginForm(html, jar, email, password) {
    let htmlString = html;
    if (Buffer.isBuffer(html)) {
        htmlString = html.toString('utf8');
    } else if (typeof html !== 'string') {
        htmlString = String(html);
    }
    
    const $ = cheerio.load(htmlString);
    let arr = [];
    $("#login_form input").each((i, v) => arr.push({ val: $(v).val(), name: $(v).attr("name") }));
    arr = arr.filter(v => v.val && v.val.length);
    const form = utils.arrToForm(arr);
    form.lsd = utils.getFrom(htmlString, "[\"LSD\",[],{\"token\":\"", "\"}");
    form.lgndim = Buffer.from(JSON.stringify({ w: 1440, h: 900, aw: 1440, ah: 834, c: 24 })).toString('base64');
    form.email = email;
    form.pass = password;
    form.default_persistent = '0';
    form.lgnrnd = utils.getFrom(htmlString, "name=\"lgnrnd\" value=\"", "\"");
    form.locale = 'en_US';
    form.timezone = '240';
    form.lgnjs = Math.floor(Date.now() / 1000);
    const willBeCookies = htmlString.split("\"_js_");
    willBeCookies.slice(1).forEach(val => {
        try {
            const cookieData = JSON.parse("[\"" + utils.getFrom(val, "", "]") + "]");
            jar.setCookie(utils.formatCookie(cookieData, "facebook"), "https://www.facebook.com");
        } catch (_) { }
    });
    return form;
}

function saveshadowxConfig(userID, botName, region) {
    try {
        const cfgPath = path.join(process.cwd(), "shadowxConfig.json");
        let existing = {};
        try { existing = JSON.parse(fs.readFileSync(cfgPath, "utf8")); } catch (_) {}
        const config = Object.assign({}, existing, {
            botUID: userID,
            botName: botName || existing.botName || "",
            region: region || existing.region || "",
            version: VERSION,
            lastLogin: new Date().toISOString(),
            logs: existing.logs !== undefined ? existing.logs : false,
        });
        fs.writeFileSync(cfgPath, JSON.stringify(config, null, 2));
    } catch (_) {}
}

function makeLoginHandler(jar, email, password, loginOptions, callback, prCallback) {
    return async function (res) {
        try {
            let html = res.body;
            if (Buffer.isBuffer(html)) {
                html = html.toString('utf8');
            }
            
            const form = makeLoginForm(html, jar, email, password);
            
            logger.info("SHADOWX", "Logging in with email/password...");
            const loginRes = await utils.post(
                "https://www.facebook.com/login/device-based/regular/login/?login_attempt=1&lwv=110",
                jar,
                form,
                loginOptions
            );
            await utils.saveCookies(jar)(loginRes);
            
            const headers = loginRes.headers;
            if (!headers.location) throw new Error("Wrong username/password.");
            
            if (headers.location.includes('/checkpoint/')) {
                // Handle 2FA
                const checkpointRes = await utils.get(headers.location, jar, null, loginOptions);
                await utils.saveCookies(jar)(checkpointRes);
                let checkpointHtml = checkpointRes.body;
                if (Buffer.isBuffer(checkpointHtml)) {
                    checkpointHtml = checkpointHtml.toString('utf8');
                }
                const $ = cheerio.load(checkpointHtml);
                let checkpointForm = [];
                $("form input").each((i, v) => checkpointForm.push({ val: $(v).val(), name: $(v).attr("name") }));
                checkpointForm = checkpointForm.filter(v => v.val && v.val.length);
                const cpForm = utils.arrToForm(checkpointForm);
                
                if (checkpointHtml.includes("checkpoint/?next")) {
                    return new Promise((resolve, reject) => {
                        const submit2FA = async (code) => {
                            try {
                                cpForm.approvals_code = code;
                                cpForm['submit[Continue]'] = $("#checkpointSubmitButton").html();
                                const approvalRes = await utils.post(
                                    "https://www.facebook.com/checkpoint/?next=https%3A%2F%2Fwww.facebook.com%2Fhome.php",
                                    jar,
                                    cpForm,
                                    loginOptions
                                );
                                await utils.saveCookies(jar)(approvalRes);
                                const approvalError = $("#approvals_code").parent().attr("data-xui-error");
                                if (approvalError) throw new Error("Invalid 2FA code.");
                                cpForm.name_action_selected = 'dont_save';
                                const finalRes = await utils.post(
                                    "https://www.facebook.com/checkpoint/?next=https%3A%2F%2Fwww.facebook.com%2Fhome.php",
                                    jar,
                                    cpForm,
                                    loginOptions
                                );
                                await utils.saveCookies(jar)(finalRes);
                                const appState = utils.getAppState(jar);
                                resolve(await loginHelper(appState, email, password, loginOptions, callback));
                            } catch (error) {
                                reject(error);
                            }
                        };
                        throw {
                            error: 'login-approval',
                            continue: submit2FA
                        };
                    });
                }
                
                if (!loginOptions.forceLogin) throw new Error("Couldn't login. Facebook might have blocked this account.");
                cpForm['submit[This was me]'] = checkpointHtml.includes("Suspicious Login Attempt") ? "This was me" : "This Is Okay";
                await utils.post("https://www.facebook.com/checkpoint/?next=https%3A%2F%2Fwww.facebook.com%2Fhome.php", jar, cpForm, loginOptions);
                cpForm.name_action_selected = 'save_device';
                await utils.post("https://www.facebook.com/checkpoint/?next=https%3A%2F%2Fwww.facebook.com%2Fhome.php", jar, cpForm, loginOptions);
                const appState = utils.getAppState(jar);
                return await loginHelper(appState, email, password, loginOptions, callback);
            }
            
            await utils.get('https://www.facebook.com/', jar, null, loginOptions);
            return await utils.saveCookies(jar);
        } catch (error) {
            callback(error);
        }
    };
}

async function loginHelper(appState, email, password, globalOptions, callback) {
    const jar = utils.getJar();

    let mainPromise;
    if (appState) {
        let parsed = appState;
        if (typeof parsed === "string") {
            try { parsed = JSON.parse(parsed); } catch (_) {}
        }
        parsed.forEach(c => {
            const cookieName = c.key || c.name;
            if (!cookieName || !c.value) return;
            const domain = c.domain || '.facebook.com';
            const expires = c.expirationDate
                ? new Date(c.expirationDate * 1000).toUTCString()
                : (c.expires || '');
            const str = `${cookieName}=${c.value}; expires=${expires}; domain=${domain}; path=${c.path || '/'};`;
            const url = 'http://' + domain.replace(/^\./, 'www.');
            try { jar.setCookie(str, url); } catch (_) { }
        });
        mainPromise = utils.get('https://www.facebook.com/', jar, null, globalOptions, { noRef: true })
            .then(utils.saveCookies(jar));
    } else {
        mainPromise = utils.get("https://www.facebook.com/", null, null, globalOptions, { noRef: true })
            .then(utils.saveCookies(jar))
            .then(makeLoginHandler(jar, email, password, globalOptions, callback))
            .then(() => utils.get('https://www.facebook.com/', jar, null, globalOptions).then(utils.saveCookies(jar)));
    }

    let ctx, api;
    mainPromise = mainPromise
        .then(async res => {
            let body = res.body;
            if (Buffer.isBuffer(body)) {
                body = body.toString('utf8');
            }
            
            // Handle redirects
            const reg = /<meta http-equiv="refresh" content="0;url=([^"]+)[^>]+>/;
            const redirect = reg.exec(body);
            if (redirect && redirect[1]) {
                res = await utils.get(redirect[1], jar, null, globalOptions).then(utils.saveCookies(jar));
                body = res.body;
                if (Buffer.isBuffer(body)) {
                    body = body.toString('utf8');
                }
            }
            
            // Check user agent
            const mobileAgentRegex = /MPageLoadClientMetrics/gs;
            if (!mobileAgentRegex.test(body)) {
                globalOptions.userAgent = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36";
                res = await utils.get('https://www.facebook.com/', jar, null, globalOptions, { noRef: true }).then(utils.saveCookies(jar));
                body = res.body;
                if (Buffer.isBuffer(body)) {
                    body = body.toString('utf8');
                }
            }
            
            return { res, body };
        })
        .then(({ res, body }) => {
            const built = buildAPI(globalOptions, body, jar);
            ctx = built.ctx;
            api = built.api;
            return res;
        });

    // Handle page ID if specified
    if (globalOptions.pageID) {
        mainPromise = mainPromise
            .then(() => utils.get(
                `https://www.facebook.com/${globalOptions.pageID}/messages/?section=messages&subsection=inbox`,
                jar, null, globalOptions
            ))
            .then(resData => {
                let body = resData.body;
                if (Buffer.isBuffer(body)) {
                    body = body.toString('utf8');
                }
                let url = utils.getFrom(body, 'window.location.replace("https:\\/\\/www.facebook.com\\', '");').split('\\').join('');
                url = url.substring(0, url.length - 1);
                return utils.get('https://www.facebook.com' + url, jar, null, globalOptions);
            });
    }

    mainPromise
        .then(async () => {
            let botName = "";
            try {
                const users = await api.getUserInfo([ctx.userID]);
                if (users && users[ctx.userID]) botName = users[ctx.userID].name || "";
            } catch (_) {}

            logger.banner(NAME, VERSION, ctx.userID, botName, ctx.region, false);
            saveshadowxConfig(ctx.userID, botName, ctx.region);
            
            // Check for updates after successful login
            logger.info('SHADOWX', 'Checking for updates...');
            checkForFCAUpdate().catch(() => {
                logger.debug('UPDATE', 'Update check completed');
            });
            
            callback(null, api);
        })
        .catch(e => {
            logger.error('SHADOWX', 'Login failed');
            callback(e);
        });
}

function login(loginData, options, callback) {
    if (typeof options === "function") {
        callback = options;
        options = {};
    }

    const globalOptions = {
        selfListen: false,
        listenEvents: true,
        listenTyping: false,
        updatePresence: false,
        forceLogin: false,
        autoMarkDelivery: false,
        autoMarkRead: false,
        autoReconnect: true,
        logRecordSize: 100,
        online: false,
        emitReady: false,
        autoUpdate: true,
        userAgent: "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/18.4 Safari/605.1.15"
    };

    setOptions(globalOptions, options || {});

    let promise;
    if (typeof callback !== "function" && typeof callback !== "undefined") {
        callback = undefined;
    }
    if (!callback) {
        promise = new Promise((resolve, reject) => {
            callback = (err, api) => err ? reject(err) : resolve(api);
        });
    }

    if (loginData.email && loginData.password) {
        setOptions(globalOptions, {
            logLevel: "silent",
            forceLogin: true,
            userAgent: "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36"
        });
        loginHelper(loginData.appState, loginData.email, loginData.password, globalOptions, callback);
    } else if (loginData.appState) {
        setOptions(globalOptions, options);
        loginHelper(loginData.appState, loginData.email, loginData.password, globalOptions, callback);
    } else {
        callback(new Error("loginData must contain either appState or email+password."));
    }

    return promise;
}

module.exports = login;
module.exports.login = login;
module.exports.VERSION = VERSION;
module.exports.NAME = NAME;
module.exports.checkUpdate = checkForFCAUpdate;
