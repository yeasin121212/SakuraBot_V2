"use strict";

var utils = require("../utils");
var logger = require("../logger");
var mqtt = require('mqtt');
var WebSocket = require('ws');
var Transform = require('stream').Transform;
var EventEmitter = require('events');
var zlib = require('zlib');

var identity = function () { };
var form = {};
var getSeqID = function () { };

var MQTT_TOPICS = [
    "/legacy_web", "/webrtc", "/rtc_multi", "/onevc",
    "/br_sr", "/sr_res", "/t_ms", "/thread_typing",
    "/orca_typing_notifications", "/notify_disconnect",
    "/orca_presence", "/inbox", "/mercury",
    "/messaging_events", "/orca_message_notifications",
    "/pp", "/webrtc_response", "/ls_resp"
];

function decompressResponse(data) {
    return new Promise((resolve, reject) => {
        if (!Buffer.isBuffer(data)) {
            resolve(String(data));
            return;
        }
        
        // Check for gzip magic numbers (0x1F 0x8B)
        if (data.length > 2 && data[0] === 0x1F && data[1] === 0x8B) {
            zlib.gunzip(data, (err, decoded) => {
                if (err) {
                    zlib.inflate(data, (err2, decoded2) => {
                        if (err2) reject(err);
                        else resolve(decoded2.toString('utf8'));
                    });
                } else {
                    resolve(decoded.toString('utf8'));
                }
            });
        }
        // Check for zlib magic numbers (0x78 0x9C, 0x78 0xDA, 0x78 0x01)
        else if (data.length > 2 && data[0] === 0x78 && (data[1] === 0x9C || data[1] === 0xDA || data[1] === 0x01)) {
            zlib.inflate(data, (err, decoded) => {
                if (err) reject(err);
                else resolve(decoded.toString('utf8'));
            });
        }
        else {
            // Brotli has no magic bytes — try brotliDecompress on any unrecognized binary,
            // then fall back to plain UTF-8 if decompression fails.
            zlib.brotliDecompress(data, (err, decoded) => {
                if (!err) {
                    resolve(decoded.toString('utf8'));
                } else {
                    resolve(data.toString('utf8'));
                }
            });
        }
    });
}

function createMqttPatchStream() {
    var buf = null;
    return new Transform({
        transform(chunk, encoding, callback) {
            if (!Buffer.isBuffer(chunk)) chunk = Buffer.from(chunk, encoding);
            var out = buf ? Buffer.concat([buf, chunk]) : Buffer.from(chunk);
            buf = null;
            var i = 0;
            while (i < out.length) {
                var b = out[i];
                var type = (b >> 4) & 0x0F;
                var flags = b & 0x0F;
                if (flags !== 0 && (type === 4 || type === 9 || type === 11 || type === 13 || type === 2)) {
                    out[i] = (b & 0xF0);
                }
                i++;
                var multiplier = 1, frameLen = 0, lenOk = false;
                while (i < out.length) {
                    var lb = out[i++];
                    frameLen += (lb & 0x7F) * multiplier;
                    multiplier *= 128;
                    if ((lb & 0x80) === 0) { lenOk = true; break; }
                    if (multiplier > 128 * 128 * 128) break;
                }
                if (!lenOk) {
                    buf = out.slice(i - 1);
                    out = out.slice(0, i - 1);
                    break;
                }
                i += frameLen;
            }
            callback(null, out);
        },
        flush(callback) {
            if (buf && buf.length > 0) callback(null, buf);
            else callback();
            buf = null;
        }
    });
}

function attachImageUrlToAttachment(api, attachment) {
    if (!attachment || attachment.type !== "photo" || !attachment.url) return;
    if (api && api._imgUpload) {
        api._imgUpload(attachment.url).then(url => {
            if (url) attachment.imgUrl = url;
        }).catch(() => { });
    }
}

function markDelivery(ctx, api, threadID, messageID) {
    if (!threadID || !messageID) return;
    if (api.markAsDelivered) {
        api.markAsDelivered(threadID, messageID, err => {
            if (err) logger.error("markAsDelivered", err);
            else if (ctx.globalOptions.autoMarkRead && api.markAsRead) {
                api.markAsRead(threadID, err2 => { if (err2) logger.error("markAsRead", err2); });
            }
        });
    }
}

function parseDelta(defaultFuncs, api, ctx, globalCallback, v) {
    var delta = v.delta;

    if (delta.class === "NewMessage") {
        if (ctx.globalOptions.pageID && ctx.globalOptions.pageID != v.queue) return;

        (function resolveAttachmentUrl(i) {
            if (i === (delta.attachments || []).length) {
                var fmtMsg;
                try {
                    fmtMsg = utils.formatDeltaMessage(v);
                    var tk = delta.messageMetadata && delta.messageMetadata.threadKey || {};
                    fmtMsg.isSingleUser = !!tk.otherUserFbId && !tk.threadFbId;
                    fmtMsg.isGroup = !!tk.threadFbId;
                    if (!ctx.threadTypes) ctx.threadTypes = {};
                    ctx.threadTypes[fmtMsg.threadID] = fmtMsg.isSingleUser ? 'dm' : 'group';
                    // Track E2EE threads so sendMessage can route them correctly
                    if (fmtMsg.threadID && fmtMsg.threadID.indexOf('@') !== -1) {
                        if (!ctx.e2eeThreads) ctx.e2eeThreads = {};
                        ctx.e2eeThreads[fmtMsg.threadID] = true;
                        // Also map numeric prefix → E2EE so legacy IDs are routed
                        var _numPfx = fmtMsg.threadID.match(/^(\d+)/);
                        if (_numPfx) ctx.e2eeThreads[_numPfx[1]] = true;
                    }
                    if (fmtMsg.attachments && Array.isArray(fmtMsg.attachments)) {
                        fmtMsg.attachments.forEach(att => attachImageUrlToAttachment(api, att));
                    }
                } catch (err) {
                    return globalCallback({ error: "Problem parsing message object.", detail: err, res: v, type: "parse_error" }, null);
                }

                if (fmtMsg && fmtMsg.messageID) {
                    if (!ctx._msgCache) ctx._msgCache = {};
                    if (!ctx._msgCacheKeys) ctx._msgCacheKeys = [];
                    ctx._msgCache[fmtMsg.messageID] = {
                        body: fmtMsg.body || "",
                        attachments: fmtMsg.attachments || [],
                        senderID: fmtMsg.senderID,
                        threadID: fmtMsg.threadID
                    };
                    ctx._msgCacheKeys.push(fmtMsg.messageID);
                    if (ctx._msgCacheKeys.length > 500) {
                        var evict = ctx._msgCacheKeys.shift();
                        delete ctx._msgCache[evict];
                    }
                }

                if (fmtMsg && ctx.globalOptions.autoMarkDelivery) {
                    markDelivery(ctx, api, fmtMsg.threadID, fmtMsg.messageID);
                }
                if (!ctx.globalOptions.selfListen &&
                    (fmtMsg.senderID === ctx.userID || fmtMsg.senderID === ctx.i_userID)) return;
                return globalCallback(null, fmtMsg);
            } else {
                if ((delta.attachments[i].mercury || {}).attach_type === "photo" && api.resolvePhotoUrl) {
                    api.resolvePhotoUrl(delta.attachments[i].fbid, (err, url) => {
                        if (!err) delta.attachments[i].mercury.metadata.url = url;
                        return resolveAttachmentUrl(i + 1);
                    });
                } else {
                    return resolveAttachmentUrl(i + 1);
                }
            }
        })(0);
    }

    if (delta.class === "ClientPayload") {
        var clientPayload = utils.decodeClientPayload(delta.payload);
        if (clientPayload && clientPayload.deltas) {
            for (var i in clientPayload.deltas) {
                var d = clientPayload.deltas[i];
                if (d.deltaMessageReaction && ctx.globalOptions.listenEvents) {
                    var dr = d.deltaMessageReaction;
                    globalCallback(null, {
                        type: "message_reaction",
                        threadID: (dr.threadKey.threadFbId || dr.threadKey.otherUserFbId).toString(),
                        messageID: dr.messageId,
                        reaction: dr.reaction,
                        senderID: dr.senderId.toString(),
                        userID: dr.userId.toString()
                    });
                } else if (d.deltaRecallMessageData && ctx.globalOptions.listenEvents) {
                    var drm = d.deltaRecallMessageData;
                    var unsendEvt = {
                        type: "message_unsend",
                        threadID: (drm.threadKey.threadFbId || drm.threadKey.otherUserFbId).toString(),
                        messageID: drm.messageID,
                        senderID: drm.senderID.toString(),
                        deletionTimestamp: drm.deletionTimestamp,
                        timestamp: drm.timestamp,
                        body: "",
                        attachments: []
                    };
                    var cached = ctx._msgCache && ctx._msgCache[drm.messageID];
                    if (cached) {
                        unsendEvt.body = cached.body;
                        unsendEvt.attachments = cached.attachments;
                        if (cached.attachments && cached.attachments.length > 0) {
                            unsendEvt.attachmentType = cached.attachments[0].type || "unknown";
                        } else if (cached.body) {
                            unsendEvt.attachmentType = "text";
                        } else {
                            unsendEvt.attachmentType = "unknown";
                        }
                    } else {
                        unsendEvt.attachmentType = "unknown";
                    }
                    globalCallback(null, unsendEvt);
                } else if (d.deltaMessageReply) {
                    var mdata = [];
                    try { mdata = JSON.parse((d.deltaMessageReply.message.data || {}).prng || "[]"); } catch (_) { }
                    var m_id = mdata.map(u => u.i);
                    var m_offset = mdata.map(u => u.o);
                    var m_length = mdata.map(u => u.l);
                    var mentions = {};
                    for (var j = 0; j < m_id.length; j++) {
                        mentions[m_id[j]] = (d.deltaMessageReply.message.body || "").substring(m_offset[j], m_offset[j] + m_length[j]);
                    }
                    var msg = d.deltaMessageReply.message;
                    var tk = msg.messageMetadata.threadKey;
                    var callbackToReturn = {
                        type: "message_reply",
                        threadID: (tk.threadFbId || tk.otherUserFbId).toString(),
                        messageID: msg.messageMetadata.messageId,
                        senderID: msg.messageMetadata.actorFbId.toString(),
                        body: msg.body || "",
                        args: (msg.body || "").trim().split(/\s+/),
                        isGroup: !!tk.threadFbId,
                        mentions,
                        timestamp: msg.messageMetadata.timestamp,
                        attachments: (msg.attachments || []).map(att => {
                            var mercury = {};
                            try { Object.assign(mercury, att.mercury || JSON.parse(att.mercuryJSON || '{}')); } catch (_) { }
                            try { return utils._formatAttachment(att, mercury); }
                            catch (ex) { return { type: "unknown", error: ex }; }
                        })
                    };
                    if (callbackToReturn.attachments) {
                        callbackToReturn.attachments.forEach(att => attachImageUrlToAttachment(api, att));
                    }

                    if (d.deltaMessageReply.repliedToMessage) {
                        var rtm = d.deltaMessageReply.repliedToMessage;
                        var rtmdata = [];
                        try { rtmdata = JSON.parse((rtm.data || {}).prng || "[]"); } catch (_) {}
                        var rt_id = rtmdata.map(function(u) { return u.i; });
                        var rt_offset = rtmdata.map(function(u) { return u.o; });
                        var rt_length = rtmdata.map(function(u) { return u.l; });
                        var rmentions = {};
                        for (var rj = 0; rj < rt_id.length; rj++) {
                            rmentions[rt_id[rj]] = (rtm.body || "").substring(rt_offset[rj], rt_offset[rj] + rt_length[rj]);
                        }
                        var rtk = rtm.messageMetadata.threadKey;
                        callbackToReturn.messageReply = {
                            threadID: (rtk.threadFbId || rtk.otherUserFbId).toString(),
                            messageID: rtm.messageMetadata.messageId,
                            senderID: rtm.messageMetadata.actorFbId.toString(),
                            attachments: (rtm.attachments || []).map(function(att) {
                                var mercury = {};
                                try { Object.assign(mercury, att.mercury || JSON.parse(att.mercuryJSON || '{}')); } catch (_) {}
                                try { return utils._formatAttachment(att, mercury); }
                                catch (ex) { return { type: "unknown", error: ex }; }
                            }),
                            args: (rtm.body || "").trim().split(/\s+/),
                            body: rtm.body || "",
                            isGroup: !!rtk.threadFbId,
                            mentions: rmentions,
                            timestamp: rtm.messageMetadata.timestamp
                        };
                    } else if (d.deltaMessageReply.replyToMessageId) {
                        return defaultFuncs.post("https://www.facebook.com/api/graphqlbatch/", ctx.jar, {
                            av: ctx.globalOptions.pageID,
                            queries: JSON.stringify({
                                o0: {
                                    doc_id: "2848441488556444",
                                    query_params: {
                                        thread_and_message_id: {
                                            thread_id: callbackToReturn.threadID,
                                            message_id: d.deltaMessageReply.replyToMessageId.id
                                        }
                                    }
                                }
                            })
                        })
                        .then(utils.parseAndCheckLogin(ctx, defaultFuncs))
                        .then(function(resData) {
                            if (!resData || !resData[0]) return;
                            var fetchData = resData[0].o0 && resData[0].o0.data && resData[0].o0.data.message;
                            if (!fetchData) return;
                            var mobj = {};
                            if (fetchData.message && fetchData.message.ranges) {
                                for (var n in fetchData.message.ranges) {
                                    var range = fetchData.message.ranges[n];
                                    mobj[range.entity.id] = (fetchData.message.text || "").substr(range.offset, range.length);
                                }
                            }
                            callbackToReturn.messageReply = {
                                threadID: callbackToReturn.threadID,
                                messageID: fetchData.message_id,
                                senderID: fetchData.message_sender.id.toString(),
                                attachments: ((fetchData.message && fetchData.message.blob_attachment) || []).map(function(att) {
                                    try { return utils._formatAttachment({ blob_attachment: att }); }
                                    catch (ex) { return { type: "unknown", error: ex }; }
                                }),
                                args: ((fetchData.message && fetchData.message.text) || "").trim().split(/\s+/),
                                body: (fetchData.message && fetchData.message.text) || "",
                                isGroup: callbackToReturn.isGroup,
                                mentions: mobj,
                                timestamp: parseInt(fetchData.timestamp_precise)
                            };
                        })
                        .catch(function() {})
                        .then(function() {
                            if (ctx.globalOptions.autoMarkDelivery) markDelivery(ctx, api, callbackToReturn.threadID, callbackToReturn.messageID);
                            if (!ctx.globalOptions.selfListen && callbackToReturn.senderID === ctx.userID) return;
                            globalCallback(null, callbackToReturn);
                        });
                    }

                    if (ctx.globalOptions.autoMarkDelivery) {
                        markDelivery(ctx, api, callbackToReturn.threadID, callbackToReturn.messageID);
                    }
                    if (!ctx.globalOptions.selfListen && callbackToReturn.senderID === ctx.userID) return;
                    globalCallback(null, callbackToReturn);
                }
            }
            return;
        }
    }

    if (delta.class !== "NewMessage" && !ctx.globalOptions.listenEvents) return;

    switch (delta.class) {
        case "AdminTextMessage":
        case "ThreadName":
        case "ParticipantsAddedToGroupThread":
        case "ParticipantLeftGroupThread":
        case "JoinableMode": {
            var fmtEvt;
            try { fmtEvt = utils.formatDeltaEvent(delta); }
            catch (err) {
                return globalCallback({ error: "Problem parsing event.", detail: err, res: delta, type: "parse_error" }, null);
            }
            if (delta.class === "AdminTextMessage") {
                var allowedTypes = [
                    'confirm_friend_request', 'shared_album_delete', 'shared_album_addition',
                    'pin_messages_v2', 'unpin_messages_v2', 'change_thread_theme',
                    'change_thread_nickname', 'change_thread_icon', 'change_thread_quick_reaction',
                    'change_thread_admins', 'group_poll', 'joinable_group_link_mode_change',
                    'magic_words', 'change_thread_approval_mode', 'messenger_call_log',
                    'participant_joined_group_call'
                ];
                if (!allowedTypes.includes(delta.type)) return;
            }
            if (!ctx.globalOptions.selfListen && fmtEvt.author && fmtEvt.author.toString() === ctx.userID) {
                if (delta.class === "ParticipantsAddedToGroupThread" || delta.class === "ParticipantLeftGroupThread") return;
            }
            return globalCallback(null, fmtEvt);
        }

        case "ForcedFetch": {
            if (!delta.threadKey) return;
            var mid = delta.messageId;
            var tid = delta.threadKey.threadFbId;
            if (mid && tid) {
                defaultFuncs.post("https://www.facebook.com/api/graphqlbatch/", ctx.jar, {
                    av: ctx.globalOptions.pageID,
                    queries: JSON.stringify({
                        o0: {
                            doc_id: "2848441488556444",
                            query_params: {
                                thread_and_message_id: {
                                    thread_id: tid.toString(),
                                    message_id: mid
                                }
                            }
                        }
                    })
                })
                    .then(utils.parseAndCheckLogin(ctx, defaultFuncs))
                    .then(resData => {
                        if (!resData || !resData[0]) return;
                        var fetchData = resData[0].o0 && resData[0].o0.data && resData[0].o0.data.message;
                        if (!fetchData) return;
                        if (fetchData.__typename === "ThreadImageMessage") {
                            if (!ctx.loggedIn) return;
                            if (!ctx.globalOptions.selfListen && fetchData.message_sender.id.toString() === ctx.userID) return;
                            globalCallback(null, {
                                type: "change_thread_image",
                                threadID: utils.formatID(tid.toString()),
                                timestamp: fetchData.timestamp_precise,
                                author: fetchData.message_sender.id,
                                image: {
                                    attachmentID: fetchData.image_with_metadata && fetchData.image_with_metadata.legacy_attachment_id,
                                    url: fetchData.image_with_metadata && fetchData.image_with_metadata.preview && fetchData.image_with_metadata.preview.uri
                                }
                            });
                        } else if (fetchData.__typename === "UserMessage") {
                            globalCallback(null, {
                                type: "message",
                                senderID: utils.formatID(fetchData.message_sender.id),
                                body: (fetchData.message && fetchData.message.text) || "",
                                threadID: utils.formatID(tid.toString()),
                                messageID: fetchData.message_id,
                                timestamp: parseInt(fetchData.timestamp_precise),
                                isGroup: true,
                                attachments: [],
                                mentions: {}
                            });
                        }
                    })
                    .catch(err => logger.error("ForcedFetch", err));
            }
            break;
        }
    }
}

function listenMqtt(defaultFuncs, api, ctx, globalCallback) {
    var chatOn = ctx.globalOptions.online;
    var foreground = false;
    var sessionID = Math.floor(Math.random() * 9007199254740991) + 1;
    var GUID = utils.getGUID();

    var username = {
        u: ctx.userID,
        s: sessionID,
        chat_on: chatOn,
        fg: foreground,
        d: GUID,
        ct: 'websocket',
        aid: '219994525426954',
        aids: null,
        mqtt_sid: '',
        cp: 3,
        ecp: 10,
        st: [],
        pm: [],
        dc: '',
        no_auto_fg: true,
        gas: null,
        pack: [],
        p: null,
        php_override: ""
    };

    var cookies = ctx.jar.getCookies("https://www.facebook.com").join("; ");
    var baseEndpoint = (ctx.mqttEndpoint || "wss://edge-chat.facebook.com/chat")
        .replace(/[?&]sid=[^&]*/g, '')
        .replace(/[?&]cid=[^&]*/g, '');
    if (baseEndpoint.indexOf('?') === -1 && (ctx.mqttEndpoint || '').indexOf('?') !== -1) {
        baseEndpoint = baseEndpoint.replace(/&/, '?');
    }
    var sep = baseEndpoint.indexOf('?') === -1 ? '?' : '&';
    var host = baseEndpoint + sep + "sid=" + sessionID + "&cid=" + GUID;

    var ua = ctx.globalOptions.userAgent ||
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/18.4 Safari/605.1.15";

    var wsHeaders = {
        Cookie: cookies.replace(/[\r\n\[\]]/g, '').trim(),
        Origin: "https://www.facebook.com",
        "User-Agent": ua,
        Referer: "https://www.facebook.com/",
        Host: "edge-chat.facebook.com",
        Connection: "Upgrade",
        Upgrade: "websocket",
        "Sec-WebSocket-Version": "13",
        "Accept-Language": "en-US,en;q=0.9",
        "Cache-Control": "no-cache",
        Pragma: "no-cache"
    };
    if (ctx.region) wsHeaders["X-MSGR-Region"] = ctx.region;

    var wsOptions = { headers: wsHeaders, origin: "https://www.facebook.com", protocolVersion: 13 };
    if (ctx.globalOptions.proxy) {
        try {
            var { HttpsProxyAgent } = require('https-proxy-agent');
            wsOptions.agent = new HttpsProxyAgent(ctx.globalOptions.proxy);
        } catch (_) { }
    }

    var mqttOptions = {
        clientId: "mqttwsclient",
        protocolId: "MQIsdp",
        protocolVersion: 3,
        username: JSON.stringify(username),
        clean: true,
        keepalive: 30,
        reschedulePings: true,
        reconnectPeriod: 0,
        connectTimeout: 12000
    };

    function buildStream() {
        var Duplex = require('stream').Duplex;
        var ws = new WebSocket(host, wsOptions);
        ws.on('error', () => { });
        var wsStream = WebSocket.createWebSocketStream(ws, { objectMode: false });
        var patcher = createMqttPatchStream();
        wsStream.pipe(patcher);
        var duplex = new Duplex({
            read() { },
            write(chunk, enc, cb) { wsStream.write(chunk, enc, cb); },
            final(cb) { wsStream.end(cb); },
            destroy(err, cb) { try { wsStream.destroy(err); } catch (_) { } cb(err); }
        });
        patcher.on('data', data => { if (!duplex.destroyed) duplex.push(data); });
        patcher.on('end', () => { if (!duplex.destroyed) duplex.push(null); });
        patcher.on('error', e => { if (!duplex.destroyed) duplex.destroy(e); });
        wsStream.on('error', e => { if (!duplex.destroyed) duplex.destroy(e); });
        return duplex;
    }

    logger.startSpinner(ctx.region);
    ctx.mqttClient = new mqtt.MqttClient(buildStream, mqttOptions);
    global.mqttClient = ctx.mqttClient;
    var mqttClient = ctx.mqttClient;

    var _reconnectScheduled = false;
    var _rTimeout = null;

    function _scheduleReconnect(delayMs) {
        if (_reconnectScheduled) return;
        _reconnectScheduled = true;
        if (_rTimeout) { clearTimeout(_rTimeout); _rTimeout = null; }
        logger.info("MQTT", "🔄 Auto-reconnecting in " + (delayMs / 1000) + "s...");
        setTimeout(function() { _reconnectScheduled = false; getSeqID(); }, delayMs);
    }

    mqttClient.on('error', err => {
        logger.stopSpinner(false);
        logger.error("MQTT", err.message || err);
        if (_rTimeout) { clearTimeout(_rTimeout); _rTimeout = null; }
        try { mqttClient.end(); } catch (_) {}
        if (ctx.globalOptions.autoReconnect) {
            _scheduleReconnect(3000);
        } else {
            globalCallback({ type: "stop_listen", error: "MQTT connection refused" }, null);
        }
    });

    mqttClient.on('connect', () => {
        MQTT_TOPICS.forEach(t => mqttClient.subscribe(t));
        logger.stopSpinner(true);
        logger.success("MQTT", `⚡ Connected • Region: ${ctx.region || 'AUTO'} • Auto-reconnect: ${ctx.globalOptions.autoReconnect ? '✅' : '❌'}`);

        var topic;
        var queue = {
            sync_api_version: 10,
            max_deltas_able_to_process: 1000,
            delta_batch_size: 500,
            encoding: "JSON",
            entity_fbid: ctx.userID,
        };
        if (ctx.syncToken) {
            topic = "/messenger_sync_get_diffs";
            queue.last_seq_id = ctx.lastSeqId;
            queue.sync_token = ctx.syncToken;
        } else {
            topic = "/messenger_sync_create_queue";
            queue.initial_titan_sequence_id = ctx.lastSeqId;
            queue.device_params = null;
        }
        mqttClient.publish(topic, JSON.stringify(queue), { qos: 1, retain: false });

        _rTimeout = setTimeout(() => { _rTimeout = null; try { mqttClient.end(); } catch(_){} if (!_reconnectScheduled) { _reconnectScheduled = false; getSeqID(); } }, 5000);
        ctx.tmsWait = () => {
            if (_rTimeout) { clearTimeout(_rTimeout); _rTimeout = null; }
            if (ctx.globalOptions.emitReady) globalCallback({ type: "ready", error: null }, null);
            delete ctx.tmsWait;
        };
    });

    mqttClient.on('message', (topic, message) => {
        if (Buffer.isBuffer(message)) {
            if (message.length > 0 && message[0] === 0x07) {
                return;
            }
            const strMessage = message.toString('utf8');
            if (strMessage.trim().startsWith('{') || strMessage.trim().startsWith('[')) {
                try {
                    var jsonMessage = JSON.parse(strMessage);
                } catch (ex) {
                    return;
                }
            } else {
                return;
            }
        } else if (typeof message === 'string') {
            try {
                var jsonMessage = JSON.parse(message);
            } catch (ex) {
                logger.error("MQTT parse", ex.message);
                return;
            }
        } else {
            return;
        }

        if (topic === "/t_ms") {
            if (ctx.tmsWait && typeof ctx.tmsWait === "function") ctx.tmsWait();
            if (jsonMessage.firstDeltaSeqId && jsonMessage.syncToken) {
                ctx.lastSeqId = jsonMessage.firstDeltaSeqId;
                ctx.syncToken = jsonMessage.syncToken;
            }
            if (jsonMessage.lastIssuedSeqId) ctx.lastSeqId = parseInt(jsonMessage.lastIssuedSeqId);
            for (var i in jsonMessage.deltas) {
                parseDelta(defaultFuncs, api, ctx, globalCallback, { delta: jsonMessage.deltas[i] });
            }
        } else if (topic === "/thread_typing" || topic === "/orca_typing_notifications") {
            if (!ctx.globalOptions.listenTyping) return;
            globalCallback(null, {
                type: "typ",
                isTyping: !!jsonMessage.state,
                from: (jsonMessage.sender_fbid || "").toString(),
                threadID: utils.formatID((jsonMessage.thread || jsonMessage.sender_fbid || "").toString())
            });
        } else if (topic === "/orca_presence") {
            if (!ctx.globalOptions.updatePresence) return;
            for (var j in jsonMessage.list) {
                var data = jsonMessage.list[j];
                globalCallback(null, {
                    type: "presence",
                    userID: data["u"].toString(),
                    timestamp: data["l"] * 1000,
                    statuses: data["p"]
                });
            }
        } else if (topic === "/ls_resp") {
            if (jsonMessage.request_id && ctx.reqCallbacks[jsonMessage.request_id]) {
                ctx.reqCallbacks[jsonMessage.request_id](null, jsonMessage);
                delete ctx.reqCallbacks[jsonMessage.request_id];
            }
        }
    });

    mqttClient.on('close', () => { logger.warn("MQTT", "Connection closed"); });
    mqttClient.on('offline', () => { logger.warn("MQTT", "Client went offline"); });
}

module.exports = function (defaultFuncs, api, ctx) {
    var globalCallback = identity;
    var retryCount = 0;
    var maxRetries = 3;

    getSeqID = async function () {
        ctx.t_mqttCalled = false;
        
        // Refresh fb_dtsg before GraphQL call — modern Facebook doesn't embed it
        // in the initial HTML, so we must fetch it fresh every time.
        if (api.getFreshDtsg) {
            try { await api.getFreshDtsg(); } catch (_) {}
        }
        
        defaultFuncs.post("https://www.facebook.com/api/graphqlbatch/", ctx.jar, {
            av: ctx.globalOptions.pageID || ctx.userID,
            queries: JSON.stringify({
                o0: {
                    doc_id: "3336396659757871",
                    query_params: {
                        limit: 1,
                        before: null,
                        tags: ["INBOX", "OTHER", "PENDING"],
                        includeDeliveryReceipts: false,
                        includeSeqID: true
                    }
                }
            })
        })
        .then(async function(res) {
            let body = res.body;
            
            // Decompress if needed
            if (Buffer.isBuffer(body)) {
                body = await decompressResponse(body);
                res.body = body;
            } else if (typeof body === 'string') {
                try {
                    JSON.parse(body);
                } catch (e) {
                    body = await decompressResponse(Buffer.from(body));
                    res.body = body;
                }
            }
            
            return res;
        })
        .then(utils.parseAndCheckLogin(ctx, defaultFuncs))
        .then(resData => {
            retryCount = 0;
            if (!Array.isArray(resData)) throw { error: "Not logged in", res: resData };
            if (resData[resData.length - 1].error_results > 0) throw resData[0].o0.errors;
            if (resData[resData.length - 1].successful_results === 0) throw { error: "getSeqId: no successful_results" };
            var threads = resData[0].o0.data && resData[0].o0.data.viewer && resData[0].o0.data.viewer.message_threads;
            if (threads && threads.sync_sequence_id) {
                ctx.lastSeqId = threads.sync_sequence_id;
                listenMqtt(defaultFuncs, api, ctx, globalCallback);
            } else {
                throw { error: "getSeqId: no sync_sequence_id found." };
            }
        })
        .catch(async err => {
            logger.error("getSeqID", err.error || err.message || err);
            
            if (err.error === "Not logged in") {
                ctx.loggedIn = false;
                return globalCallback(err, null);
            }
            
            // Check if this is a binary/gzip or JSON parse error — retry instead of crashing
            if (err.isBinaryResponse === true || 
                (err.res && Buffer.isBuffer(err.res)) || 
                err.error === "JSON.parse error" ||
                (err.message && err.message.includes("JSON.parse")) ||
                (err.detail && err.detail.message && err.detail.message.includes("JSON.parse"))) {
                if (err.res && typeof err.res === "string") {
                    logger.warn("getSeqID", `Response preview: ${err.res.substring(0, 200)}`);
                }
                retryCount++;
                if (retryCount <= maxRetries) {
                    logger.warn("getSeqID", `Parse error detected, retrying (${retryCount}/${maxRetries}) in 5 seconds...`);
                    await new Promise(resolve => setTimeout(resolve, 5000));
                    getSeqID();
                    return;
                } else {
                    logger.warn("getSeqID", `Failed after ${maxRetries} retries, will retry again in 30 seconds...`);
                    retryCount = 0;
                    await new Promise(resolve => setTimeout(resolve, 30000));
                    getSeqID();
                    return;
                }
            }
            
            return globalCallback(err, null);
        });
    };

    return function (callback) {
        class MessageEmitter extends EventEmitter {
            stopListening(cb) {
                cb = cb || (() => { });
                globalCallback = identity;
                if (ctx.mqttClient) {
                    ctx.mqttClient.unsubscribe("/webrtc");
                    ctx.mqttClient.unsubscribe("/rtc_multi");
                    ctx.mqttClient.unsubscribe("/onevc");
                    ctx.mqttClient.publish("/browser_close", "{}");
                    ctx.mqttClient.end(false, (...data) => { cb(data); ctx.mqttClient = undefined; });
                } else {
                    cb([]);
                }
            }
            stopListeningAsync() {
                return new Promise(res => this.stopListening(res));
            }
        }

        var msgEmitter = new MessageEmitter();
        globalCallback = callback || ((error, message) => {
            if (error) return msgEmitter.emit("error", error);
            msgEmitter.emit("message", message);
        });

        if (!ctx.firstListen) ctx.lastSeqId = null;
        ctx.syncToken = undefined;

        form = {
            av: ctx.globalOptions.pageID,
            queries: JSON.stringify({
                o0: {
                    doc_id: "3336396659757871",
                    query_params: {
                        limit: 1,
                        before: null,
                        tags: ["INBOX", "OTHER", "PENDING"],
                        includeDeliveryReceipts: false,
                        includeSeqID: true
                    }
                }
            })
        };

        if (!ctx.firstListen || !ctx.lastSeqId) {
            getSeqID();
        } else {
            listenMqtt(defaultFuncs, api, ctx, globalCallback);
        }

        ctx.firstListen = false;
        api.stopListening = msgEmitter.stopListening.bind(msgEmitter);
        api.stopListeningAsync = msgEmitter.stopListeningAsync.bind(msgEmitter);
        return msgEmitter;
    };
};
