require("dotenv").config();

const express = require("express");
const http = require("http");
const WebSocket = require("ws");
const path = require("path");
const fs = require("fs");
const os = require("os");
const bcrypt = require("bcryptjs");
const jwt = require("jsonwebtoken");
const cookieParser = require("cookie-parser");
const mongoose = require("mongoose");
const { Redis } = require("@upstash/redis");

const app = express();
const server = http.createServer(app);
const wss = new WebSocket.Server({ server });

const PORT = process.env.PORT || 3001;
const HOST = process.env.HOST || "0.0.0.0";
const JWT_SECRET = process.env.JWT_SECRET || "local_secret_key";
const DATA_FILE = path.join(__dirname, "local-data.json");
const MONGODB_URI = process.env.MONGODB_URI?.trim();
const MONGODB_DB_NAME = process.env.MONGODB_DB_NAME || "infinity-chat";
const UPSTASH_REDIS_REST_URL = process.env.UPSTASH_REDIS_REST_URL?.trim();
const UPSTASH_REDIS_REST_TOKEN = process.env.UPSTASH_REDIS_REST_TOKEN?.trim();

app.use(express.json({ limit: "10mb" }));
app.use(cookieParser());
app.use(express.static(path.join(__dirname, "public")));

const passthroughSchemaOptions = {
  strict: false,
  versionKey: false
};

const userSchema = new mongoose.Schema({
  _id: String,
  username: { type: String, required: true, unique: true, index: true },
  password: String,
  avatar: String,
  bio: String,
  createdAt: String
}, passthroughSchemaOptions);

const messageSchema = new mongoose.Schema({
  _id: String,
  type: String,
  roomId: { type: String, index: true },
  roomTitle: String,
  username: { type: String, index: true },
  avatar: String,
  message: String,
  timestamp: String
}, passthroughSchemaOptions);

const groupSchema = new mongoose.Schema({
  _id: String,
  name: String,
  admin: String,
  creator: String,
  members: [String],
  avatar: String,
  createdAt: String
}, passthroughSchemaOptions);

const User = mongoose.model("User", userSchema, "users");
const Message = mongoose.model("Message", messageSchema, "messages");
const Group = mongoose.model("Group", groupSchema, "groups");

let mongoEnabled = false;
let redisEnabled = false;
let redisStatusMessage = "Redis credentials are not configured.";
let redis = null;

function loadLocalData() {
  try {
    if (!fs.existsSync(DATA_FILE)) {
      fs.writeFileSync(DATA_FILE, JSON.stringify({ users: [], messages: [], groups: [] }, null, 2));
    }
    return JSON.parse(fs.readFileSync(DATA_FILE, "utf8"));
  } catch {
    return { users: [], messages: [], groups: [] };
  }
}

function saveLocalData(data) {
  fs.writeFileSync(DATA_FILE, JSON.stringify(data, null, 2));
}

async function connectMongo() {
  if (!MONGODB_URI) return false;

  await mongoose.connect(MONGODB_URI, {
    dbName: MONGODB_DB_NAME,
    serverSelectionTimeoutMS: 10000
  });

  await Promise.all([
    User.createCollection(),
    Message.createCollection(),
    Group.createCollection()
  ]);

  await Promise.all([
    User.syncIndexes(),
    Message.syncIndexes(),
    Group.syncIndexes()
  ]);

  mongoEnabled = true;
  return true;
}

async function connectRedis() {
  if (!UPSTASH_REDIS_REST_URL || !UPSTASH_REDIS_REST_TOKEN) {
    redisStatusMessage = "Redis credentials are not configured.";
    return false;
  }

  try {
    redis = new Redis({
      url: UPSTASH_REDIS_REST_URL,
      token: UPSTASH_REDIS_REST_TOKEN
    });

    const healthKey = "infinity-chat:redis-health";
    const healthValue = Date.now().toString();

    await redis.set(healthKey, healthValue, { ex: 60 });
    const savedValue = await redis.get(healthKey);

    if (String(savedValue) !== healthValue) {
      throw new Error("Redis health check returned an unexpected value.");
    }

    redisEnabled = true;
    redisStatusMessage = "Redis is connected.";
    return true;
  } catch (error) {
    redisEnabled = false;
    redisStatusMessage = error.message;
    return false;
  }
}

async function loadData() {
  if (!mongoEnabled) return loadLocalData();

  const [users, messages, groups] = await Promise.all([
    User.find({}).lean(),
    Message.find({}).sort({ timestamp: 1 }).lean(),
    Group.find({}).lean()
  ]);

  const localData = loadLocalData();
  if (!users.length && !messages.length && !groups.length && (
    localData.users.length || localData.messages.length || localData.groups.length
  )) {
    await saveData(localData);
    return localData;
  }

  return { users, messages, groups };
}

async function saveData(data) {
  if (!mongoEnabled) {
    saveLocalData(data);
    return;
  }

  const userWrites = data.users.map(user => ({
    replaceOne: {
      filter: { _id: user._id },
      replacement: user,
      upsert: true
    }
  }));

  const messageWrites = data.messages.map(message => ({
    replaceOne: {
      filter: { _id: message._id },
      replacement: message,
      upsert: true
    }
  }));

  const groupWrites = data.groups.map(group => ({
    replaceOne: {
      filter: { _id: group._id },
      replacement: group,
      upsert: true
    }
  }));

  await Promise.all([
    userWrites.length ? User.bulkWrite(userWrites, { ordered: false }) : Promise.resolve(),
    messageWrites.length ? Message.bulkWrite(messageWrites, { ordered: false }) : Promise.resolve(),
    groupWrites.length ? Group.bulkWrite(groupWrites, { ordered: false }) : Promise.resolve()
  ]);

  await Promise.all([
    deleteMongoDocuments(User, data.users.map(user => user._id)),
    deleteMongoDocuments(Message, data.messages.map(message => message._id)),
    deleteMongoDocuments(Group, data.groups.map(group => group._id))
  ]);
}

async function deleteMongoDocuments(collection, idsToKeep) {
  if (!mongoEnabled) return;
  await collection.deleteMany({ _id: { $nin: idsToKeep } });
}


let db = { users: [], messages: [], groups: [] };
const onlineUsers = new Map();

async function normalizeData() {
  const seenUsers = new Set();
  const uniqueUsers = [];

  db.users.forEach(user => {
    user.username = String(user.username || "").trim().toLowerCase();
    if (!user.username || seenUsers.has(user.username)) return;
    seenUsers.add(user.username);
    uniqueUsers.push(user);
  });

  db.users = uniqueUsers;
  db.groups = db.groups.map(group => ({
    ...group,
    admin: group.admin || group.creator,
    members: Array.from(new Set((group.members || [])
      .map(member => String(member).trim().toLowerCase())
      .filter(member => seenUsers.has(member))))
  })).filter(group => group.members.length > 0);

  db.messages = db.messages.filter(message => seenUsers.has(message.username));
  await saveData(db);
}

function createToken(user) {
  return jwt.sign(
    { id: user._id, username: user.username, avatar: user.avatar, bio: user.bio || "" },
    JWT_SECRET,
    { expiresIn: "7d" }
  );
}

function auth(req, res, next) {
  let token = req.cookies.token;

  if (!token && req.headers.authorization?.startsWith("Bearer ")) {
    token = req.headers.authorization.split(" ")[1];
  }

  if (!token) return res.status(401).json({ error: "No token provided." });

  try {
    req.user = jwt.verify(token, JWT_SECRET);
    next();
  } catch {
    res.status(403).json({ error: "Invalid token." });
  }
}

function asyncHandler(handler) {
  return (req, res, next) => Promise.resolve(handler(req, res, next)).catch(next);
}

function getLanUrls(port) {
  return Object.values(os.networkInterfaces())
    .flat()
    .filter(item => item && item.family === "IPv4" && !item.internal)
    .map(item => `http://${item.address}:${port}`);
}

function broadcast(data) {
  const message = JSON.stringify(data);
  wss.clients.forEach(client => {
    if (client.readyState === WebSocket.OPEN) {
      client.send(message);
    }
  });
}

function sendToUsers(usernames, data) {
  const message = JSON.stringify(data);
  usernames.forEach(username => {
    const sockets = onlineUsers.get(username);
    if (!sockets) return;

    sockets.forEach(client => {
      if (client.readyState === WebSocket.OPEN) {
        client.send(message);
      }
    });
  });
}

function getUserPublic(user) {
  return {
    username: user.username,
    avatar: user.avatar,
    bio: user.bio || "",
    online: onlineUsers.has(user.username)
  };
}

function normalizeMembers(members) {
  if (!Array.isArray(members)) return [];
  return members
    .map(member => String(member).trim().toLowerCase())
    .filter(Boolean);
}

function privateRoomId(userA, userB) {
  return ["dm", ...[userA, userB].sort()].join(":");
}

function getRoomAccess(roomId, username) {
  if (roomId === "lounge") {
    return { ok: true, members: db.users.map(user => user.username), title: "Lounge Chat" };
  }

  if (roomId?.startsWith("dm:")) {
    const members = roomId.split(":").slice(1);
    return {
      ok: members.length === 2 && members.includes(username),
      members,
      title: members.find(member => member !== username) || "Direct Message"
    };
  }

  const group = db.groups.find(item => item._id === roomId);
  if (!group) return { ok: false, members: [] };

  return {
    ok: group.members.includes(username),
    members: group.members,
    title: group.name,
    group
  };
}

function getOnlineList() {
  return Array.from(onlineUsers.keys()).map(username => {
    const user = db.users.find(u => u.username === username);
    return {
      username,
      avatar: user?.avatar || "",
      bio: user?.bio || ""
    };
  });
}

app.post("/api/auth/signup", async (req, res) => {
  try {
    const { username, password, avatar } = req.body;

    if (!username || !password) {
      return res.status(400).json({ error: "Username and password are required." });
    }

    const cleanUsername = username.trim().toLowerCase();

    if (db.users.find(u => u.username === cleanUsername)) {
      return res.status(400).json({ error: "Username already exists." });
    }

    const hashedPassword = await bcrypt.hash(password, 10);

    const user = {
      _id: "u_" + Date.now(),
      username: cleanUsername,
      password: hashedPassword,
      avatar: avatar || `https://api.dicebear.com/9.x/bottts/svg?seed=${cleanUsername}`,
      bio: "Infinity Chat user",
      createdAt: new Date().toISOString()
    };

    db.users.push(user);
    await saveData(db);

    const token = createToken(user);

    res.cookie("token", token, {
      httpOnly: true,
      maxAge: 7 * 24 * 60 * 60 * 1000
    });

    res.json({
      success: true,
      token,
      user: {
        username: user.username,
        avatar: user.avatar,
        bio: user.bio
      }
    });
  } catch {
    res.status(500).json({ error: "Signup failed." });
  }
});

app.post("/api/auth/login", async (req, res) => {
  try {
    const { username, password } = req.body;
    const cleanUsername = username.trim().toLowerCase();

    const user = db.users.find(u => u.username === cleanUsername);
    if (!user) return res.status(400).json({ error: "Invalid username or password." });

    const match = await bcrypt.compare(password, user.password);
    if (!match) return res.status(400).json({ error: "Invalid username or password." });

    const token = createToken(user);

    res.cookie("token", token, {
      httpOnly: true,
      maxAge: 7 * 24 * 60 * 60 * 1000
    });

    res.json({
      success: true,
      token,
      user: {
        username: user.username,
        avatar: user.avatar,
        bio: user.bio
      }
    });
  } catch {
    res.status(500).json({ error: "Login failed." });
  }
});

app.get("/api/auth/me", auth, (req, res) => {
  const user = db.users.find(u => u.username === req.user.username);
  if (!user) return res.status(404).json({ error: "User not found." });

  res.json({
    success: true,
    user: {
      username: user.username,
      avatar: user.avatar,
      bio: user.bio
    }
  });
});

app.post("/api/auth/logout", (req, res) => {
  res.clearCookie("token");
  res.json({ success: true });
});

app.get("/api/security/status", (req, res) => {
  res.json({
    success: true,
    secureChat: mongoEnabled && redisEnabled,
    mongodb: mongoEnabled,
    redis: redisEnabled,
    message: mongoEnabled && redisEnabled
      ? "Secure Chat Active"
      : "Secure Chat is waiting for database services."
  });
});

app.get("/api/users", auth, (req, res) => {
  const users = db.users.map(getUserPublic);

  res.json({ success: true, users });
});

app.put("/api/profile", auth, asyncHandler(async (req, res) => {
  const { avatar, bio } = req.body;
  const user = db.users.find(u => u.username === req.user.username);

  if (!user) return res.status(404).json({ error: "User not found." });

  if (avatar) user.avatar = avatar;
  user.bio = bio || "";

  await saveData(db);

  res.json({
    success: true,
    user: {
      username: user.username,
      avatar: user.avatar,
      bio: user.bio
    }
  });
}));

app.get("/api/messages/:roomId", auth, (req, res) => {
  const roomId = req.params.roomId || "lounge";
  const access = getRoomAccess(roomId, req.user.username);

  if (!access.ok) {
    return res.status(403).json({ error: "You do not have access to this chat." });
  }

  const messages = db.messages
    .filter(m => m.roomId === roomId)
    .slice(-50);

  res.json({ success: true, messages });
});

app.post("/api/groups", auth, asyncHandler(async (req, res) => {
  const { name, members } = req.body;

  if (!name || !name.trim()) {
    return res.status(400).json({ error: "Group name is required." });
  }

  const requestedMembers = normalizeMembers(members);
  const validMembers = requestedMembers.filter(member =>
    member !== req.user.username && db.users.some(user => user.username === member)
  );

  const group = {
    _id: "g_" + Date.now(),
    name: name.trim(),
    admin: req.user.username,
    creator: req.user.username,
    members: Array.from(new Set([req.user.username, ...validMembers])),
    avatar: `https://api.dicebear.com/9.x/initials/svg?seed=${encodeURIComponent(name)}`,
    createdAt: new Date().toISOString()
  };

  db.groups.push(group);
  await saveData(db);

  sendToUsers(group.members, { type: "groups_updated" });

  res.json({ success: true, group });
}));

app.get("/api/groups", auth, (req, res) => {
  const groups = db.groups.filter(g => g.members.includes(req.user.username));
  res.json({ success: true, groups });
});

app.delete("/api/groups/:groupId", auth, asyncHandler(async (req, res) => {
  const group = db.groups.find(g => g._id === req.params.groupId);

  if (!group) return res.status(404).json({ error: "Group not found." });
  if ((group.admin || group.creator) !== req.user.username) {
    return res.status(403).json({ error: "Only the group admin can delete this group." });
  }

  const members = [...group.members];
  db.groups = db.groups.filter(g => g._id !== group._id);
  db.messages = db.messages.filter(message => message.roomId !== group._id);
  await saveData(db);

  sendToUsers(members, { type: "group_deleted", roomId: group._id });
  res.json({ success: true });
}));

app.delete("/api/groups/:groupId/members/:username", auth, asyncHandler(async (req, res) => {
  const group = db.groups.find(g => g._id === req.params.groupId);
  const member = req.params.username.trim().toLowerCase();

  if (!group) return res.status(404).json({ error: "Group not found." });
  if ((group.admin || group.creator) !== req.user.username) {
    return res.status(403).json({ error: "Only the group admin can remove members." });
  }
  if (member === req.user.username) {
    return res.status(400).json({ error: "The admin cannot remove themselves." });
  }

  group.members = group.members.filter(username => username !== member);
  await saveData(db);

  sendToUsers([...group.members, member], { type: "groups_updated", roomId: group._id });
  res.json({ success: true, group });
}));

wss.on("connection", (ws, req) => {
  const params = new URLSearchParams(req.url.split("?")[1]);
  const token = params.get("token");

  let user;

  try {
    user = jwt.verify(token, JWT_SECRET);
  } catch {
    ws.close();
    return;
  }

  ws.username = user.username;

  if (!onlineUsers.has(user.username)) {
    onlineUsers.set(user.username, new Set());
  }

  onlineUsers.get(user.username).add(ws);

  broadcast({
    type: "online_list",
    users: getOnlineList()
  });

  ws.send(JSON.stringify({
    type: "history",
    roomId: "lounge",
    messages: db.messages.filter(m => m.roomId === "lounge").slice(-50)
  }));

  ws.on("message", async raw => {
    try {
      const data = JSON.parse(raw.toString());

      if (!data.message || !data.message.trim()) return;

      const currentUser = db.users.find(u => u.username === user.username);
      const roomId = data.roomId || "lounge";
      const access = getRoomAccess(roomId, user.username);

      if (!access.ok) return;

      const msg = {
        _id: "m_" + Date.now(),
        type: "chat",
        roomId,
        roomTitle: access.title,
        username: user.username,
        avatar: currentUser?.avatar || user.avatar,
        message: data.message.trim(),
        timestamp: new Date().toISOString()
      };

      db.messages.push(msg);
      await saveData(db);

      sendToUsers(access.members, msg);
    } catch (error) {
      console.log("Message failed:", error.message);
    }
  });

  ws.on("close", () => {
    const set = onlineUsers.get(user.username);

    if (set) {
      set.delete(ws);

      if (set.size === 0) {
        onlineUsers.delete(user.username);
      }
    }

    broadcast({
      type: "online_list",
      users: getOnlineList()
    });
  });
});

async function startServer() {
  const connectedToMongo = await connectMongo();
  const connectedToRedis = await connectRedis();
  db = await loadData();
  await normalizeData();

  server.listen(PORT, HOST, () => {
    if (connectedToMongo) {
      console.log(`[MONGODB] Connected to database "${MONGODB_DB_NAME}".`);
      console.log(`[MONGODB] Collections ready: ${MONGODB_DB_NAME}.users, ${MONGODB_DB_NAME}.messages, ${MONGODB_DB_NAME}.groups`);
    } else {
      console.log("[LOCAL DB] Loaded local-data.json");
      console.log("[SYSTEM] MONGODB_URI is not defined. Falling back to LOCAL JSON DATABASE.");
    }

    if (connectedToRedis) {
      console.log("[UPSTASH] Redis connected and health check passed.");
      console.log("[SECURITY] Secure Chat successfully connected.");
    } else if (UPSTASH_REDIS_REST_URL && UPSTASH_REDIS_REST_TOKEN) {
      console.log(`[UPSTASH] Redis health check failed: ${redisStatusMessage}`);
    } else {
      console.log("[SYSTEM] Redis credentials not found. Using local Redis mock.");
    }

    console.log(`[SYSTEM] Server listening locally on http://localhost:${PORT}`);
    getLanUrls(PORT).forEach(url => {
      console.log(`[MOBILE] Open on phone using same Wi-Fi: ${url}`);
    });
  });
}

startServer().catch(error => {
  console.error("[SYSTEM] Server failed to start:", error.message);
  process.exit(1);
});
