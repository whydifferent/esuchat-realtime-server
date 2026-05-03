require("dotenv").config();

const express = require("express");
const http = require("http");
const cors = require("cors");
const mysql = require("mysql2/promise");
const { Server } = require("socket.io");

const app = express();

/* 🔒 Restrict CORS (replace with your real domain) */
app.use(cors({
  origin: [
    "https://purple-nightingale-405503.hostingersite.com"
  ],
  methods: ["GET", "POST"]
}));

const server = http.createServer(app);

const io = new Server(server, {
  cors: {
    origin: [
      "https://purple-nightingale-405503.hostingersite.com"
    ],
    methods: ["GET", "POST"]
  }
});

let db;

/* ✅ ONLINE COUNT */
async function onlineCount() {
  try {
    const [rows] = await db.query(
      "SELECT COUNT(*) AS c FROM users WHERE is_online = 1"
    );
    return rows[0].c;
  } catch (err) {
    console.error("Online count error:", err.message);
    return 0;
  }
}

/* ✅ PRIVATE ROOM */
function privateRoom(a, b) {
  const first = Math.min(Number(a), Number(b));
  const second = Math.max(Number(a), Number(b));
  return `private_${first}_${second}`;
}

/* 🔥 SOCKET CONNECTION */
io.on("connection", async (socket) => {

  const userId = parseInt(socket.handshake.query.user_id || "0", 10);
  const name = socket.handshake.query.name || "user";

  console.log("Connected:", socket.id, "User:", userId);

  if (userId) {
    try {
      await db.query(
        "UPDATE users SET is_online = 1, socket_id = ?, last_seen = NOW() WHERE id = ?",
        [socket.id, userId]
      );

      io.emit("presence_update", {
        user_id: userId,
        name,
        is_online: true,
        online_count: await onlineCount()
      });

    } catch (err) {
      console.error("Presence update error:", err.message);
    }
  }

  /* GROUP EVENTS */
  socket.on("join_group", (data) => {
    if (!data?.group_id) return;
    socket.join("group_" + data.group_id);
  });

  socket.on("group_message", (msg) => {
    if (!msg?.group_id) return;
    io.to("group_" + msg.group_id).emit("group_message", msg);
  });

  socket.on("group_typing", (data) => {
    if (!data?.group_id) return;
    socket.to("group_" + data.group_id).emit("group_typing", data);
  });

  socket.on("group_message_deleted", (data) => {
    if (!data?.group_id || !data?.message_id) return;
    io.to("group_" + data.group_id).emit("group_message_deleted", {
      message_id: data.message_id
    });
  });

  /* PRIVATE EVENTS */
  socket.on("join_private", (data) => {
    if (!data?.room) return;
    socket.join("private_" + data.room);
  });

  socket.on("private_message", (msg) => {
    if (!msg?.sender_id || !msg?.receiver_id) return;

    const room = privateRoom(msg.sender_id, msg.receiver_id);
    io.to(room).emit("private_message", msg);
  });

  socket.on("private_message_deleted", (data) => {
    if (!data?.room || !data?.message_id) return;

    io.to("private_" + data.room).emit("private_message_deleted", {
      message_id: data.message_id
    });
  });

  socket.on("private_typing", (data) => {
    if (!data?.room) return;
    socket.to("private_" + data.room).emit("private_typing", data);
  });

  /* DISCONNECT */
  socket.on("disconnect", async () => {
    console.log("Disconnected:", socket.id, "User:", userId);

    if (userId) {
      try {
        await db.query(
          "UPDATE users SET is_online = 0, socket_id = NULL, last_seen = NOW() WHERE id = ?",
          [userId]
        );

        io.emit("presence_update", {
          user_id: userId,
          name,
          is_online: false,
          online_count: await onlineCount()
        });

      } catch (err) {
        console.error("Disconnect error:", err.message);
      }
    }
  });
});

/* 🔥 DATABASE + SERVER START */
(async () => {
  db = await mysql.createPool({
    host: process.env.DB_HOST,
    user: process.env.DB_USER,
    password: process.env.DB_PASSWORD,
    database: process.env.DB_NAME,
    waitForConnections: true,
    connectionLimit: 10,
    queueLimit: 0
  });

  const port = process.env.PORT || 3000;

  server.listen(port, "0.0.0.0", () => {
    console.log("Realtime server running on port " + port);
  });

})().catch((err) => {
  console.error("Startup error:", err);
  process.exit(1);
});
