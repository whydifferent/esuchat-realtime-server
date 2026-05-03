require("dotenv").config();

const express = require("express");
const http = require("http");
const cors = require("cors");
const mysql = require("mysql2/promise");
const { Server } = require("socket.io");

const app = express();

const allowedOrigins = [
  "https://purple-nightingale-405503.hostingersite.com"
];

app.use(cors({
  origin: allowedOrigins,
  methods: ["GET", "POST"]
}));

const server = http.createServer(app);

const io = new Server(server, {
  transports: ["websocket", "polling"],
  cors: {
    origin: allowedOrigins,
    methods: ["GET", "POST"]
  }
});

let db;

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

function privateRoom(a, b) {
  const first = Math.min(Number(a), Number(b));
  const second = Math.max(Number(a), Number(b));

  return `private_${first}_${second}`;
}

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

      console.log("User online:", userId);
    } catch (err) {
      console.error("Presence update error:", err.message);
    }
  }

  socket.on("join_group", (data) => {
    if (!data || !data.group_id) return;

    const room = "group_" + data.group_id;
    socket.join(room);

    console.log("Joined group room:", room);
  });

  socket.on("group_message", (message) => {
    if (!message || !message.group_id) return;

    const room = "group_" + message.group_id;
    io.to(room).emit("group_message", message);

    console.log("Group message sent to:", room);
  });

  socket.on("group_typing", (data) => {
    if (!data || !data.group_id) return;

    const room = "group_" + data.group_id;
    socket.to(room).emit("group_typing", data);
  });

  socket.on("group_message_deleted", (data) => {
    if (!data || !data.group_id || !data.message_id) return;

    const room = "group_" + data.group_id;

    io.to(room).emit("group_message_deleted", {
      message_id: data.message_id
    });

    console.log("Group message deleted in:", room, "Message:", data.message_id);
  });

  socket.on("join_private", (data) => {
    if (!data || !data.room) return;

    const room = "private_" + data.room;
    socket.join(room);

    console.log("Joined private room:", room);
  });

  socket.on("private_message", (message) => {
    if (!message || !message.sender_id || !message.receiver_id) {
      console.log("Invalid private message payload:", message);
      return;
    }

    const room = privateRoom(message.sender_id, message.receiver_id);

    io.to(room).emit("private_message", message);

    console.log("Private message sent to:", room);
  });

  socket.on("private_message_deleted", (data) => {
    if (!data || !data.room || !data.message_id) return;

    const room = "private_" + data.room;

    io.to(room).emit("private_message_deleted", {
      message_id: data.message_id
    });

    console.log("Private message deleted in:", room, "Message:", data.message_id);
  });

  socket.on("private_typing", (data) => {
    if (!data || !data.room) return;

    const room = "private_" + data.room;
    socket.to(room).emit("private_typing", data);
  });

  socket.on("disconnect", async () => {
    console.log("Disconnected:", socket.id, "User:", userId);

    if (userId) {
      try {
        const [result] = await db.query(
          "UPDATE users SET is_online = 0, socket_id = NULL, last_seen = NOW() WHERE id = ? AND socket_id = ?",
          [userId, socket.id]
        );

        if (result.affectedRows > 0) {
          io.emit("presence_update", {
            user_id: userId,
            name,
            is_online: false,
            online_count: await onlineCount()
          });

          console.log("User offline:", userId);
        } else {
          console.log("Old socket disconnected, user still online:", userId);
        }
      } catch (err) {
        console.error("Disconnect presence error:", err.message);
      }
    }
  });
});

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
    console.log("2go realtime server running on port " + port);
  });
})().catch((err) => {
  console.error("Server startup error:", err);
  process.exit(1);
});
