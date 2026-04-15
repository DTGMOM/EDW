const express = require("express");
const mysql = require("mysql2/promise");
const cors = require("cors");
const multer = require("multer");
const path = require("path");
const fs = require("fs"); // ✅ FIXED

// Adding trial comment
const app = express();

app.use(cors());
app.use(express.json());

// ================= DB =================
const db = mysql.createPool({
  host: "localhost",
  user: "root",
  password: "root",
  database: "university_db"
});

app.use(cors());

console.log("✅ DB Connected");

// ================= AUTH =================

// REGISTER
app.post("/api/register", async (req, res) => {
  try {
    const { username, password, role, designation, address, university } = req.body;

    await db.query(
      `INSERT INTO users 
      (username, password, role, designation, address, university) 
      VALUES (?, ?, ?, ?, ?, ?)`,
      [username, password, role, designation, address, university]
    );

    res.send("Registered Successfully");
  } catch (err) {
    res.status(500).send(err);
  }
});

// LOGIN
app.post("/api/login", async (req, res) => {
  try {
    const { username, password } = req.body;

    const [rows] = await db.query(
      "SELECT * FROM users WHERE username=? AND password=?",
      [username, password]
    );

    if (rows.length === 0) {
      return res.status(401).json({ message: "Invalid login" });
    }

    const user = rows[0];

    // ✅ ADD HERE
    res.json({
      id: user.id,
      username: user.username,
      role: user.role,
      permissions: user.permissions
        ? JSON.parse(user.permissions)
        : []
    });
console.log("Permissions from DB:", user.permissions);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// ================= USERS =================

// GET USERS
app.get("/api/users", async (req, res) => {
  try {
    const [result] = await db.query("SELECT * FROM users");
    res.json(result);
  } catch (err) {
    res.status(500).send(err);
  }
});

// DELETE USER
app.delete("/api/users/:id", async (req, res) => {
  try {
    await db.query("DELETE FROM users WHERE id=?", [req.params.id]);
    res.send("Deleted");
  } catch (err) {
    res.status(500).send(err);
  }
});

// UPDATE USER
app.put("/api/users/:id", async (req, res) => {
  const { username, role, designation, university, address } = req.body;

  await db.query(
    `UPDATE users 
     SET username=?, role=?, designation=?, university=?, address=? 
     WHERE id=?`,
    [username, role, designation, university, address, req.params.id]
  );

  res.send("Updated");
});

// ================= EVENTS =================

// ADD EVENT
app.post("/api/events", async (req, res) => {
  try {
    const { title, description, event_date } = req.body;

    await db.query(
      "INSERT INTO events (title,description,event_date) VALUES (?,?,?)",
      [title, description, event_date]
    );

    res.send("Event Added");
  } catch (err) {
    res.status(500).send(err);
  }
});

// GET EVENTS
app.get("/api/events", async (req, res) => {
  try {
    const [rows] = await db.query(
      "SELECT id, title, description, DATE(event_date) as event_date FROM events ORDER BY id DESC"
    );

    res.json(rows);
  } catch (err) {
    res.status(500).send(err);
  }
});

app.put("/api/events/:id", async (req, res) => {
  try {
    const { title, description, event_date } = req.body;

    await db.query(
      "UPDATE events SET title=?, description=?, event_date=? WHERE id=?",
      [title, description, event_date, req.params.id]
    );

    res.send("Event Updated");
  } catch (err) {
    res.status(500).send(err);
  }
});

app.delete("/api/events/:id", async (req, res) => {
  try {
    await db.query("DELETE FROM events WHERE id=?", [req.params.id]);
    res.send("Deleted");
  } catch (err) {
    res.status(500).send(err);
  }
});


// ================= FILE UPLOAD =================

const storage = multer.diskStorage({
  destination: (req, file, cb) => {
    cb(null, "./uploads");
  },
  filename: (req, file, cb) => {
    cb(null, Date.now() + "-" + file.originalname);
  }
});

const upload = multer({ storage });

app.use("/uploads", express.static("uploads"));

// UPLOAD
app.post("/api/upload", upload.single("file"), (req, res) => {
  try {
    if (!req.file) return res.status(400).send("No file");

    res.send({
      fileName: req.file.originalname,
      filePath: `uploads/${req.file.filename}`,
      fileType: req.file.mimetype
    });
  } catch (err) {
    res.status(500).send("Upload failed");
  }
});

// ================= PAPERS =================

// SAVE PAPER
app.post("/api/papers", async (req, res) => {
  try {
    const { title, fileName, filePath, fileType, user_id } = req.body;

    const now = new Date();

    await db.query(
      `INSERT INTO papers 
      (title, file_name, file_path, file_type, user_id, status, upload_date, modified_date) 
      VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
      [title, fileName, filePath, fileType, user_id, "Pending", now, now]
    );

    res.send("Saved");
  } catch (err) {
    res.status(500).send(err);
  }
});

// UPDATE PAPER
app.put("/api/papers/:id", async (req, res) => {

  try {
    const { title, status } = req.body;
    const id = req.params.id;

    let fields = [];
    let values = [];

    if (title) {
      fields.push("title=?");
      values.push(title);
    }

    if (status) {
      fields.push("status=?");
      values.push(status);
    }

    values.push(id);

    const sql = `UPDATE papers SET ${fields.join(", ")} WHERE id=?`;

    await db.query(sql, values);

    res.send("Updated successfully");
  } catch (err) {
    res.status(500).send(err);
  }
});

// GET PAPERS
app.get("/api/papers", async (req, res) => {
  const { status } = req.query;

  let query = `
    SELECT 
      id,
      title,
      file_name,
      file_path,
      upload_date,
      modified_date,
      status
    FROM papers
  `;

  let params = [];

  if (status) {
    query += " WHERE status=?";
    params.push(status);
  }

  query += " ORDER BY id DESC";

  const [rows] = await db.query(query, params);
  res.json(rows);
});

// DELETE PAPER
app.delete("/api/papers/:id", async (req, res) => {
  try {
    const id = req.params.id;

    const [result] = await db.query(
      "SELECT file_path FROM papers WHERE id=?",
      [id]
    );

    const filePath = result[0]?.file_path;

    await db.query("DELETE FROM papers WHERE id=?", [id]);

    if (filePath) {
      const fullPath = path.join(__dirname, filePath);
      fs.unlink(fullPath, () => {});
    }

    res.send("Deleted fully");
  } catch (err) {
    res.status(500).send(err);
  }
});

// ================= DASHBOARD =================

app.get("/api/dashboard-stats", async (req, res) => {
  try {
    const [users] = await db.query("SELECT COUNT(*) AS count FROM users");
    const [events] = await db.query("SELECT COUNT(*) AS count FROM events");
    const [papers] = await db.query("SELECT COUNT(*) AS count FROM papers");
    const [workshops] = await db.query("SELECT COUNT(*) AS count FROM workshops");
    const [news] = await db.query("SELECT COUNT(*) AS count FROM news");

    res.json({
      users: users[0].count,
      events: events[0].count,
      papers: papers[0].count,
      workshops: workshops[0].count,
      news: news[0].count
    });
  } catch (err) {
    res.status(500).json(err);
  }
});

// ================= FORGOT PASSWORD =================

app.post("/api/forgot", async (req, res) => {
  try {
    const { username } = req.body;

    const [result] = await db.query(
      "SELECT * FROM users WHERE username=?",
      [username]
    );

    if (result.length === 0) {
      return res.status(404).send("User not found");
    }

    await db.query(
      "UPDATE users SET password=? WHERE username=?",
      ["123456", username]
    );

    res.send("Password reset to 123456");
  } catch (err) {
    res.status(500).send(err);
  }
});

// ================= USER PROFILE =================

app.get("/api/user/:username", async (req, res) => {
  try {
    const { username } = req.params;

    const [rows] = await db.query(
      "SELECT id, username, role, designation, address, university FROM users WHERE username=?",
      [username]
    );

    if (rows.length === 0) {
      return res.status(404).send("User not found");
    }

    res.json(rows[0]);
  } catch (err) {
    res.status(500).send(err);
  }
});

// GET ONLY PENDING
// app.get("/api/papers/pending", (req, res) => {
//   db.query(
//     "SELECT * FROM papers WHERE status='Pending' OR status IS NULL",
//     (err, result) => {
//       if (err) return res.status(500).send(err);
//       res.send(result);
//     }
//   );
// });

// ================== GET PENDING PAPERS ==================
app.get("/api/papers/pending", async (req, res) => {
  try {
    const [rows] = await db.query(
      "SELECT * FROM papers WHERE status='Pending' OR status IS NULL"
    );

    res.json(rows);
  } catch (err) {
    console.error("FETCH ERROR:", err);
    res.status(500).json({ error: err.message });
  }
});

// ================== UPDATE STATUS ==================
app.put("/api/papers/:id", async (req, res) => {
  try {
    const { status } = req.body;
    const { id } = req.params;

    await db.query("UPDATE papers SET status=? WHERE id=?", [
      status,
      id,
    ]);

    res.json({ message: "Status updated successfully" });
  } catch (err) {
    console.error("UPDATE ERROR:", err);
    res.status(500).json({ error: err.message });
  }
});
// ================= CMS =================
app.post("/api/cms", async (req, res) => {
  const { title, slug, content, status } = req.body;

  await db.query(
    "INSERT INTO cms_pages (title, slug, content, status) VALUES (?, ?, ?, ?)",
    [title, slug, content, status]
  );

  res.send("Page created");
});

app.get("/api/cms", async (req, res) => {
  const [rows] = await db.query("SELECT * FROM cms_pages");
  res.json(rows);
});

app.put("/api/cms/:id", async (req, res) => {
  const { title, slug, content, status } = req.body;

  await db.query(
    "UPDATE cms_pages SET title=?, slug=?, content=?, status=? WHERE id=?",
    [title, slug, content, status, req.params.id]
  );

  res.send("Updated");
});

app.delete("/api/cms/:id", async (req, res) => {
  await db.query("DELETE FROM cms_pages WHERE id=?", [req.params.id]);
  res.send("Deleted");
});

app.get("/api/cms/:slug", async (req, res) => {
  const [rows] = await db.query(
    "SELECT * FROM cms WHERE slug=? AND status='Published'",
    [req.params.slug]
  );

  if (rows.length === 0) {
    return res.status(404).send("Page not found");
  }

  res.json(rows[0]);
});


// ================= START =================

app.put("/api/roles/:id", async (req, res) => {
  try {
    const { role, permissions } = req.body;

    await db.query(
      "UPDATE users SET role=?, permissions=? WHERE id=?",
      [role, JSON.stringify(permissions), req.params.id]
    );

    res.send("Role & Permissions Updated");
  } catch (err) {
    res.status(500).send(err);
  }
});

// ================= workshops =================

app.post("/api/workshops", async (req, res) => {
  const { title, description, trainer, location, workshop_date, status } = req.body;

  await db.query(
    "INSERT INTO workshops (title, description, trainer, location, workshop_date, status) VALUES (?, ?, ?, ?, ?, ?)",
    [title, description, trainer, location, workshop_date, status]
  );

  res.send("Workshop Added");
});

app.get("/api/workshops", async (req, res) => {
  const [rows] = await db.query("SELECT * FROM workshops ORDER BY id DESC");
  res.json(rows);
});

app.delete("/api/workshops/:id", async (req, res) => {
  await db.query("DELETE FROM workshops WHERE id=?", [req.params.id]);
  res.send("Deleted");
});

app.put("/api/workshops/:id", async (req, res) => {
  const { title, description, trainer, location, workshop_date, status } = req.body;

  await db.query(
    "UPDATE workshops SET title=?, description=?, trainer=?, location=?, workshop_date=?, status=? WHERE id=?",
    [title, description, trainer, location, workshop_date, status, req.params.id]
  );

  res.send("Updated");
});


// ================= News =================

const newstorage = multer.diskStorage({
  destination: "uploads/",
  filename: (req, file, cb) => {
    cb(null, Date.now() + "_" + file.originalname);
  }
});

const newupload = multer({ storage: newstorage });

app.post("/api/news", newupload.single("image"), async (req, res) => {
  try {
    const { title, content, news_date, status, publish_at, meta_title, meta_description } = req.body;

    const image = req.file ? req.file.filename : null;

    await db.query(
      "INSERT INTO news (title, content, image, news_date, status, publish_at, meta_title, meta_description) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
      [title, content, image, news_date, status, publish_at, meta_title, meta_description]
    );

    res.send("News Added");
  } catch (err) {
    console.log("🔥 ERROR:", err);
    res.status(500).send(err);
  }
});


app.get("/api/news", async (req, res) => {
  const [rows] = await db.query("SELECT * FROM news ORDER BY id DESC");
  res.json(rows);
});


app.put("/api/news/:id", async (req, res) => {
  const { title, content, news_date, status } = req.body;

  await db.query(
    "UPDATE news SET title=?, content=?, news_date=?, status=? WHERE id=?",
    [title, content, news_date, status, req.params.id]
  );

  res.send("Updated");
});

app.delete("/api/news/:id", async (req, res) => {
  await db.query("DELETE FROM news WHERE id=?", [req.params.id]);
  res.send("Deleted");
});

app.listen(5000, () => {
  console.log("🚀 Backend running on http://localhost:5000");
});