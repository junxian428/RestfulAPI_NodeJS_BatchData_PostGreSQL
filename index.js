const express = require('express');
const { Pool } = require('pg');
const Cursor = require('pg-cursor');
const app = express();

const pool = new Pool({
  user: 'youruser',
  host: 'localhost',
  database: 'yourdatabase',
  password: 'yourpassword',
  port: 5432,
});

app.get('/stream-cursor', async (req, res) => {
  const client = await pool.connect();

  try {
    await client.query('BEGIN'); // Start a transaction
    const cursor = client.query(new Cursor('SELECT * FROM large_table'));

    res.setHeader('Content-Type', 'application/json');
    res.write('['); // Open JSON array
    let firstRow = true;

    const fetchNextBatch = () => {
      cursor.read(1000, (err, rows) => {
        if (err) {
          console.error('Cursor error:', err);
          res.status(500).end();
          cursor.close(() => client.release());
          return;
        }

        if (rows.length === 0) {
          // Close JSON array and end response when no more rows
          res.write(']');
          res.end();
          cursor.close(() => client.query('COMMIT', () => client.release()));
          return;
        }

        // Stream rows as JSON, adding commas between rows
        rows.forEach((row) => {
          const json = JSON.stringify(row);
          res.write((firstRow ? '' : ',') + json);
          firstRow = false;
        });

        // Fetch the next batch
        fetchNextBatch();
      });
    };

    fetchNextBatch(); // Start fetching batches
  } catch (err) {
    console.error('Error:', err);
    res.status(500).end();
    client.release();
  }
});

const PORT = 3000;
app.listen(PORT, () => console.log(`Server running on http://localhost:${PORT}`));
