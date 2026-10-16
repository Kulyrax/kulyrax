//! `init <db>` creates the `users` table; `dump <db>` prints its rows as
//! `id|name|email`, one per line, ordered by id — just enough to let
//! `run.sh` set up and verify the example without a `sqlite3` CLI installed.

use std::env;

fn main() {
    let args: Vec<String> = env::args().collect();
    let usage = "usage: dbtool <init|dump> <db-path>";
    let cmd = args.get(1).expect(usage);
    let db_path = args.get(2).expect(usage);

    let conn = rusqlite::Connection::open(db_path).expect("open sqlite db");

    match cmd.as_str() {
        "init" => {
            conn.execute(
                "CREATE TABLE IF NOT EXISTS users (id TEXT PRIMARY KEY, name TEXT, email TEXT)",
                [],
            )
            .expect("create users table");
        }
        "dump" => {
            let mut stmt = conn
                .prepare("SELECT id, name, email FROM users ORDER BY id")
                .expect("prepare select");
            let rows = stmt
                .query_map([], |r| {
                    Ok((
                        r.get::<_, String>(0)?,
                        r.get::<_, String>(1)?,
                        r.get::<_, String>(2)?,
                    ))
                })
                .expect("query users");
            for row in rows {
                let (id, name, email) = row.expect("read row");
                println!("{id}|{name}|{email}");
            }
        }
        other => panic!("unknown command {other:?}; {usage}"),
    }
}
