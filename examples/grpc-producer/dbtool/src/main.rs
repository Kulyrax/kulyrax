//! `init <db>` creates the `users` table; `dump <db>` prints its rows as
//! `id|name`, ordered by id.

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
                "CREATE TABLE IF NOT EXISTS users (id TEXT PRIMARY KEY, name TEXT)",
                [],
            )
            .expect("create users table");
        }
        "dump" => {
            let mut stmt = conn
                .prepare("SELECT id, name FROM users ORDER BY id")
                .expect("prepare select");
            let rows = stmt
                .query_map([], |r| Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?)))
                .expect("query users");
            for row in rows {
                let (id, name) = row.expect("read row");
                println!("{id}|{name}");
            }
        }
        other => panic!("unknown command {other:?}; {usage}"),
    }
}
