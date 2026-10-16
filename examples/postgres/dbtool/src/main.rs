//! `init <url>` (re)creates the `users` table; `dump <url>` prints its rows as
//! `id|name|bio|followers`, one per line, ordered by id — just enough to let
//! `run.sh` set up and verify the example without a `psql` CLI installed.

use std::env;

fn main() {
    let args: Vec<String> = env::args().collect();
    let usage = "usage: dbtool <init|dump> <postgres-url>";
    let cmd = args.get(1).expect(usage);
    let url = args.get(2).expect(usage);

    let mut client = postgres::Client::connect(url, postgres::NoTls).expect("connect to postgres");

    match cmd.as_str() {
        "init" => {
            client
                .batch_execute(
                    "DROP TABLE IF EXISTS users;
                     DROP TABLE IF EXISTS conduit_projection_state;
                     CREATE TABLE users (
                         id        text PRIMARY KEY,
                         name      text,
                         bio       text,
                         followers bigint
                     )",
                )
                .expect("create users table");
        }
        "dump" => {
            for row in client
                .query(
                    "SELECT id, name, bio, followers FROM users ORDER BY id",
                    &[],
                )
                .expect("query users")
            {
                let id: String = row.get(0);
                let name: String = row.get(1);
                let bio: String = row.get(2);
                let followers: i64 = row.get(3);
                println!("{id}|{name}|{bio}|{followers}");
            }
        }
        other => panic!("unknown command {other:?}; {usage}"),
    }
}
