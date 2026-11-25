//! Hide credentials in connection strings before they reach an error message,
//! a log line or a `Debug` print.
//!
//! Every backend and source that takes a URL or DSN uses [`redact_url`] for
//! anything it echoes. It works on the raw text, not on a parsed URL, because
//! the strings that need it most are the ones that do not parse (an unescaped
//! character in a password is a common reason a URL is refused).
//!
//! What is hidden:
//! - the password in `scheme://user:password@host…` (shown as `user:***@`),
//!   and a bare userinfo with no `:` when the scheme uses it as a secret
//!   (`redis://token@host` → `redis://***@host`);
//! - the value of every query parameter, or `key=value` keyword, named
//!   `password`, `sslpassword`, `token`, `apikey`, `api_key`, `access_token` or
//!   `secret`, compared without case.
//!
//! Redaction may hide more than the secret (when an `@` appears later in the
//! string, everything before it is treated as userinfo); it never shows less.

/// The value shown in place of a secret.
pub const MASK: &str = "***";

/// Query parameters and connection keywords whose values are secrets.
const SECRET_KEYS: &[&str] = &[
    "password",
    "sslpassword",
    "token",
    "apikey",
    "api_key",
    "access_token",
    "secret",
];

fn is_secret_key(key: &str) -> bool {
    let key = key.trim();
    SECRET_KEYS.iter().any(|k| k.eq_ignore_ascii_case(key))
}

/// `s` with every credential hidden: the URL's userinfo password and every
/// secret query parameter, or for a keyword DSN (`host=… password=…`) every
/// secret keyword's value.
pub fn redact_url(s: &str) -> String {
    match s.find("://") {
        Some(scheme_end) => redact_uri(s, scheme_end + 3),
        None => redact_keywords(s),
    }
}

/// A `scheme://…` string; `rest` is where the part after `://` starts.
fn redact_uri(s: &str, rest: usize) -> String {
    let (head, tail) = s.split_at(rest);
    let mut out = String::with_capacity(s.len());
    out.push_str(head);
    // Userinfo ends at the last `@`: a password may itself hold `@`, `/`, `?`
    // or `#` when it was not percent-encoded.
    let after_userinfo = match tail.rfind('@') {
        Some(at) => {
            let userinfo = &tail[..at];
            match userinfo.find(':') {
                Some(colon) => {
                    out.push_str(&userinfo[..colon]);
                    out.push(':');
                    out.push_str(MASK);
                }
                None if userinfo.is_empty() => {}
                // `redis://secret@host` uses a lone userinfo as the password;
                // a lone user name is not a secret, but which one it is cannot
                // be told from the text, so it is hidden too.
                None => out.push_str(MASK),
            }
            out.push('@');
            &tail[at + 1..]
        }
        None => tail,
    };
    match after_userinfo.find('?') {
        Some(q) => {
            out.push_str(&after_userinfo[..=q]);
            let (query, fragment) = match after_userinfo[q + 1..].find('#') {
                Some(h) => after_userinfo[q + 1..].split_at(h),
                None => (&after_userinfo[q + 1..], ""),
            };
            let params: Vec<String> = query
                .split('&')
                .map(|pair| match pair.split_once('=') {
                    Some((k, _)) if is_secret_key(k) => format!("{k}={MASK}"),
                    _ => pair.to_string(),
                })
                .collect();
            out.push_str(&params.join("&"));
            out.push_str(fragment);
        }
        None => out.push_str(after_userinfo),
    }
    out
}

/// A keyword DSN: `key=value` pairs separated by whitespace, a value
/// optionally in single quotes (with `\'` inside), as libpq accepts.
fn redact_keywords(s: &str) -> String {
    let bytes = s.as_bytes();
    let mut out = String::with_capacity(s.len());
    let mut i = 0;
    while i < bytes.len() {
        // Copy whitespace.
        if bytes[i].is_ascii_whitespace() {
            out.push(bytes[i] as char);
            i += 1;
            continue;
        }
        // A token: up to `=` (a key) or whitespace (not a pair).
        let start = i;
        while i < bytes.len() && bytes[i] != b'=' && !bytes[i].is_ascii_whitespace() {
            i += 1;
        }
        let key = &s[start..i];
        // libpq allows spaces between a key and its `=`.
        let mut eq = i;
        while eq < bytes.len() && bytes[eq] == b' ' {
            eq += 1;
        }
        if eq >= bytes.len() || bytes[eq] != b'=' {
            out.push_str(key);
            continue;
        }
        out.push_str(&s[start..=eq]);
        i = eq + 1;
        // Spaces may follow `=` in libpq; kept as they are.
        while i < bytes.len() && bytes[i] == b' ' {
            out.push(' ');
            i += 1;
        }
        // The value runs to the next whitespace outside quotes, however many
        // quoted pieces it has (`'a'b'c d'` is one value). A malformed value
        // is still taken whole, so no piece of a secret is left showing.
        let value_start = i;
        while i < bytes.len() && !bytes[i].is_ascii_whitespace() {
            if bytes[i] == b'\'' {
                i += 1;
                while i < bytes.len() && bytes[i] != b'\'' {
                    if bytes[i] == b'\\' {
                        i += 1;
                    }
                    i += 1;
                }
            }
            i = (i + 1).min(bytes.len());
        }
        if is_secret_key(key) {
            out.push_str(MASK);
        } else {
            out.push_str(&s[value_start..i]);
        }
    }
    out
}

/// `MASK` for a secret that is set, the empty string for one that is not:
/// for `Debug` impls of configs with a separate password field.
pub fn redact_secret(secret: &str) -> &'static str {
    if secret.is_empty() { "" } else { MASK }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn userinfo_password_is_hidden() {
        assert_eq!(
            redact_url("postgres://app:Hunter2@db:5432/app"),
            "postgres://app:***@db:5432/app"
        );
        // Unescaped `@`, `/`, `?`, `#` in the password: still hidden.
        assert_eq!(
            redact_url("postgres://app:p@ss/w?rd#x@db:54x32/app"),
            "postgres://app:***@db:54x32/app"
        );
        assert_eq!(
            redact_url("redis://:s3cret@cache:6379"),
            "redis://:***@cache:6379"
        );
        assert_eq!(
            redact_url("redis://s3cret@cache:6379"),
            "redis://***@cache:6379"
        );
        assert_eq!(redact_url("http://es:9200"), "http://es:9200");
    }

    #[test]
    fn secret_query_parameters_are_hidden() {
        assert_eq!(
            redact_url("mongodb://u:p@h/db?authSource=admin&Password=x&apiKey=k&tls=true"),
            "mongodb://u:***@h/db?authSource=admin&Password=***&apiKey=***&tls=true"
        );
        assert_eq!(
            redact_url("mysql://h/db?ssl-mode=REQUIRED&SSLPASSWORD=abc#frag"),
            "mysql://h/db?ssl-mode=REQUIRED&SSLPASSWORD=***#frag"
        );
        for key in ["token", "api_key", "access_token", "secret"] {
            let s = format!("http://h/?{key}=v&a=b");
            assert_eq!(redact_url(&s), format!("http://h/?{key}=***&a=b"), "{key}");
        }
    }

    #[test]
    fn keyword_dsn_secrets_are_hidden() {
        assert_eq!(
            redact_url("host=db user=app password=Hunter2 dbname=app"),
            "host=db user=app password=*** dbname=app"
        );
        assert_eq!(
            redact_url("host=db password = 'it''s \\' secret' sslpassword=x port=5432"),
            "host=db password = *** sslpassword=*** port=5432"
        );
    }

    #[test]
    fn nothing_secret_survives() {
        for s in [
            "postgres://app:Hunter2@db:54x32/app?password=Hunter2",
            "redis://Hunter2@h",
            "host=h password=Hunter2",
            "mongodb://a:Hunter2@h1,h2/db?replicaSet=rs&token=Hunter2",
            // Quoting libpq would refuse: the whole value is still hidden.
            "host=h password='Hun'ter2' x",
            "host=h password='Hunter2 still open",
            "host=h PASSWORD   =   Hunter2",
        ] {
            assert!(
                !redact_url(s).contains("Hunter2"),
                "{s} -> {}",
                redact_url(s)
            );
        }
    }
}
