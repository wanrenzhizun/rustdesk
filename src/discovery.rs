use std::{
    io::Read,
    sync::Mutex,
    time::{Duration, Instant},
};

use hbb_common::{
    config::{Config, EXE_RENDEZVOUS_SERVER},
    log, tokio,
};

/// How often [`watch`] compares the saved endpoint against the one already applied.
const WATCH_INTERVAL: std::time::Duration = std::time::Duration::from_secs(2);

/// Option holding the discovery endpoint. Empty means the feature is off.
pub const OPTION_DISCOVERY_URL: &str = "discovery-url";
const OPTION_ONLINE_SERVER: &str = "online-server";

const CACHE_TTL: Duration = Duration::from_secs(60);
const FETCH_TIMEOUT: Duration = Duration::from_secs(3);
/// How long the first options read waits for the endpoint's answer.
const FIRST_WAIT: Duration = Duration::from_secs(5);
const MAX_BODY_BYTES: u64 = 64 * 1024;
const KEY_LEN: usize = 44;

/// Servers announced by the discovery endpoint. An empty field is "not announced".
#[derive(Clone, Debug, Default, PartialEq, Eq)]
struct Discovered {
    id_server: String,
    relay_server: String,
    api_server: String,
    online_server: String,
    key: String,
}

impl Discovered {
    fn is_empty(&self) -> bool {
        self.id_server.is_empty()
            && self.relay_server.is_empty()
            && self.api_server.is_empty()
            && self.online_server.is_empty()
            && self.key.is_empty()
    }
}

struct Cache {
    last_attempt: Option<Instant>,
    last_url: String,
    value: Option<Discovered>,
}

static CACHE: Mutex<Cache> = Mutex::new(Cache {
    last_attempt: None,
    last_url: String::new(),
    value: None,
});

/// What the callers below hand out right now. Nothing here is persisted: the values the user
/// typed stay in the config file and take over again as soon as discovery stops answering.
struct State {
    active: Discovered,
    /// The executable level rendezvous server we replaced, kept to put it back on reset.
    previous_exe_server: Option<String>,
}

static STATE: Mutex<State> = Mutex::new(State {
    active: Discovered {
        id_server: String::new(),
        relay_server: String::new(),
        api_server: String::new(),
        online_server: String::new(),
        key: String::new(),
    },
    previous_exe_server: None,
});

/// Watches `discovery-url` and applies a change the moment it happens.
///
/// The connect loop only reads the endpoint when it (re)starts, so without this a saved address
/// would idle until something else forced a reconnect. Polling the option keeps the hook out of
/// the FFI layer, which changes often upstream.
pub fn watch() {
    if WATCHING.swap(true, std::sync::atomic::Ordering::SeqCst) {
        return;
    }
    tokio::spawn(watch_here());
}

/// Polls the endpoint until the process dies, restarting the mediator when the answer changes.
async fn watch_here() {
    let mut seen_url = String::new();
    loop {
        tokio::time::sleep(WATCH_INTERVAL).await;
        let url = Config::get_option(OPTION_DISCOVERY_URL).trim().to_owned();
        if url.is_empty() {
            if seen_url.is_empty() {
                continue;
            }
            log::info!("discovery: endpoint cleared, back to the manual settings");
            seen_url.clear();
            apply(None);
            crate::rendezvous_mediator::RendezvousMediator::restart();
            continue;
        }
        // The document behind the URL can change without the URL changing, so re-read it
        // instead of remembering either one. Comparing the answer keeps the reconnect away
        // from every poll that returns the same servers.
        let before = STATE.lock().unwrap().active.clone();
        let changed_url = url != seen_url;
        seen_url = url.clone();
        drop_before_refetch();
        sync().await;
        let after = STATE.lock().unwrap().active.clone();
        if changed_url || after != before {
            log::info!("discovery: servers changed, reconnecting");
            crate::rendezvous_mediator::RendezvousMediator::restart();
        }
    }
}
/// Set by whichever entry point started the poller, so only one runs per process.
static WATCHING: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);

/// Makes the next [`sync`] fetch instead of serving the cached answer.
fn drop_before_refetch() {
    CACHE.lock().unwrap().last_attempt = None;
}

/// Resolves the endpoint once on a worker thread, for callers that run before the connect loop.
///
/// The client UI reads its options before any server exists, so without a first resolution here
/// it reports the public server until the host happens to start.
/// Runs the first resolution on a worker thread and waits for it, bounded.
///
/// A caller that reads its options before the connect loop exists would otherwise report the
/// public server for as long as the fetch takes, so the first read waits instead. Once the cache
/// holds an answer the wait returns immediately, which is what every later read does.
pub fn start_blocking() {
    // A condvar rather than a channel: the value is read from many threads, and a channel's
    // receiver is not Sync.
    static DONE: std::sync::OnceLock<
        (std::sync::Mutex<bool>, std::sync::Condvar),
    > = std::sync::OnceLock::new();
    static STARTED: std::sync::atomic::AtomicBool =
        std::sync::atomic::AtomicBool::new(false);

    let gate = DONE.get_or_init(|| (std::sync::Mutex::new(false), std::sync::Condvar::new()));
    if !STARTED.swap(true, std::sync::atomic::Ordering::SeqCst) {
        let _ = std::thread::Builder::new()
            .name("discovery-first".to_owned())
            .spawn(|| {
                if let Ok(rt) = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                {
                    // The loop keeps the resolved servers current after this first answer; the
                    // connect path only starts it once a server exists, which on mobile is not
                    // until the user shares a screen.
                    rt.block_on(async {
                        sync().await;
                        let (lock, cv) = &*DONE.get().expect("gate");
                        *lock.lock().unwrap() = true;
                        cv.notify_all();
                        watch_here().await;
                    });
                } else {
                    let (lock, cv) = &*DONE.get().expect("gate");
                    *lock.lock().unwrap() = true;
                    cv.notify_all();
                }
            });
    }
    let (lock, cv) = gate;
    let mut done = lock.lock().unwrap();
    if !*done {
        let _ = cv.wait_timeout(done, FIRST_WAIT);
    }
}
/// Injects the discovered servers into the runtime config when `discovery-url` is set.
///
/// Called by the rendezvous mediator once at startup and again before every reconnect, so it has
/// to stay cheap: a fresh cache only re-applies, a stale one fetches on a blocking worker bounded
/// by [`FETCH_TIMEOUT`]. A failed fetch keeps the previous answer, and with no previous answer the
/// manual configuration stays in place.
pub async fn sync() {
    let url = Config::get_option(OPTION_DISCOVERY_URL);
    let url = url.trim().to_owned();
    if url.is_empty() {
        if !STATE.lock().unwrap().is_active() {
            log::trace!("discovery: no endpoint configured");
            return;
        }
        log::info!("discovery: endpoint cleared, back to the manual settings");
        apply(None);
        return;
    }
    {
        let mut cache = CACHE.lock().unwrap();
        if cache.last_url != url {
            cache.last_url = url.clone();
            cache.last_attempt = None;
        }
        if let Some(at) = cache.last_attempt {
            if at.elapsed() < CACHE_TTL {
                let cached = cache.value.clone();
                drop(cache);
                apply(cached.as_ref());
                return;
            }
        }
    }
    let fetched = match tokio::task::spawn_blocking({
        let url = url.clone();
        move || fetch(&url)
    })
    .await
    {
        Ok(result) => result,
        Err(err) => Err(format!("worker failed: {err}")),
    };
    let cached = {
        let mut cache = CACHE.lock().unwrap();
        cache.last_attempt = Some(Instant::now());
        match fetched {
            Ok(value) => cache.value = Some(value),
            Err(err) => log::warn!("discovery: {url}: {err}"),
        }
        cache.value.clone()
    };
    apply(cached.as_ref());
}

/// Whether the rendezvous server in use comes from the endpoint rather than the manual setting.
/// Overlays the servers the endpoint announced onto an options map.
///
/// The client processes read their options over IPC from the host, so the values resolved here
/// have to travel with that answer: without this the UI keeps reporting the public server while
/// the host is already connected to the endpoint's.
pub fn overlay_options(options: &mut std::collections::HashMap<String, String>) {
    let state = STATE.lock().unwrap();
    if !state.is_active() {
        return;
    }
    for (option, value) in [
        ("custom-rendezvous-server", &state.active.id_server),
        ("relay-server", &state.active.relay_server),
        ("api-server", &state.active.api_server),
        ("online-server", &state.active.online_server),
        ("key", &state.active.key),
    ] {
        if !value.is_empty() {
            options.insert(option.to_owned(), value.clone());
        }
    }
}

pub fn id_server() -> Option<String> {
    non_empty(STATE.lock().unwrap().active.id_server.clone())
}

/// The announced API server, when it is currently overridden.
pub fn api_server() -> Option<String> {
    non_empty(STATE.lock().unwrap().active.api_server.clone())
}

/// The announced online-status server, when it is currently overridden.
///
/// The client otherwise derives it as `id_server` port minus one, which only works when the
/// endpoints share a host and stay adjacent, as they do on an unmapped rustdesk-server.
pub fn online_server() -> Option<String> {
    non_empty(STATE.lock().unwrap().active.online_server.clone())
        .or_else(|| non_empty(Config::get_option(OPTION_ONLINE_SERVER)))
}

/// The announced relay server, when it is currently overridden.
pub fn relay_server() -> Option<String> {
    non_empty(STATE.lock().unwrap().active.relay_server.clone())
}

/// The announced public key, when it is currently overridden.
pub fn key() -> Option<String> {
    non_empty(STATE.lock().unwrap().active.key.clone())
}

fn non_empty(value: String) -> Option<String> {
    if value.is_empty() {
        None
    } else {
        Some(value)
    }
}

impl State {
    fn is_active(&self) -> bool {
        !self.active.is_empty() || self.previous_exe_server.is_some()
    }
}

fn apply(value: Option<&Discovered>) {
    let mut state = STATE.lock().unwrap();
    match value {
        Some(value) => apply_value(&mut state, value),
        None => restore(&mut state),
    }
}

fn apply_value(state: &mut State, value: &Discovered) {
    if !value.id_server.is_empty() {
        if state.previous_exe_server.is_none() {
            state.previous_exe_server = Some(EXE_RENDEZVOUS_SERVER.read().unwrap().clone());
        }
        let mut exe_rendezvous_server = EXE_RENDEZVOUS_SERVER.write().unwrap();
        if *exe_rendezvous_server != value.id_server {
            log::info!("discovery: rendezvous server set to {}", value.id_server);
            *exe_rendezvous_server = value.id_server.clone();
        }
    }
    for (announced, applied, option) in [
        (&value.relay_server, &state.active.relay_server, "relay-server"),
        (&value.api_server, &state.active.api_server, "api-server"),
        (
            &value.online_server,
            &state.active.online_server,
            "online-server",
        ),
        (&value.key, &state.active.key, "key"),
    ] {
        if !announced.is_empty() && announced != applied {
            log::info!("discovery: {option} set to {announced}");
        }
    }
    state.active = value.clone();
}

fn restore(state: &mut State) {
    if let Some(previous) = state.previous_exe_server.take() {
        let mut exe_rendezvous_server = EXE_RENDEZVOUS_SERVER.write().unwrap();
        if *exe_rendezvous_server != previous {
            log::info!("discovery: rendezvous server restored to {previous:?}");
            *exe_rendezvous_server = previous;
        }
    }
    state.active = Discovered::default();
}

fn fetch(url: &str) -> Result<Discovered, String> {
    if !url.starts_with("https://") {
        log::warn!("discovery: {url} is not an https address");
    }
    // Reuse the app's client so the TLS backend matches the rest of the client and the
    // per-host probe cache is shared. A plain HTTP endpoint takes the Plain path.
    let client = crate::hbbs_http::create_http_client_with_url(url);
    let resp = client
        .get(url)
        .timeout(FETCH_TIMEOUT)
        .send()
        .map_err(|err| err.to_string())?;
    let status = resp.status();
    if !status.is_success() {
        return Err(format!("http status {status}"));
    }
    let mut body = Vec::new();
    resp.take(MAX_BODY_BYTES)
        .read_to_end(&mut body)
        .map_err(|err| err.to_string())?;
    if body.len() as u64 >= MAX_BODY_BYTES {
        log::warn!("discovery: response truncated at {MAX_BODY_BYTES} bytes");
    }
    parse_body(&String::from_utf8_lossy(&body)).ok_or_else(|| "unusable response".to_owned())
}

/// JSON (`{...}`) or plain text whose first non-empty line is the ID server.
fn parse_body(body: &str) -> Option<Discovered> {
    let body = body.trim_start_matches('\u{feff}').trim();
    if body.is_empty() {
        return None;
    }
    if !body.starts_with('{') {
        let id_server = body.lines().map(str::trim).find(|line| !line.is_empty())?;
        return if is_plausible_server(id_server) {
            Some(Discovered {
                id_server: id_server.to_owned(),
                ..Default::default()
            })
        } else {
            None
        };
    }
    let value: serde_json::Value = serde_json::from_str(body).ok()?;
    let field = |name: &str| {
        value
            .get(name)
            .and_then(|field| field.as_str())
            .unwrap_or_default()
            .trim()
            .to_owned()
    };
    let mut discovered = Discovered {
        id_server: field("id_server"),
        relay_server: field("relay_server"),
        api_server: field("api_server"),
        online_server: field("online_server"),
        key: field("key"),
    };
    if !is_plausible_server(&discovered.id_server) {
        log::warn!("discovery: response has no usable id_server");
        return None;
    }
    if !discovered.key.is_empty() && !is_valid_key(&discovered.key) {
        log::warn!("discovery: ignoring malformed key");
        discovered.key.clear();
    }
    Some(discovered)
}

/// `host:port`, or a bare host that upstream completes with the default port.
fn is_plausible_server(server: &str) -> bool {
    !server.is_empty() && !server.contains(char::is_whitespace) && !server.contains('/')
}

/// A RustDesk public key is base64 of a 32 byte key, so 44 characters with padding.
fn is_valid_key(key: &str) -> bool {
    key.len() == KEY_LEN
        && key.ends_with('=')
        && key
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'+' || b == b'/' || b == b'=')
}

#[cfg(test)]
mod tests {
    use super::{is_valid_key, parse_body, Discovered};

    fn id_only(id_server: &str) -> Discovered {
        Discovered {
            id_server: id_server.to_owned(),
            ..Default::default()
        }
    }

    #[test]
    fn parse_json_with_all_fields() {
        let body = r#"{
            "id_server": "119.1.189.42:23456",
            "relay_server": "119.1.189.42:23457",
            "api_server": "http://119.1.189.42:21114",
            "online_server": "119.1.189.42:23455",
            "key": "OeVuKk5nlHiXp+APNn0Y3pC1Iwpwn44JGqrQCsWqmBw="
        }"#;
        assert_eq!(
            parse_body(body),
            Some(Discovered {
                id_server: "119.1.189.42:23456".to_owned(),
                relay_server: "119.1.189.42:23457".to_owned(),
                api_server: "http://119.1.189.42:21114".to_owned(),
                online_server: "119.1.189.42:23455".to_owned(),
                key: "OeVuKk5nlHiXp+APNn0Y3pC1Iwpwn44JGqrQCsWqmBw=".to_owned(),
            })
        );
    }

    #[test]
    fn parse_json_reads_an_explicit_online_server() {
        let parsed = parse_body(
            r#"{"id_server": "a:11663", "online_server": "a:34567"}"#,
        )
        .unwrap();
        assert_eq!(parsed.online_server, "a:34567");
    }

    #[test]
    fn parse_json_leaves_online_server_empty_when_absent() {
        let parsed = parse_body(r#"{"id_server": "a:11663"}"#).unwrap();
        assert_eq!(parsed.online_server, "");
    }

    #[test]
    fn parse_json_passes_api_server_through_untouched() {
        let parsed =
            parse_body(r#"{"id_server": "a:1", "api_server": "https://api.example.com/"}"#)
                .unwrap();
        assert_eq!(parsed.api_server, "https://api.example.com/");
    }

    #[test]
    fn parse_json_without_optional_fields_leaves_them_empty() {
        let parsed = parse_body(r#"{"id_server": "a:1"}"#).unwrap();
        assert_eq!(parsed, id_only("a:1"));
    }

    #[test]
    fn parse_json_drops_empty_and_malformed_key() {
        for body in [
            r#"{"id_server": "a:1", "key": ""}"#,
            r#"{"id_server": "a:1", "key": "not-a-key"}"#,
            r#"{"id_server": "a:1", "key": "OeVuKk5nlHiXp+APNn0Y3pC1Iwpwn44JGqrQCsWqmBw"}"#,
        ] {
            assert_eq!(parse_body(body).unwrap().key, "");
        }
    }

    #[test]
    fn parse_json_requires_id_server() {
        assert_eq!(parse_body(r#"{"relay_server": "b:2"}"#), None);
        assert_eq!(parse_body(r#"{"id_server": ""}"#), None);
    }

    #[test]
    fn parse_invalid_json_fails() {
        assert_eq!(parse_body(r#"{"id_server": "a:1""#), None);
        assert_eq!(parse_body(r#"{"id_server": "a b"}"#), None);
    }

    #[test]
    fn parse_plain_text_uses_first_line() {
        assert_eq!(
            parse_body("119.1.189.42:23456
"),
            Some(id_only("119.1.189.42:23456"))
        );
        assert_eq!(
            parse_body("
  119.1.189.42:23456  
119.1.189.42:23457
"),
            Some(id_only("119.1.189.42:23456"))
        );
    }

    #[test]
    fn parse_empty_body_fails() {
        assert_eq!(parse_body(""), None);
        assert_eq!(parse_body("  
	
"), None);
    }

    #[test]
    fn key_must_be_base64_of_32_bytes() {
        assert!(is_valid_key("OeVuKk5nlHiXp+APNn0Y3pC1Iwpwn44JGqrQCsWqmBw="));
        assert!(!is_valid_key("OeVuKk5nlHiXp+APNn0Y3pC1Iwpwn44JGqrQCsWqmBw"));
        assert!(!is_valid_key("OeVuKk5nlHiXp APNn0Y3pC1Iwpwn44JGqrQCsWqmBw="));
    }
}
