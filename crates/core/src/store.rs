use std::collections::HashMap;
use std::sync::RwLock;
use std::time::{Duration, Instant};

struct Entry {
    value: Vec<u8>,
    expires_at: Option<Instant>,
}

impl Entry {
    fn is_expired(&self, now: Instant) -> bool {
        matches!(self.expires_at, Some(deadline) if deadline <= now)
    }
}

/// A thread-safe in-memory key-value store with per-key expiry.
///
/// Expired entries are removed lazily on access rather than by a background
/// sweep, so `Store` never spawns a task of its own — callers who want
/// active eviction can drive `purge_expired` on a timer.
pub struct Store {
    map: RwLock<HashMap<Vec<u8>, Entry>>,
}

impl Store {
    pub fn new() -> Self {
        Store {
            map: RwLock::new(HashMap::new()),
        }
    }

    pub fn set(&self, key: Vec<u8>, value: Vec<u8>) {
        let mut map = self.map.write().unwrap();
        map.insert(
            key,
            Entry {
                value,
                expires_at: None,
            },
        );
    }

    pub fn set_with_ttl(&self, key: Vec<u8>, value: Vec<u8>, ttl: Duration) {
        let mut map = self.map.write().unwrap();
        map.insert(
            key,
            Entry {
                value,
                expires_at: Some(Instant::now() + ttl),
            },
        );
    }

    pub fn get(&self, key: &[u8]) -> Option<Vec<u8>> {
        let now = Instant::now();
        {
            let map = self.map.read().unwrap();
            match map.get(key) {
                Some(entry) if !entry.is_expired(now) => return Some(entry.value.clone()),
                Some(_) => {}
                None => return None,
            }
        }
        // Entry was present but expired: drop it under a write lock.
        let mut map = self.map.write().unwrap();
        map.remove(key);
        None
    }

    pub fn del(&self, key: &[u8]) -> bool {
        self.map.write().unwrap().remove(key).is_some()
    }

    pub fn exists(&self, key: &[u8]) -> bool {
        self.get(key).is_some()
    }

    /// Sets a new expiry on an existing key. Returns `false` if the key is
    /// absent or already expired.
    pub fn expire(&self, key: &[u8], ttl: Duration) -> bool {
        let mut map = self.map.write().unwrap();
        match map.get_mut(key) {
            Some(entry) if !entry.is_expired(Instant::now()) => {
                entry.expires_at = Some(Instant::now() + ttl);
                true
            }
            _ => false,
        }
    }

    /// Remaining time-to-live for a key: `Some(None)` for a key with no
    /// expiry set, `Some(Some(d))` for a key expiring in `d`, `None` if the
    /// key doesn't exist (or is already expired).
    pub fn ttl(&self, key: &[u8]) -> Option<Option<Duration>> {
        let now = Instant::now();
        let map = self.map.read().unwrap();
        match map.get(key) {
            Some(entry) if entry.is_expired(now) => None,
            Some(entry) => Some(entry.expires_at.map(|d| d.saturating_duration_since(now))),
            None => None,
        }
    }

    pub fn len(&self) -> usize {
        self.map.read().unwrap().len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn flush(&self) {
        self.map.write().unwrap().clear();
    }

    /// Sweeps all currently-expired entries out of the map. Useful for a
    /// caller that wants to bound memory rather than rely purely on lazy
    /// eviction from `get`.
    pub fn purge_expired(&self) -> usize {
        let now = Instant::now();
        let mut map = self.map.write().unwrap();
        let before = map.len();
        map.retain(|_, entry| !entry.is_expired(now));
        before - map.len()
    }
}

impl Default for Store {
    fn default() -> Self {
        Store::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread;

    fn key(s: &str) -> Vec<u8> {
        s.as_bytes().to_vec()
    }

    #[test]
    fn set_then_get_returns_value() {
        let store = Store::new();
        store.set(key("a"), b"1".to_vec());
        assert_eq!(store.get(&key("a")), Some(b"1".to_vec()));
    }

    #[test]
    fn get_missing_key_returns_none() {
        let store = Store::new();
        assert_eq!(store.get(&key("missing")), None);
    }

    #[test]
    fn set_overwrites_existing_value() {
        let store = Store::new();
        store.set(key("a"), b"1".to_vec());
        store.set(key("a"), b"2".to_vec());
        assert_eq!(store.get(&key("a")), Some(b"2".to_vec()));
    }

    #[test]
    fn del_removes_key_and_reports_presence() {
        let store = Store::new();
        store.set(key("a"), b"1".to_vec());
        assert!(store.del(&key("a")));
        assert!(!store.del(&key("a")));
        assert_eq!(store.get(&key("a")), None);
    }

    #[test]
    fn exists_reflects_current_state() {
        let store = Store::new();
        assert!(!store.exists(&key("a")));
        store.set(key("a"), b"1".to_vec());
        assert!(store.exists(&key("a")));
    }

    #[test]
    fn ttl_of_key_without_expiry_is_some_none() {
        let store = Store::new();
        store.set(key("a"), b"1".to_vec());
        assert_eq!(store.ttl(&key("a")), Some(None));
    }

    #[test]
    fn ttl_of_missing_key_is_none() {
        let store = Store::new();
        assert_eq!(store.ttl(&key("a")), None);
    }

    #[test]
    fn set_with_ttl_expires_key() {
        let store = Store::new();
        store.set_with_ttl(key("a"), b"1".to_vec(), Duration::from_millis(20));
        assert_eq!(store.get(&key("a")), Some(b"1".to_vec()));
        thread::sleep(Duration::from_millis(40));
        assert_eq!(store.get(&key("a")), None);
    }

    #[test]
    fn expire_sets_ttl_on_existing_key() {
        let store = Store::new();
        store.set(key("a"), b"1".to_vec());
        assert!(store.expire(&key("a"), Duration::from_millis(20)));
        assert!(store.ttl(&key("a")).unwrap().is_some());
        thread::sleep(Duration::from_millis(40));
        assert_eq!(store.get(&key("a")), None);
    }

    #[test]
    fn expire_on_missing_key_returns_false() {
        let store = Store::new();
        assert!(!store.expire(&key("a"), Duration::from_secs(1)));
    }

    #[test]
    fn len_and_is_empty_track_live_entries() {
        let store = Store::new();
        assert!(store.is_empty());
        store.set(key("a"), b"1".to_vec());
        store.set(key("b"), b"2".to_vec());
        assert_eq!(store.len(), 2);
        store.del(&key("a"));
        assert_eq!(store.len(), 1);
        assert!(!store.is_empty());
    }

    #[test]
    fn flush_clears_all_entries() {
        let store = Store::new();
        store.set(key("a"), b"1".to_vec());
        store.set(key("b"), b"2".to_vec());
        store.flush();
        assert!(store.is_empty());
    }

    #[test]
    fn purge_expired_removes_only_expired_entries() {
        let store = Store::new();
        store.set(key("live"), b"1".to_vec());
        store.set_with_ttl(key("dead"), b"2".to_vec(), Duration::from_millis(10));
        thread::sleep(Duration::from_millis(30));
        let removed = store.purge_expired();
        assert_eq!(removed, 1);
        assert_eq!(store.len(), 1);
        assert!(store.exists(&key("live")));
    }
}
