//! Sizing redb's page cache.
//!
//! redb reads pages through `pread` and keeps recently used ones in its own
//! cache. Its default is 1 GiB, and ElyraSQL used that default, so a scan over
//! any table larger than the cache found nothing cached and re-read every page
//! through a syscall on every query -- even with the whole file in the OS page
//! cache and memory to spare. On a 2.6 GB table on a 128 GB machine that was a
//! third of an aggregate's CPU time.
//!
//! The default is now a quarter of the memory the process can use: physical
//! memory, or a container's cgroup limit when that is lower. The cache fills
//! only as pages are read, so a small database costs no more than before. An
//! explicit `ELYRASQL_PAGE_CACHE_MB` overrides it.

/// The environment variable that sets the page cache size, in MiB.
pub const PAGE_CACHE_ENV: &str = "ELYRASQL_PAGE_CACHE_MB";

const MIB: u64 = 1024 * 1024;
/// Used when the memory available cannot be determined: redb's own default.
const FALLBACK: u64 = 1024 * MIB;
/// Never smaller than this, however little memory the process has.
const FLOOR: u64 = 64 * MIB;

/// Where the page cache size came from, for the startup log line.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CacheSource {
    /// `ELYRASQL_PAGE_CACHE_MB`.
    Configured,
    /// A quarter of the memory available to the process.
    Memory { available: u64 },
    /// The available memory could not be read.
    Fallback,
}

/// The page cache size in bytes and where it came from.
pub fn page_cache_bytes() -> (u64, CacheSource) {
    let configured = std::env::var(PAGE_CACHE_ENV).ok();
    size_from(configured.as_deref(), available_memory())
}

/// The pure decision, separated from the environment for testing.
fn size_from(configured: Option<&str>, available: Option<u64>) -> (u64, CacheSource) {
    if let Some(mb) = configured.and_then(|v| v.trim().parse::<u64>().ok()) {
        return ((mb * MIB).max(FLOOR), CacheSource::Configured);
    }
    match available {
        Some(available) => (
            (available / 4).max(FLOOR),
            CacheSource::Memory { available },
        ),
        None => (FALLBACK, CacheSource::Fallback),
    }
}

/// Memory the process can use: the smaller of physical memory and a cgroup
/// memory limit. In a container, physical memory is the host's; sizing from it
/// could push the process past its limit and get it killed.
fn available_memory() -> Option<u64> {
    let physical = physical_memory();
    match (physical, cgroup_limit()) {
        (Some(p), Some(c)) => Some(p.min(c)),
        (p, c) => p.or(c),
    }
}

#[cfg(unix)]
fn physical_memory() -> Option<u64> {
    // SAFETY: sysconf reads a system constant and has no preconditions.
    let (pages, page_size) = unsafe {
        (
            libc::sysconf(libc::_SC_PHYS_PAGES),
            libc::sysconf(libc::_SC_PAGESIZE),
        )
    };
    if pages <= 0 || page_size <= 0 {
        return None;
    }
    (pages as u64).checked_mul(page_size as u64)
}

#[cfg(not(unix))]
fn physical_memory() -> Option<u64> {
    None
}

/// The cgroup memory limit, v2 or v1, if one is set.
fn cgroup_limit() -> Option<u64> {
    [
        "/sys/fs/cgroup/memory.max",
        "/sys/fs/cgroup/memory/memory.limit_in_bytes",
    ]
    .iter()
    .find_map(|path| {
        std::fs::read_to_string(path)
            .ok()
            .and_then(|s| parse_cgroup_limit(&s))
    })
}

/// A cgroup limit file's value, or `None` for "no limit". v2 writes `max`; v1
/// writes a very large number (about 2^63, page-aligned) when unlimited.
fn parse_cgroup_limit(text: &str) -> Option<u64> {
    let text = text.trim();
    if text == "max" {
        return None;
    }
    let n = text.parse::<u64>().ok()?;
    (n < (1u64 << 62)).then_some(n)
}

#[cfg(test)]
mod tests {
    use super::*;

    const GIB: u64 = 1024 * MIB;

    #[test]
    fn a_quarter_of_available_memory_by_default() {
        assert_eq!(
            size_from(None, Some(128 * GIB)),
            (
                32 * GIB,
                CacheSource::Memory {
                    available: 128 * GIB
                }
            )
        );
        assert_eq!(size_from(None, Some(16 * GIB)).0, 4 * GIB);
    }

    #[test]
    fn a_small_container_gets_a_small_cache_not_redbs_gigabyte() {
        // 512 MiB limit -> 128 MiB, where the old fixed default was 1 GiB.
        assert_eq!(size_from(None, Some(512 * MIB)).0, 128 * MIB);
        // And never below the floor.
        assert_eq!(size_from(None, Some(64 * MIB)).0, FLOOR);
    }

    #[test]
    fn the_environment_overrides() {
        assert_eq!(
            size_from(Some("8192"), Some(128 * GIB)),
            (8 * GIB, CacheSource::Configured)
        );
        assert_eq!(size_from(Some(" 256 "), None).0, 256 * MIB);
        // A value below the floor is raised to it; garbage is ignored.
        assert_eq!(size_from(Some("1"), None).0, FLOOR);
        assert_eq!(size_from(Some("lots"), Some(16 * GIB)).0, 4 * GIB);
    }

    #[test]
    fn unknown_memory_falls_back_to_redbs_default() {
        assert_eq!(size_from(None, None), (FALLBACK, CacheSource::Fallback));
    }

    #[test]
    fn cgroup_limits_parse_and_unlimited_is_none() {
        assert_eq!(parse_cgroup_limit("536870912\n"), Some(512 * MIB));
        assert_eq!(parse_cgroup_limit("max\n"), None);
        assert_eq!(parse_cgroup_limit("9223372036854771712"), None); // v1 "unlimited"
        assert_eq!(parse_cgroup_limit("junk"), None);
    }

    #[test]
    fn physical_memory_is_readable_here() {
        #[cfg(unix)]
        assert!(physical_memory().is_some_and(|m| m >= 256 * MIB));
    }
}
