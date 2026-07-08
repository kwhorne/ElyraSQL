//! ElyraSQL analytics (OLAP) acceleration layer.
//!
//! ElyraSQL routes heavy analytical queries (large aggregations, scans over
//! many rows) through a columnar engine that reads the same single-file data
//! exposed by [`elyra_storage`]. The OLTP path stays row-oriented and
//! transactional; this layer is read-mostly and vectorised.
//!
//! Milestone status: **planned**. The public surface below is the contract
//! the query engine will call; the columnar backend is wired up in the OLAP
//! milestone. Nothing here exposes the underlying engine name.

use elyra_core::Result;

/// Decides whether a query should be served by the analytical path.
pub struct OlapRouter;

impl Default for OlapRouter {
    fn default() -> Self {
        Self::new()
    }
}

impl OlapRouter {
    pub fn new() -> Self {
        OlapRouter
    }

    /// Heuristic: should this statement go to the analytical engine?
    /// Always `false` until the OLAP milestone lands.
    pub fn should_accelerate(&self, _sql: &str) -> bool {
        false
    }

    /// Execute an analytical query. Not yet available in this build.
    pub fn execute(&self, _sql: &str) -> Result<()> {
        Err(elyra_core::Error::Analytics(
            "analytical engine not enabled in this build".into(),
        ))
    }
}
