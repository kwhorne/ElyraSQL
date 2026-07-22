//! ElyraSQL native vector search.
//!
//! ElyraSQL treats vectors as a first-class column type (`VECTOR(n)`) with a
//! MySQL-flavoured surface: `VEC_DISTANCE(a, b)` plus distance functions used
//! in `ORDER BY ... LIMIT k` for approximate nearest-neighbour (ANN) search.
//!
//! Milestone status: **planned** (an HNSW index backs `VECTOR` columns).
//! The distance math below is real and used for exact search / tests today.

pub mod hnsw;
pub use hnsw::{Hnsw, HnswParts};

/// Distance/similarity metrics supported by ElyraSQL vector search.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Metric {
    /// Squared L2 (Euclidean) distance.
    L2,
    /// Cosine distance = 1 - cosine similarity.
    Cosine,
    /// Negative inner product (so smaller = more similar).
    InnerProduct,
}

/// Compute the distance between two equal-length vectors under `metric`.
/// Returns `None` on dimension mismatch.
pub fn distance(a: &[f32], b: &[f32], metric: Metric) -> Option<f32> {
    if a.len() != b.len() {
        return None;
    }
    Some(match metric {
        Metric::L2 => l2_sq(a, b),
        Metric::InnerProduct => -dot(a, b),
        Metric::Cosine => {
            let d = dot(a, b);
            let na = dot(a, a).sqrt();
            let nb = dot(b, b).sqrt();
            if na == 0.0 || nb == 0.0 {
                1.0
            } else {
                1.0 - d / (na * nb)
            }
        }
    })
}

/// Squared L2 distance, SIMD-accelerated (8-wide) with a scalar remainder.
fn l2_sq(a: &[f32], b: &[f32]) -> f32 {
    use wide::f32x8;
    let mut acc = f32x8::ZERO;
    let mut ca = a.chunks_exact(8);
    let mut cb = b.chunks_exact(8);
    for (xa, xb) in ca.by_ref().zip(cb.by_ref()) {
        let va = f32x8::new(xa.try_into().unwrap());
        let vb = f32x8::new(xb.try_into().unwrap());
        let d = va - vb;
        acc += d * d;
    }
    let mut sum = acc.reduce_add();
    for (x, y) in ca.remainder().iter().zip(cb.remainder()) {
        let d = x - y;
        sum += d * d;
    }
    sum
}

/// Inner product, SIMD-accelerated (8-wide) with a scalar remainder.
fn dot(a: &[f32], b: &[f32]) -> f32 {
    use wide::f32x8;
    let mut acc = f32x8::ZERO;
    let mut ca = a.chunks_exact(8);
    let mut cb = b.chunks_exact(8);
    for (xa, xb) in ca.by_ref().zip(cb.by_ref()) {
        let va = f32x8::new(xa.try_into().unwrap());
        let vb = f32x8::new(xb.try_into().unwrap());
        acc += va * vb;
    }
    let mut sum = acc.reduce_add();
    for (x, y) in ca.remainder().iter().zip(cb.remainder()) {
        sum += x * y;
    }
    sum
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn l2_identity_is_zero() {
        assert_eq!(
            distance(&[1.0, 2.0, 3.0], &[1.0, 2.0, 3.0], Metric::L2),
            Some(0.0)
        );
    }

    #[test]
    fn cosine_opposite_is_two() {
        let d = distance(&[1.0, 0.0], &[-1.0, 0.0], Metric::Cosine).unwrap();
        assert!((d - 2.0).abs() < 1e-6);
    }

    #[test]
    fn dimension_mismatch_is_none() {
        assert_eq!(distance(&[1.0], &[1.0, 2.0], Metric::L2), None);
    }
}
