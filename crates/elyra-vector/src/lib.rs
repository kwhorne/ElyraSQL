//! ElyraSQL native vector search.
//!
//! ElyraSQL treats vectors as a first-class column type (`VECTOR(n)`) with a
//! MySQL-flavoured surface: `VEC_DISTANCE(a, b)` plus distance functions used
//! in `ORDER BY ... LIMIT k` for approximate nearest-neighbour (ANN) search.
//!
//! An HNSW index backs indexed `VECTOR` columns for approximate nearest-neighbour
//! search. The distance math below is also used for exact search and tests.

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
    // `as_chunks` hands back `&[[f32; 8]]`, so the lanes go straight into the
    // vector type -- `chunks_exact` yielded slices that needed a fallible
    // `try_into().unwrap()` per iteration in the inner loop.
    let (ca, ra) = a.as_chunks::<8>();
    let (cb, rb) = b.as_chunks::<8>();
    for (xa, xb) in ca.iter().zip(cb) {
        let d = f32x8::new(*xa) - f32x8::new(*xb);
        acc += d * d;
    }
    let mut sum = acc.reduce_add();
    for (x, y) in ra.iter().zip(rb) {
        let d = x - y;
        sum += d * d;
    }
    sum
}

/// Inner product, SIMD-accelerated (8-wide) with a scalar remainder.
fn dot(a: &[f32], b: &[f32]) -> f32 {
    use wide::f32x8;
    let mut acc = f32x8::ZERO;
    let (ca, ra) = a.as_chunks::<8>();
    let (cb, rb) = b.as_chunks::<8>();
    for (xa, xb) in ca.iter().zip(cb) {
        acc += f32x8::new(*xa) * f32x8::new(*xb);
    }
    let mut sum = acc.reduce_add();
    for (x, y) in ra.iter().zip(rb) {
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
