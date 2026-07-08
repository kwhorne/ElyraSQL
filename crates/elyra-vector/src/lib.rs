//! ElyraSQL native vector search.
//!
//! ElyraSQL treats vectors as a first-class column type (`VECTOR(n)`) with a
//! MySQL-flavoured surface: `VEC_DISTANCE(a, b)` plus distance functions used
//! in `ORDER BY ... LIMIT k` for approximate nearest-neighbour (ANN) search.
//!
//! Milestone status: **planned** (an HNSW index backs `VECTOR` columns).
//! The distance math below is real and used for exact search / tests today.

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
        Metric::L2 => a.iter().zip(b).map(|(x, y)| (x - y) * (x - y)).sum(),
        Metric::InnerProduct => -a.iter().zip(b).map(|(x, y)| x * y).sum::<f32>(),
        Metric::Cosine => {
            let dot: f32 = a.iter().zip(b).map(|(x, y)| x * y).sum();
            let na: f32 = a.iter().map(|x| x * x).sum::<f32>().sqrt();
            let nb: f32 = b.iter().map(|x| x * x).sum::<f32>().sqrt();
            if na == 0.0 || nb == 0.0 {
                1.0
            } else {
                1.0 - dot / (na * nb)
            }
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn l2_identity_is_zero() {
        assert_eq!(distance(&[1.0, 2.0, 3.0], &[1.0, 2.0, 3.0], Metric::L2), Some(0.0));
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
