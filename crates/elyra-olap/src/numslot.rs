//! Exact running state for one numeric aggregate over a typed column.
//!
//! The vectorised (columnar) aggregation paths used to carry every numeric
//! column as `f64` and convert integer results back at the end. That is exact
//! only below 2^53: `SUM` of `BIGINT` values past it rounded, and `MIN`/`MAX`
//! returned a neighbouring integer, silently. [`NumSlot`] keeps an integer
//! column's state as integers -- an `i128` sum, `i64` extremes -- and a float
//! column's as `f64`, and finishes both through the same rules as the general
//! aggregator ([`crate::GroupAggregator`]), so every path returns the same value
//! to the last digit.
//!
//! Which half is used is fixed by the column type, so the slot is small enough
//! to keep one per group per aggregate on the grouped paths.

use crate::AggFunc;
use elyra_core::Value;

/// State for one numeric aggregate (`COUNT`/`SUM`/`AVG`/`MIN`/`MAX`) over one
/// typed column. `count` is the number of non-NULL values seen.
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum NumSlot {
    /// An integer column: sum exact in `i128`, extremes exact in `i64`.
    Int {
        count: i64,
        sum: i128,
        min: i64,
        max: i64,
    },
    /// A floating-point column.
    Float {
        count: i64,
        sum: f64,
        min: f64,
        max: f64,
    },
}

impl NumSlot {
    /// An empty slot for a column of the given kind.
    pub fn new(is_int: bool) -> Self {
        if is_int {
            NumSlot::Int {
                count: 0,
                sum: 0,
                min: i64::MAX,
                max: i64::MIN,
            }
        } else {
            NumSlot::Float {
                count: 0,
                sum: 0.0,
                min: f64::INFINITY,
                max: f64::NEG_INFINITY,
            }
        }
    }

    /// Number of non-NULL values accumulated.
    #[inline]
    pub fn count(&self) -> i64 {
        match self {
            NumSlot::Int { count, .. } | NumSlot::Float { count, .. } => *count,
        }
    }

    /// Count one row without a value, for `COUNT(*)`.
    #[inline]
    pub fn count_row(&mut self) {
        match self {
            NumSlot::Int { count, .. } | NumSlot::Float { count, .. } => *count += 1,
        }
    }

    /// Add one integer value. Into a float slot it is widened, as the column's
    /// own coercion would; an integer slot keeps it exact.
    #[inline]
    pub fn push_int(&mut self, v: i64) {
        match self {
            NumSlot::Int {
                count,
                sum,
                min,
                max,
            } => {
                *count += 1;
                *sum += i128::from(v);
                if v < *min {
                    *min = v;
                }
                if v > *max {
                    *max = v;
                }
            }
            // Widening an integer into a float slot always succeeds.
            NumSlot::Float { .. } => {
                let _ = self.push_float(v as f64);
            }
        }
    }

    /// Add one floating-point value. Returns `false` if the slot is an integer
    /// slot: a fraction cannot be kept exactly there, and the caller must use
    /// the general path instead of guessing.
    #[inline]
    #[must_use]
    pub fn push_float(&mut self, v: f64) -> bool {
        match self {
            NumSlot::Float {
                count,
                sum,
                min,
                max,
            } => {
                *count += 1;
                *sum += v;
                *min = min.min(v);
                *max = max.max(v);
                true
            }
            NumSlot::Int { .. } => false,
        }
    }

    /// Add a batch of integer values (the columnar paths' tight loop).
    pub fn add_ints(&mut self, vals: &[i64]) {
        if vals.is_empty() {
            return;
        }
        match self {
            NumSlot::Int {
                count,
                sum,
                min,
                max,
            } => {
                *count += vals.len() as i64;
                *sum += vals.iter().map(|&x| i128::from(x)).sum::<i128>();
                *min = (*min).min(vals.iter().copied().min().unwrap_or(i64::MAX));
                *max = (*max).max(vals.iter().copied().max().unwrap_or(i64::MIN));
            }
            NumSlot::Float { .. } => {
                for &v in vals {
                    let _ = self.push_float(v as f64);
                }
            }
        }
    }

    /// Add a batch of floating-point values. Returns `false` for an integer
    /// slot (see [`push_float`](Self::push_float)).
    #[must_use]
    pub fn add_floats(&mut self, vals: &[f64]) -> bool {
        if vals.is_empty() {
            return true;
        }
        match self {
            NumSlot::Float {
                count,
                sum,
                min,
                max,
            } => {
                *count += vals.len() as i64;
                *sum += vals.iter().sum::<f64>();
                *min = min.min(vals.iter().copied().fold(f64::INFINITY, f64::min));
                *max = max.max(vals.iter().copied().fold(f64::NEG_INFINITY, f64::max));
                true
            }
            NumSlot::Int { .. } => false,
        }
    }

    /// Fold another partial (same column kind) into this one.
    pub fn merge(&mut self, other: &NumSlot) {
        match (self, other) {
            (
                NumSlot::Int {
                    count,
                    sum,
                    min,
                    max,
                },
                NumSlot::Int {
                    count: c,
                    sum: s,
                    min: lo,
                    max: hi,
                },
            ) => {
                *count += c;
                *sum += s;
                *min = (*min).min(*lo);
                *max = (*max).max(*hi);
            }
            (
                NumSlot::Float {
                    count,
                    sum,
                    min,
                    max,
                },
                NumSlot::Float {
                    count: c,
                    sum: s,
                    min: lo,
                    max: hi,
                },
            ) => {
                *count += c;
                *sum += s;
                *min = min.min(*lo);
                *max = max.max(*hi);
            }
            // Partials of one aggregate share a column, so the kinds always
            // match; a mismatch is a caller bug, not data.
            _ => debug_assert!(false, "NumSlot::merge across column kinds"),
        }
    }

    /// The aggregate's result, with the same types and rounding as the general
    /// aggregator: an integer `SUM` is an exact `DECIMAL` (MySQL widens it,
    /// because the total can exceed the column's range), an integer `AVG` an
    /// exact `DECIMAL` with `div_precision_increment` extra digits, and `MIN`/
    /// `MAX` keep the column's type. `COUNT(*)` reads the rows counted with
    /// [`count_row`](Self::count_row).
    pub fn finish(&self, func: AggFunc) -> Value {
        let count = self.count();
        if matches!(func, AggFunc::Count | AggFunc::CountStar) {
            return Value::Int(count);
        }
        if count == 0 {
            return Value::Null;
        }
        match (*self, func) {
            (NumSlot::Int { sum, .. }, AggFunc::Sum) => Value::Decimal(sum, 0),
            (NumSlot::Int { sum, .. }, AggFunc::Avg) => {
                exact_avg(sum, 0, count).unwrap_or(Value::Float(sum as f64 / count as f64))
            }
            (NumSlot::Int { min, .. }, AggFunc::Min) => Value::Int(min),
            (NumSlot::Int { max, .. }, AggFunc::Max) => Value::Int(max),
            (NumSlot::Float { sum, .. }, AggFunc::Sum) => Value::Float(sum),
            (NumSlot::Float { sum, .. }, AggFunc::Avg) => Value::Float(sum / count as f64),
            (NumSlot::Float { min, .. }, AggFunc::Min) => Value::Float(min),
            (NumSlot::Float { max, .. }, AggFunc::Max) => Value::Float(max),
            _ => Value::Null,
        }
    }
}

/// MySQL's exact average of an exact sum: the input's scale plus
/// `div_precision_increment`, rounded half away from zero -- the rule division
/// uses. `sum` is unscaled at `scale`. `None` if the scaled value overflows
/// `i128`; callers then fall back to a float average rather than fail the query.
pub fn exact_avg(sum: i128, scale: u8, count: i64) -> Option<Value> {
    let out_scale = scale.saturating_add(elyra_core::DIV_SCALE_INCREMENT);
    10i128
        .checked_pow(u32::from(elyra_core::DIV_SCALE_INCREMENT))
        .and_then(|f| sum.checked_mul(f))
        .and_then(|n| elyra_core::div_round_half_away(n, i128::from(count)))
        .map(|units| Value::Decimal(units, out_scale))
}

#[cfg(test)]
mod tests {
    use super::*;

    const BIG: i64 = 9_007_199_254_740_993; // 2^53 + 1: not representable in f64

    #[test]
    fn integer_sum_min_max_are_exact_past_2_pow_53() {
        let mut s = NumSlot::new(true);
        s.add_ints(&[BIG, 1]);
        assert_eq!(
            s.finish(AggFunc::Sum),
            Value::Decimal(9_007_199_254_740_994, 0)
        );
        assert_eq!(s.finish(AggFunc::Max), Value::Int(BIG));
        assert_eq!(s.finish(AggFunc::Min), Value::Int(1));
        assert_eq!(s.finish(AggFunc::Count), Value::Int(2));
    }

    #[test]
    fn integer_avg_is_an_exact_decimal_with_four_digits() {
        // MySQL: AVG over (10, 21, 2^53+1, 1) = 2251799813685256.2500
        let mut s = NumSlot::new(true);
        s.add_ints(&[10, 21, BIG, 1]);
        assert_eq!(
            s.finish(AggFunc::Avg),
            Value::Decimal(22_517_998_136_852_562_500, 4)
        );
        // Rounds half away from zero, like MySQL: 1/3 -> 0.3333, -2/3 -> -0.6667.
        let mut t = NumSlot::new(true);
        t.add_ints(&[1, 0, 0]);
        assert_eq!(t.finish(AggFunc::Avg), Value::Decimal(3333, 4));
        let mut u = NumSlot::new(true);
        u.add_ints(&[-1, -1, 0]);
        assert_eq!(u.finish(AggFunc::Avg), Value::Decimal(-6667, 4));
    }

    #[test]
    fn a_sum_past_i64_does_not_wrap() {
        let mut s = NumSlot::new(true);
        s.add_ints(&[i64::MAX, i64::MAX, i64::MAX]);
        assert_eq!(
            s.finish(AggFunc::Sum),
            Value::Decimal(3 * i128::from(i64::MAX), 0)
        );
    }

    #[test]
    fn batch_and_single_pushes_agree_and_merge_is_associative() {
        let vals = [5i64, -3, BIG, 0, i64::MIN, 42];
        let mut batch = NumSlot::new(true);
        batch.add_ints(&vals);
        let mut single = NumSlot::new(true);
        for &v in &vals {
            single.push_int(v);
        }
        assert_eq!(batch, single);
        let (mut a, mut b) = (NumSlot::new(true), NumSlot::new(true));
        a.add_ints(&vals[..2]);
        b.add_ints(&vals[2..]);
        a.merge(&b);
        assert_eq!(a, batch);
    }

    #[test]
    fn empty_slots_finish_as_null_and_zero_count() {
        for is_int in [true, false] {
            let s = NumSlot::new(is_int);
            for f in [AggFunc::Sum, AggFunc::Avg, AggFunc::Min, AggFunc::Max] {
                assert_eq!(s.finish(f), Value::Null, "{f:?} is_int={is_int}");
            }
            assert_eq!(s.finish(AggFunc::Count), Value::Int(0));
        }
    }

    #[test]
    fn a_float_into_an_integer_slot_is_refused_not_rounded() {
        let mut s = NumSlot::new(true);
        assert!(!s.push_float(1.5));
        assert!(!s.add_floats(&[1.5]));
        assert_eq!(s.count(), 0);
        // The reverse widens, as the column's coercion would.
        let mut f = NumSlot::new(false);
        f.push_int(3);
        assert!(f.push_float(0.5));
        assert_eq!(f.finish(AggFunc::Sum), Value::Float(3.5));
    }

    #[test]
    fn float_slots_keep_float_semantics() {
        let mut s = NumSlot::new(false);
        assert!(s.add_floats(&[1.0, 2.5, -4.0]));
        assert_eq!(s.finish(AggFunc::Sum), Value::Float(-0.5));
        assert_eq!(s.finish(AggFunc::Min), Value::Float(-4.0));
        assert_eq!(s.finish(AggFunc::Max), Value::Float(2.5));
        assert_eq!(s.finish(AggFunc::Avg), Value::Float(-0.5 / 3.0));
    }
}
