//! ElyraSQL analytics (OLAP) kernel.
//!
//! A mergeable, streaming group-aggregation engine. ElyraSQL routes large
//! aggregations here: the table is scanned in batches and each batch is
//! aggregated into a partial [`GroupAggregator`]; partials from parallel
//! workers are then [`merge`](GroupAggregator::merge)d. Because only group
//! state (not the rows) is retained, aggregation runs in memory proportional
//! to the number of groups, not the table size.

use indexmap::IndexMap;
use std::cmp::Ordering;
use std::collections::HashSet;
use std::hash::{BuildHasherDefault, Hasher};

/// Fast, non-cryptographic hasher (FxHash, as used by rustc/Firefox) for the
/// aggregation hash maps. The default `SipHash` is DoS-resistant but far slower
/// than we need for internal, trusted group keys; FxHash cuts the per-row
/// hashing cost several-fold, which dominates large `GROUP BY`.
#[derive(Default)]
pub struct FxHasher {
    hash: u64,
}

const FX_SEED: u64 = 0x51_7c_c1_b7_27_22_0a_95;

impl FxHasher {
    #[inline]
    fn add(&mut self, i: u64) {
        self.hash = (self.hash.rotate_left(5) ^ i).wrapping_mul(FX_SEED);
    }
}

impl Hasher for FxHasher {
    #[inline]
    fn finish(&self) -> u64 {
        self.hash
    }
    #[inline]
    fn write(&mut self, mut bytes: &[u8]) {
        while bytes.len() >= 8 {
            self.add(u64::from_le_bytes(bytes[..8].try_into().unwrap()));
            bytes = &bytes[8..];
        }
        if bytes.len() >= 4 {
            self.add(u32::from_le_bytes(bytes[..4].try_into().unwrap()) as u64);
            bytes = &bytes[4..];
        }
        for &b in bytes {
            self.add(b as u64);
        }
    }
}

type FxBuild = BuildHasherDefault<FxHasher>;

use elyra_core::Value;

/// Aggregate function kinds.
#[derive(Clone, Copy, PartialEq, Debug)]
pub enum AggFunc {
    CountStar,
    Count,
    Sum,
    Avg,
    Min,
    Max,
    GroupConcat,
}

/// One aggregate to compute: function, optional argument column, DISTINCT.
#[derive(Clone, Debug)]
pub struct AggSpec {
    pub func: AggFunc,
    pub arg_col: Option<usize>,
    pub distinct: bool,
    /// GROUP_CONCAT separator (default ",").
    pub separator: Option<String>,
}

#[derive(Clone)]
struct Acc {
    count: i64,
    sum: f64,
    sum_is_int: bool,
    extreme: Option<Value>,
    distinct: HashSet<String>,
    concat: Vec<String>,
    /// Exact decimal running sum (unscaled) and its scale.
    dsum: i128,
    dscale: u8,
    has_decimal: bool,
    float_sum: bool,
}

impl Acc {
    fn new() -> Self {
        Acc {
            count: 0,
            sum: 0.0,
            sum_is_int: true,
            extreme: None,
            distinct: HashSet::new(),
            concat: Vec::new(),
            dsum: 0,
            dscale: 0,
            has_decimal: false,
            float_sum: false,
        }
    }
}

/// Streaming, mergeable group aggregator.
pub struct GroupAggregator {
    group_cols: Vec<usize>,
    aggs: Vec<AggSpec>,
    /// key -> (sample row for group columns, per-agg accumulators), in
    /// first-seen insertion order (IndexMap), so there is no separate order
    /// vector and each group key is allocated exactly once.
    groups: IndexMap<Vec<u8>, (Vec<Value>, Vec<Acc>), FxBuild>,
    /// Cap on distinct groups (0 = unlimited); protects against OOM.
    max_groups: usize,
    /// Set once the cap is hit; the aggregation is then incomplete.
    overflow: bool,
    /// Reused scratch buffer for building the group key of each fed row.
    key_buf: Vec<u8>,
}

/// Default distinct-group cap, from `ELYRASQL_GROUP_MAX_GROUPS`
/// (default 5,000,000; 0 disables).
pub fn default_max_groups() -> usize {
    use std::sync::OnceLock;
    static N: OnceLock<usize> = OnceLock::new();
    *N.get_or_init(|| {
        std::env::var("ELYRASQL_GROUP_MAX_GROUPS")
            .ok()
            .and_then(|s| s.parse::<usize>().ok())
            .unwrap_or(5_000_000)
    })
}

impl GroupAggregator {
    pub fn new(group_cols: Vec<usize>, aggs: Vec<AggSpec>) -> Self {
        GroupAggregator {
            group_cols,
            aggs,
            groups: IndexMap::default(),
            max_groups: default_max_groups(),
            overflow: false,
            key_buf: Vec::new(),
        }
    }

    /// Whether the distinct-group cap was exceeded (result would be incomplete).
    pub fn overflowed(&self) -> bool {
        self.overflow
    }

    /// Feed one row into the aggregator. If the distinct-group cap is reached and
    /// the row belongs to a new group, the aggregation is marked overflowed and
    /// the row is dropped.
    pub fn feed(&mut self, row: &[Value]) {
        if !self.try_feed(row) {
            self.overflow = true;
        }
    }

    /// Feed one row, returning `false` (without recording overflow) when the row
    /// belongs to a *new* group and the cap is already reached -- so a caller
    /// running a single-pass hybrid aggregation can spill just that row to disk
    /// instead of losing it. Rows for groups already resident are always
    /// accepted.
    pub fn try_feed(&mut self, row: &[Value]) -> bool {
        // Build the key into a reused buffer -- existing groups (the common
        // case) are then found and updated in a single lookup with no per-row
        // allocation.
        group_key_into(&self.group_cols, row, &mut self.key_buf);
        if let Some(entry) = self.groups.get_mut(self.key_buf.as_slice()) {
            for (i, spec) in self.aggs.iter().enumerate() {
                let v = spec
                    .arg_col
                    .map(|c| row.get(c).cloned().unwrap_or(Value::Null));
                update(&mut entry.1[i], spec.func, v, spec.distinct);
            }
            return true;
        }
        // New group. At/over the cap, refuse to create it (bounding memory).
        if self.max_groups > 0 && self.groups.len() >= self.max_groups {
            return false;
        }
        let mut accs: Vec<Acc> = self.aggs.iter().map(|_| Acc::new()).collect();
        for (i, spec) in self.aggs.iter().enumerate() {
            let v = spec
                .arg_col
                .map(|c| row.get(c).cloned().unwrap_or(Value::Null));
            update(&mut accs[i], spec.func, v, spec.distinct);
        }
        self.groups
            .insert(self.key_buf.clone(), (row.to_vec(), accs));
        true
    }

    /// Merge another partial aggregator (from a parallel worker) into this one.
    /// Consumes `other`, moving each group's sample row and accumulators instead
    /// of cloning them -- important for high-cardinality GROUP BY where a clone
    /// would copy every group again.
    pub fn merge(&mut self, other: GroupAggregator) {
        self.overflow |= other.overflow;
        for (key, (sample, accs)) in other.groups {
            match self.groups.get_mut(&key) {
                Some((_, self_accs)) => {
                    for (i, (a, b)) in self_accs.iter_mut().zip(accs).enumerate() {
                        merge_acc(a, b, self.aggs[i].func);
                    }
                }
                None => {
                    if self.max_groups > 0 && self.groups.len() >= self.max_groups {
                        self.overflow = true;
                        continue;
                    }
                    self.groups.insert(key, (sample, accs));
                }
            }
        }
    }

    /// Seed the result of a bare `COUNT(*)` (no GROUP BY, single CountStar
    /// aggregate) directly from a known row count, avoiding an O(n) feed. The
    /// caller must ensure this plan is exactly `COUNT(*)`.
    pub fn seed_count_star(&mut self, n: u64) {
        let mut acc = Acc::new();
        acc.count = n as i64;
        self.groups.insert(Vec::new(), (Vec::new(), vec![acc]));
    }

    /// Number of groups accumulated.
    pub fn len(&self) -> usize {
        self.groups.len()
    }

    pub fn is_empty(&self) -> bool {
        self.groups.is_empty()
    }

    /// Finalise into `(group sample row, aggregate results)` pairs, in first-
    /// seen group order.
    pub fn into_groups(self) -> Vec<(Vec<Value>, Vec<Value>)> {
        let GroupAggregator { aggs, groups, .. } = self;
        groups
            .into_iter()
            .map(|(_k, (sample, accs))| {
                let results = accs
                    .iter()
                    .zip(&aggs)
                    .map(|(acc, spec)| finish(acc, spec))
                    .collect();
                (sample, results) // move the sample row -- no clone
            })
            .collect()
    }

    /// Results for an aggregate over zero rows (e.g. `COUNT(*)` -> 0).
    pub fn empty_result(&self) -> Vec<Value> {
        self.aggs.iter().map(|s| finish(&Acc::new(), s)).collect()
    }
}

/// Compact, collision-free binary encoding of a group's key columns. Much
/// cheaper than formatting each value with `Debug` (hot path for GROUP BY).
/// Encode the group key into a caller-owned buffer (cleared first), so the hot
/// path can reuse one allocation and look up existing groups without copying.
fn group_key_into(cols: &[usize], row: &[Value], out: &mut Vec<u8>) {
    out.clear();
    for &i in cols {
        match row.get(i) {
            None | Some(Value::Null) => out.push(0),
            Some(Value::Int(v)) => {
                out.push(1);
                out.extend_from_slice(&v.to_le_bytes());
            }
            Some(Value::Bool(b)) => {
                out.push(2);
                out.push(*b as u8);
            }
            Some(Value::Float(f)) => {
                out.push(3);
                out.extend_from_slice(&elyra_core::canonical_f64_bits(*f).to_le_bytes());
            }
            Some(Value::Text(s)) | Some(Value::Json(s)) => {
                // Case-fold so GROUP BY matches the default collation.
                let s = elyra_core::fold(s);
                out.push(4);
                out.extend_from_slice(&(s.len() as u32).to_le_bytes());
                out.extend_from_slice(s.as_bytes());
            }
            Some(other) => {
                out.push(5);
                let s = format!("{other:?}");
                out.extend_from_slice(&(s.len() as u32).to_le_bytes());
                out.extend_from_slice(s.as_bytes());
            }
        }
    }
}

fn update(acc: &mut Acc, func: AggFunc, val: Option<Value>, distinct: bool) {
    match func {
        AggFunc::CountStar => acc.count += 1,
        AggFunc::Count => {
            if let Some(v) = val {
                if !v.is_null() {
                    if distinct {
                        if acc
                            .distinct
                            .insert(String::from_utf8_lossy(&v.collation_key()).into_owned())
                        {
                            acc.count += 1;
                        }
                    } else {
                        acc.count += 1;
                    }
                }
            }
        }
        AggFunc::Sum | AggFunc::Avg => {
            if let Some(v) = val {
                if let Some(n) = num(&v) {
                    if distinct
                        && !acc
                            .distinct
                            .insert(String::from_utf8_lossy(&v.collation_key()).into_owned())
                    {
                        return;
                    }
                    acc.sum += n;
                    acc.count += 1;
                    match &v {
                        Value::Int(i) => {
                            acc.dsum = acc.dsum.saturating_add(
                                (*i as i128).saturating_mul(10i128.pow(acc.dscale as u32)),
                            );
                        }
                        Value::Decimal(u, s) => {
                            acc.sum_is_int = false;
                            let s = *s;
                            if s > acc.dscale {
                                acc.dsum =
                                    acc.dsum.saturating_mul(10i128.pow((s - acc.dscale) as u32));
                                acc.dscale = s;
                            }
                            acc.dsum = acc.dsum.saturating_add(
                                u.saturating_mul(10i128.pow((acc.dscale - s) as u32)),
                            );
                            acc.has_decimal = true;
                        }
                        _ => {
                            acc.sum_is_int = false;
                            acc.float_sum = true;
                        }
                    }
                }
            }
        }
        AggFunc::GroupConcat => {
            if let Some(v) = val {
                if !v.is_null() {
                    let s = v.to_wire_string().unwrap_or_default();
                    if distinct {
                        if acc.distinct.insert(s.clone()) {
                            acc.concat.push(s);
                        }
                    } else {
                        acc.concat.push(s);
                    }
                }
            }
        }
        AggFunc::Min | AggFunc::Max => {
            if let Some(v) = val {
                if v.is_null() {
                    return;
                }
                let replace = match &acc.extreme {
                    None => true,
                    Some(cur) => {
                        let ord = value_cmp(&v, cur);
                        (func == AggFunc::Min && ord == Ordering::Less)
                            || (func == AggFunc::Max && ord == Ordering::Greater)
                    }
                };
                if replace {
                    acc.extreme = Some(v);
                }
            }
        }
    }
}

fn merge_acc(a: &mut Acc, b: Acc, func: AggFunc) {
    a.count += b.count;
    a.sum += b.sum;
    a.sum_is_int = a.sum_is_int && b.sum_is_int;
    // Merge exact decimal sums, aligning to the larger scale.
    let target = a.dscale.max(b.dscale);
    let av = a
        .dsum
        .saturating_mul(10i128.pow((target - a.dscale) as u32));
    let bv = b
        .dsum
        .saturating_mul(10i128.pow((target - b.dscale) as u32));
    a.dsum = av.saturating_add(bv);
    a.dscale = target;
    a.has_decimal |= b.has_decimal;
    a.float_sum |= b.float_sum;
    a.concat.extend(b.concat);
    for d in b.distinct {
        a.distinct.insert(d);
    }
    if let Some(bv) = b.extreme {
        a.extreme = Some(match a.extreme.take() {
            None => bv,
            Some(av) => {
                let keep_b = match func {
                    AggFunc::Min => value_cmp(&bv, &av) == Ordering::Less,
                    _ => value_cmp(&bv, &av) == Ordering::Greater, // MAX (others irrelevant)
                };
                if keep_b {
                    bv
                } else {
                    av
                }
            }
        });
    }
}

fn finish(acc: &Acc, spec: &AggSpec) -> Value {
    match spec.func {
        AggFunc::GroupConcat => {
            if acc.concat.is_empty() {
                Value::Null
            } else {
                let sep = spec.separator.as_deref().unwrap_or(",");
                Value::Text(acc.concat.join(sep))
            }
        }
        AggFunc::CountStar | AggFunc::Count => Value::Int(acc.count),
        AggFunc::Sum => {
            if acc.count == 0 {
                Value::Null
            } else if acc.has_decimal && !acc.float_sum {
                Value::Decimal(acc.dsum, acc.dscale)
            } else if acc.sum_is_int && acc.sum.fract() == 0.0 {
                Value::Int(acc.sum as i64)
            } else {
                Value::Float(acc.sum)
            }
        }
        AggFunc::Avg => {
            if acc.count == 0 {
                Value::Null
            } else {
                Value::Float(acc.sum / acc.count as f64)
            }
        }
        AggFunc::Min | AggFunc::Max => acc.extreme.clone().unwrap_or(Value::Null),
    }
}

fn num(v: &Value) -> Option<f64> {
    v.as_f64()
}

/// Total order over values (NULL first). Delegates to the shared
/// [`Value::total_cmp`] so date/decimal ordering is consistent everywhere.
pub fn value_cmp(a: &Value, b: &Value) -> Ordering {
    a.total_cmp(b)
}
