//! A compact, dependency-free HNSW index (Hierarchical Navigable Small
//! World) for approximate nearest-neighbour search.
//!
//! Built in one batch from a set of vectors, then queried many times. ElyraSQL
//! rebuilds it from the single-file data when the table changes, so there is
//! no incremental insert/delete to keep consistent — the graph is always a
//! faithful snapshot.

use std::cmp::Ordering;
use std::collections::BinaryHeap;

use crate::{distance, Metric};

const M: usize = 16; // neighbours per node (upper layers)
const M0: usize = 32; // neighbours per node (layer 0)
const EF_CONSTRUCTION: usize = 128;

/// Version-stamped visited set. Reused across searches so we never allocate
/// an O(N) buffer per `search_layer` call (that made builds O(N^2)).
struct Visited {
    stamp: Vec<u32>,
    epoch: u32,
}
impl Visited {
    fn new(n: usize) -> Self {
        Visited {
            stamp: vec![0; n],
            epoch: 0,
        }
    }
    fn clear(&mut self) {
        self.epoch = self.epoch.wrapping_add(1);
        if self.epoch == 0 {
            // Wrapped: reset stamps so stale epochs don't read as visited.
            self.stamp.iter_mut().for_each(|s| *s = 0);
            self.epoch = 1;
        }
    }
    /// Grow the stamp buffer if the index has more nodes than last time.
    fn ensure(&mut self, n: usize) {
        if self.stamp.len() < n {
            self.stamp.resize(n, 0);
        }
    }
    /// Mark visited; returns true if it was already visited.
    fn seen(&mut self, node: u32) -> bool {
        let s = &mut self.stamp[node as usize];
        if *s == self.epoch {
            true
        } else {
            *s = self.epoch;
            false
        }
    }
}

/// A built HNSW index over `dim`-dimensional vectors.
pub struct Hnsw {
    metric: Metric,
    dim: usize,
    /// Pool of reusable visited-sets so `search` does not allocate an O(N)
    /// buffer per query (checked out briefly under a lock, then returned).
    visited_pool: std::sync::Mutex<Vec<Visited>>,
    vectors: Vec<Vec<f32>>,
    /// `neighbors[node][level]` = adjacency list at that level.
    neighbors: Vec<Vec<Vec<u32>>>,
    entry: u32,
    max_level: usize,
}

/// Ordered by distance; used as a max-heap (farthest on top).
#[derive(Clone, Copy)]
struct Cand {
    dist: f32,
    node: u32,
}
impl PartialEq for Cand {
    fn eq(&self, o: &Self) -> bool {
        self.dist == o.dist
    }
}
impl Eq for Cand {}
impl PartialOrd for Cand {
    fn partial_cmp(&self, o: &Self) -> Option<Ordering> {
        Some(self.cmp(o))
    }
}
impl Ord for Cand {
    fn cmp(&self, o: &Self) -> Ordering {
        self.dist.partial_cmp(&o.dist).unwrap_or(Ordering::Equal)
    }
}

/// Tiny deterministic RNG (xorshift64*) so index builds are reproducible.
struct Rng(u64);
impl Rng {
    fn next_f32(&mut self) -> f32 {
        let mut x = self.0;
        x ^= x >> 12;
        x ^= x << 25;
        x ^= x >> 27;
        self.0 = x;
        ((x.wrapping_mul(0x2545F4914F6CDD1D) >> 40) as f32) / (1u32 << 24) as f32
    }
}

impl Hnsw {
    /// Build an index from `vectors`. All vectors must share `dim`.
    pub fn build(vectors: Vec<Vec<f32>>, dim: usize, metric: Metric) -> Self {
        let n = vectors.len();
        let mut idx = Hnsw {
            metric,
            dim,
            visited_pool: std::sync::Mutex::new(Vec::new()),
            vectors,
            neighbors: Vec::with_capacity(n),
            entry: 0,
            max_level: 0,
        };
        let mut rng = Rng(0x9E3779B97F4A7C15);
        let ml = 1.0 / (M as f32).ln();
        let mut visited = Visited::new(n);

        for node in 0..n {
            let level = ((-rng.next_f32().max(1e-9).ln()) * ml) as usize;
            idx.neighbors.push(vec![Vec::new(); level + 1]);
            if node == 0 {
                idx.entry = 0;
                idx.max_level = level;
                continue;
            }
            idx.insert(node as u32, level, &mut visited);
        }
        idx
    }

    fn dist_to(&self, node: u32, q: &[f32]) -> f32 {
        distance(&self.vectors[node as usize], q, self.metric).unwrap_or(f32::INFINITY)
    }

    fn insert(&mut self, q: u32, level: usize, visited: &mut Visited) {
        let qv = self.vectors[q as usize].clone();
        let mut ep = self.entry;

        // Descend from the top down to `level + 1` with a greedy ef = 1 walk.
        let mut l = self.max_level;
        while l > level {
            ep = self.greedy_descend(&qv, ep, l);
            if l == 0 {
                break;
            }
            l -= 1;
        }

        // Connect at each level from min(level, max_level) down to 0.
        let start = level.min(self.max_level);
        let mut entry_points = vec![ep];
        for lc in (0..=start).rev() {
            let mut found = self.search_layer(&qv, &entry_points, EF_CONSTRUCTION, lc, visited);
            let m = if lc == 0 { M0 } else { M };
            let selected = select_neighbors(&mut found, m);

            // Link q -> selected and selected -> q, pruning both sides.
            for &nb in &selected {
                self.neighbors[q as usize][lc].push(nb);
                self.neighbors[nb as usize][lc].push(q);
                self.prune(nb, lc, m);
            }
            self.prune(q, lc, m);

            entry_points = selected;
            if entry_points.is_empty() {
                entry_points = vec![ep];
            }
        }

        if level > self.max_level {
            self.max_level = level;
            self.entry = q;
        }
    }

    /// Greedy single-neighbour descent at one level.
    fn greedy_descend(&self, q: &[f32], entry: u32, level: usize) -> u32 {
        let mut cur = entry;
        let mut cur_d = self.dist_to(cur, q);
        loop {
            let mut improved = false;
            if let Some(list) = self.neighbors.get(cur as usize).and_then(|n| n.get(level)) {
                for &nb in list {
                    let d = self.dist_to(nb, q);
                    if d < cur_d {
                        cur_d = d;
                        cur = nb;
                        improved = true;
                    }
                }
            }
            if !improved {
                return cur;
            }
        }
    }

    /// Best-first search at one level; returns up to `ef` nearest as a
    /// max-heap (farthest on top).
    fn search_layer(
        &self,
        q: &[f32],
        entries: &[u32],
        ef: usize,
        level: usize,
        visited: &mut Visited,
    ) -> BinaryHeap<Cand> {
        visited.clear();
        let mut candidates: BinaryHeap<std::cmp::Reverse<Cand>> = BinaryHeap::new();
        let mut results: BinaryHeap<Cand> = BinaryHeap::new();

        for &e in entries {
            let d = self.dist_to(e, q);
            visited.seen(e);
            candidates.push(std::cmp::Reverse(Cand { dist: d, node: e }));
            results.push(Cand { dist: d, node: e });
        }
        while results.len() > ef {
            results.pop();
        }

        while let Some(std::cmp::Reverse(c)) = candidates.pop() {
            let worst = results.peek().map(|r| r.dist).unwrap_or(f32::INFINITY);
            if c.dist > worst && results.len() >= ef {
                break;
            }
            if let Some(list) = self
                .neighbors
                .get(c.node as usize)
                .and_then(|n| n.get(level))
            {
                for &nb in list {
                    if visited.seen(nb) {
                        continue;
                    }
                    let d = self.dist_to(nb, q);
                    let worst = results.peek().map(|r| r.dist).unwrap_or(f32::INFINITY);
                    if d < worst || results.len() < ef {
                        candidates.push(std::cmp::Reverse(Cand { dist: d, node: nb }));
                        results.push(Cand { dist: d, node: nb });
                        if results.len() > ef {
                            results.pop();
                        }
                    }
                }
            }
        }
        results
    }

    /// Keep only the `m` closest neighbours of `node` at `level`.
    fn prune(&mut self, node: u32, level: usize, m: usize) {
        let list = &self.neighbors[node as usize][level];
        if list.len() <= m {
            return;
        }
        let qv = self.vectors[node as usize].clone();
        let mut scored: Vec<Cand> = list
            .iter()
            .map(|&nb| Cand {
                dist: self.dist_to(nb, &qv),
                node: nb,
            })
            .collect();
        scored.sort_by(|a, b| a.dist.partial_cmp(&b.dist).unwrap_or(Ordering::Equal));
        scored.truncate(m);
        self.neighbors[node as usize][level] = scored.into_iter().map(|c| c.node).collect();
    }

    /// Approximate k nearest neighbours of `q`. Returns `(node, distance)`
    /// sorted ascending by distance. `ef` controls accuracy/speed.
    pub fn search(&self, q: &[f32], k: usize, ef: usize) -> Vec<(u32, f32)> {
        if self.vectors.is_empty() {
            return Vec::new();
        }
        let mut ep = self.entry;
        let mut l = self.max_level;
        while l > 0 {
            ep = self.greedy_descend(q, ep, l);
            l -= 1;
        }
        // Reuse a pooled visited-set instead of allocating O(N) per search.
        let mut visited = self
            .visited_pool
            .lock()
            .unwrap()
            .pop()
            .unwrap_or_else(|| Visited::new(self.vectors.len()));
        visited.ensure(self.vectors.len());
        let heap = self.search_layer(q, &[ep], ef.max(k), 0, &mut visited);
        self.visited_pool
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .push(visited);
        let mut out: Vec<(u32, f32)> = heap.into_iter().map(|c| (c.node, c.dist)).collect();
        out.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(Ordering::Equal));
        out.truncate(k);
        out
    }

    pub fn dim(&self) -> usize {
        self.dim
    }
}

fn select_neighbors(found: &mut BinaryHeap<Cand>, m: usize) -> Vec<u32> {
    let mut all: Vec<Cand> = found.drain().collect();
    all.sort_by(|a, b| a.dist.partial_cmp(&b.dist).unwrap_or(Ordering::Equal));
    all.truncate(m);
    all.into_iter().map(|c| c.node).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn brute_force(vectors: &[Vec<f32>], q: &[f32], k: usize) -> Vec<u32> {
        let mut d: Vec<(u32, f32)> = vectors
            .iter()
            .enumerate()
            .map(|(i, v)| (i as u32, distance(v, q, Metric::L2).unwrap()))
            .collect();
        d.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap());
        d.truncate(k);
        d.into_iter().map(|(i, _)| i).collect()
    }

    #[test]
    fn recall_is_high_vs_brute_force() {
        // 2000 random 16-d vectors.
        let mut rng = Rng(42);
        let dim = 16;
        let n = 2000;
        let vectors: Vec<Vec<f32>> = (0..n)
            .map(|_| (0..dim).map(|_| rng.next_f32()).collect())
            .collect();
        let index = Hnsw::build(vectors.clone(), dim, Metric::L2);

        let k = 10;
        let mut hits = 0;
        let mut total = 0;
        for _ in 0..100 {
            let q: Vec<f32> = (0..dim).map(|_| rng.next_f32()).collect();
            let exact: std::collections::HashSet<u32> =
                brute_force(&vectors, &q, k).into_iter().collect();
            let approx = index.search(&q, k, 64);
            for (node, _) in approx {
                if exact.contains(&node) {
                    hits += 1;
                }
                total += 1;
            }
        }
        let recall = hits as f32 / total as f32;
        assert!(recall > 0.85, "recall too low: {recall}");
    }

    #[test]
    fn finds_exact_nearest() {
        let vectors = vec![
            vec![1.0, 0.0],
            vec![0.0, 1.0],
            vec![0.9, 0.1],
            vec![0.0, 0.0],
        ];
        let index = Hnsw::build(vectors, 2, Metric::L2);
        let res = index.search(&[1.0, 0.0], 1, 32);
        assert_eq!(res[0].0, 0);
    }
}
