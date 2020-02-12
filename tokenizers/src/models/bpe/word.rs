use super::Pair;
use rand::{thread_rng, Rng};
use std::cmp::Ordering;
use std::collections::{BinaryHeap, HashMap};

#[derive(Debug, Eq)]
struct Merge {
    pos: usize,
    rank: u32,
    new_id: u32,
}

impl PartialEq for Merge {
    fn eq(&self, other: &Self) -> bool {
        self.rank == other.rank && self.pos == other.pos
    }
}

impl PartialOrd for Merge {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        // By manually implementing this, we make the containing BinaryHeap a
        // min-heap ordered first on the rank, and the pos otherwise
        if self.rank != other.rank {
            Some(other.rank.cmp(&self.rank))
        } else {
            Some(other.pos.cmp(&self.pos))
        }
    }
}

impl Ord for Merge {
    fn cmp(&self, other: &Self) -> Ordering {
        self.partial_cmp(other).unwrap()
    }
}

#[derive(Debug, Clone, Copy)]
struct Symbol {
    c: u32,
    prev: isize,
    next: isize,
    len: usize,
}
impl Symbol {
    /// Merges the current Symbol with the other one.
    /// In order to update prev/next, we consider Self to be the Symbol on the left,
    /// and other to be the next one on the right.
    pub fn merge_with(&mut self, other: &Self, new_c: u32) {
        self.c = new_c;
        self.len += other.len;
        self.next = other.next;
    }
}

#[derive(Clone, Default)]
pub(super) struct Word {
    symbols: Vec<Symbol>,
}

impl Word {
    pub(super) fn new() -> Self {
        Word { symbols: vec![] }
    }

    pub(super) fn add(&mut self, c: u32) {
        let (prev, next) = {
            let len = self.symbols.len() as isize;
            if let Some(last) = self.symbols.last_mut() {
                // Update `next` on the previous one
                last.next = len;
                (len - 1, -1)
            } else {
                (-1, -1)
            }
        };
        self.symbols.push(Symbol {
            c,
            prev,
            next,
            len: 1,
        });
    }

    pub(super) fn merge(&mut self, c1: u32, c2: u32, replacement: u32) -> Vec<(Pair, i32)> {
        let mut changes: Vec<(Pair, i32)> = vec![];
        let mut i = 0;
        loop {
            if i >= self.symbols.len() {
                break;
            }

            // Found a pair
            if self.symbols[i].c == c1 && i + 1 < self.symbols.len() && self.symbols[i + 1].c == c2
            {
                let first = self.symbols[i];
                let second = self.symbols[i + 1];

                // If there are other characters before the pair
                if i > 0 {
                    changes.push(((self.symbols[i - 1].c, first.c), -1));
                    changes.push(((self.symbols[i - 1].c, replacement), 1));
                }

                // Remove in place
                let new_s = Symbol {
                    c: replacement,
                    prev: first.prev,
                    next: second.next,
                    len: first.len + second.len,
                };
                self.symbols.insert(i, new_s); // Insert replacement before first char of pair
                self.symbols.remove(i + 1); // Remove first char of pair
                self.symbols.remove(i + 1); // And then the second

                // If there are other characters after the pair
                if i < self.symbols.len() - 1 {
                    changes.push(((second.c, self.symbols[i + 1].c), -1));
                    changes.push(((replacement, self.symbols[i + 1].c), 1));
                }
            }

            i += 1;
        }

        changes
    }

    pub(super) fn merge_all(&mut self, merges: &HashMap<Pair, (u32, u32)>, dropout: Option<f32>) {
        let dropout = dropout.unwrap_or(0.0);
        let mut queue = BinaryHeap::new();
        let mut skip = Vec::with_capacity(queue.len());

        let mut word = self.symbols.drain(..).map(Some).collect::<Vec<_>>();
        word.windows(2).enumerate().for_each(|(index, window)| {
            let pair = (window[0].unwrap().c, window[1].unwrap().c);
            if let Some(m) = merges.get(&pair) {
                queue.push(Merge {
                    pos: index,
                    rank: m.0,
                    new_id: m.1,
                });
            }
        });

        while let Some(top) = queue.pop() {
            if dropout > 0.0 && thread_rng().gen::<f32>() < dropout {
                skip.push(top);
            } else {
                // Re-insert the skipped elements
                skip.drain(..).for_each(|s| queue.push(s));

                if word[top.pos].is_some() {
                    //if let Some(current) = word[top.pos] {
                    // Do nothing if we are the last symbol
                    if word[top.pos].unwrap().next == -1 {
                        continue;
                    }

                    // Otherwise, let's merge
                    let next = word[top.pos].unwrap().next as usize;
                    let right = word[next].take().unwrap();
                    word[top.pos]
                        .as_mut()
                        .unwrap()
                        .merge_with(&right, top.new_id);
                    // Update prev on right's next
                    if right.next > -1 && (right.next as usize) < word.len() {
                        if let Some(next) = word[right.next as usize].as_mut() {
                            next.prev = top.pos as isize;
                        }
                    }

                    // Update queue with prev
                    let current = word[top.pos].unwrap();
                    if current.prev >= 0 {
                        let prev = current.prev as usize;
                        if let Some(prev_symbol) = word[prev] {
                            let new_pair = (prev_symbol.c, current.c);
                            if let Some((rank, new_id)) = merges.get(&new_pair) {
                                queue.push(Merge {
                                    pos: prev,
                                    rank: *rank,
                                    new_id: *new_id,
                                });
                            }
                        }
                    }

                    // Update queue with next
                    let next = current.next as usize;
                    if next < word.len() {
                        if let Some(next_symbol) = word[next] {
                            let new_pair = (current.c, next_symbol.c);
                            if let Some((rank, new_id)) = merges.get(&new_pair) {
                                queue.push(Merge {
                                    pos: top.pos,
                                    rank: *rank,
                                    new_id: *new_id,
                                });
                            }
                        }
                    }
                }
            }
        }

        // Filter out the removed symbols
        self.symbols = word
            .into_iter()
            .filter(|symbol| symbol.is_some())
            .map(|symbol| symbol.unwrap())
            .collect();

        println!("Done empty queue!: {:?}\t{:?}", queue, self.symbols);
    }

    pub(super) fn get_chars(&self) -> Vec<u32> {
        self.symbols.iter().map(|s| s.c).collect()
    }

    pub(super) fn get_offsets(&self) -> Vec<(usize, usize)> {
        let mut offsets = vec![];
        let mut pos = 0;
        for symbol in &self.symbols {
            offsets.push((pos, pos + symbol.len));
            pos += symbol.len;
        }
        offsets
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_merge() {
        // Let's say we have the word 'hello' and a word-to-id vocab that looks
        // like this: {'h': 0, 'e': 1, 'l': 2, 'o': 3}.
        let mut word = Word::new();
        word.add(0); // 'h'
        word.add(1); // 'e'
        word.add(2); // 'l'
        word.add(2); // 'l'
        word.add(3); // 'o'

        // We're going to perform a merge on the pair ('l', 'l') ~= (2, 2). Let's
        // say that 'll' has the ID of 4 in the updated word-to-id vocab.
        let changes = word.merge(2, 2, 4);

        // So the word should now look like this:
        assert_eq!(
            word.get_chars(),
            &[
                0u32, // 'h'
                1u32, // 'e'
                4u32, // 'll'
                3u32, // 'o'
            ]
        );

        // The return value `changes` will be used to update the pair counts during
        // training. This merge affects the counts for the pairs
        // ('e', 'l') ~= (1, 2),
        // ('e', 'll') ~= (1, 4),
        // ('l', 'o') ~= (2, 3), and
        // ('ll', 'o') ~= (4, 3).
        // So the changes should reflect that:
        assert_eq!(
            changes,
            &[
                ((1u32, 2u32), -1i32), // count for ('e', 'l') should be decreased by 1.
                ((1u32, 4u32), 1i32),  // count for ('e', 'll') should be increased by 1.
                ((2u32, 3u32), -1i32), // count for ('l', 'o') should be decreased by 1.
                ((4u32, 3u32), 1i32),  // count for ('ll', 'o') should be increased by 1.
            ]
        );
    }
}
