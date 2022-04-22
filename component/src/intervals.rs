/*Copyright 2022 Prediktor AS

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.*/

use log::debug;
use mbei_core::graph::{Edge};
#[cfg(test)]
use mbei_core::graph::{edges_from_deltas, Delta, DeltaType, Node};

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct ReprocessInterval {
    pub(crate) from: u64,
    pub(crate) to: Option<u64>,
}

impl ReprocessInterval {
    fn subtract(&self, other_from: u64, other_to: &Option<u64>) -> Vec<ReprocessInterval> {
        let mut result = vec![];
        if self.from < other_from {
            result.push(ReprocessInterval {
                from: self.from,
                to: Some(other_from - 1),
            });
            if other_to.is_none() {
                ();
            } else if self.to.is_none() {
                result.push(ReprocessInterval {
                    from: other_to.unwrap() + 1,
                    to: None,
                });
            } else if other_to.unwrap() < self.to.unwrap() {
                result.push(ReprocessInterval {
                    from: other_to.unwrap() + 1,
                    to: self.to,
                });
            } else {
                //We know that other is finite and ends before self.
                ();
            }
        } else {
            // self is starting equal to or after other
            if other_to.is_none() {
                //If the other has no end, then it is contained in other, and there is nothing left.
                // | other start | self start -- other never ends
                ();
            } else if self.to.is_none() {
                // | other start | self start | other end -- self never ends
                result.push(ReprocessInterval {
                    from: other_to.unwrap() + 1,
                    to: None,
                });
            } else if self.to.unwrap() <= other_to.unwrap() {
                // Again self is conained in other, this time in a finite setting
                // | other start | self start self end | other end |
                ();
            } else if self.from < other_to.unwrap() {
                // | other start | self start | other end | self end
                result.push(ReprocessInterval {
                    from: other_to.unwrap() + 1,
                    to: self.to,
                });
            } else {
                //
                ();
            }
        }
        result
    }

    fn overlaps(&self, other_from: u64, other_to: &Option<u64>) -> bool {
        if self.from <= other_from && (self.to.is_none() || self.to.unwrap() > other_from) {
            return true;
        }
        if other_from <= self.from && (other_to.is_none() || other_to.unwrap() > self.from) {
            return true;
        }
        return false;
    }
}

/// All edges here should be the same edge, but with different intervals.
/// Note that we cover two cases here: first the case where we add deltas, second the case where we remove deltas.
pub(crate) fn find_intervals_to_reprocess(
    updated_edges_with_timestamp: &Vec<Edge>,
    existing_edges_with_timestamp: &Vec<Edge>,
) -> Vec<ReprocessInterval> {
    //Added intervals that also overlap some existing interval
    let mut added_intervals = vec![];
    //Lost interval that also overlaps some updated interval
    let mut lost_intervals = vec![];
    for upd in updated_edges_with_timestamp {
        let first_upd_reprocess = ReprocessInterval {
            from: upd.from_timestamp.unwrap(),
            to: upd.to_timestamp.clone(),
        };
        let mut upd_reprocess_vec = vec![first_upd_reprocess];
        for ex in existing_edges_with_timestamp {
            let mut new_upd_reprocess_vec = vec![];
            for upd_reprocess in upd_reprocess_vec {
                if upd_reprocess.overlaps(ex.from_timestamp.unwrap(), &ex.to_timestamp) {
                    let mut subtraction_result =
                        upd_reprocess.subtract(ex.from_timestamp.unwrap(), &ex.to_timestamp);
                    new_upd_reprocess_vec.append(&mut subtraction_result);
                } else {
                    new_upd_reprocess_vec.push(upd_reprocess);
                }
            }

            upd_reprocess_vec = new_upd_reprocess_vec;
        }
        for upd_reprocess in upd_reprocess_vec {
            added_intervals.push(upd_reprocess);
        }
    }

    for ex in existing_edges_with_timestamp {
        let first_ex_reprocess = ReprocessInterval {
            from: ex.from_timestamp.unwrap(),
            to: ex.to_timestamp.clone(),
        };
        let mut ex_reprocess_vec = vec![first_ex_reprocess];
        for upd in updated_edges_with_timestamp {
            let mut new_ex_reprocess_vec = vec![];
            for ex_reprocess in ex_reprocess_vec {
                if ex_reprocess.overlaps(upd.from_timestamp.unwrap(), &upd.to_timestamp) {
                    let mut subtraction_result =
                        ex_reprocess.subtract(upd.from_timestamp.unwrap(), &upd.to_timestamp);
                    new_ex_reprocess_vec.append(&mut subtraction_result);
                } else {
                    new_ex_reprocess_vec.push(ex_reprocess);
                }
            }
            ex_reprocess_vec = new_ex_reprocess_vec;
        }
        for ex_reprocess in ex_reprocess_vec {
            lost_intervals.push(ex_reprocess);
        }
    }
    added_intervals.append(&mut lost_intervals);
    let reprocess_intervals = find_non_redundant_intervals(added_intervals);
    debug!("Reprocess: {:?}", &reprocess_intervals);
    reprocess_intervals
}

pub(crate) fn find_non_redundant_intervals(
    mut reprocess_intervals: Vec<ReprocessInterval>,
) -> Vec<ReprocessInterval> {
    if reprocess_intervals.is_empty() {
        return reprocess_intervals;
    }
    reprocess_intervals.sort_by_key(|ri| ri.from);
    let mut new_intervals = vec![];
    let mut i = 0;
    'outer: loop {
        let mut this_interval = reprocess_intervals.get(i).unwrap().clone();
        if (i + 1) >= reprocess_intervals.len() || this_interval.to.is_none() {
            new_intervals.push(this_interval.clone());
            break;
        }
        assert!(this_interval.to.is_some());
        for j in (i + 1)..reprocess_intervals.len() {
            let next_interval = reprocess_intervals.get(j).unwrap();
            if next_interval.from <= this_interval.to.unwrap() {
                if next_interval.to.is_none() || (next_interval.to.is_some() && this_interval.to.unwrap() < next_interval.to.unwrap()) {
                    this_interval.to = next_interval.to;
                }
                if (j + 1) >= reprocess_intervals.len() || this_interval.to.is_none() {
                    new_intervals.push(this_interval);
                    break 'outer;
                } else {
                    i = j + 1;
                }
            } else {
                new_intervals.push(this_interval.clone());
                i = j;
                break;
            }
        }
    }
    new_intervals
}

#[test]
fn test_zero_existing_intervals() {
    let d = Delta {
        src: Node::material_instance_node("MyBarrel0", "Barrel"),
        trg: Node::object_instance_node("MyStampAssembly", "StampAssembly"),
        edge_type: "At".to_string(),
        delta_type: DeltaType::Addition,
        timestamp: 4,
    };
    let edges = edges_from_deltas(&vec![&d]);
    let to_reprocess = find_intervals_to_reprocess(&edges, &vec![]);
    assert_eq!(vec![ReprocessInterval { from: 4, to: None }], to_reprocess);
}

#[test]
fn test_removed_earlier() {
    let my_barrel = Node::material_instance_node("MyBarrel0", "Barrel");
    let my_platform = Node::object_instance_node("MyPlatform0", "Platform");
    let new_deltas = vec![Delta {
        src: my_barrel.clone(),
        trg: my_platform.clone(),
        edge_type: "At".to_string(),
        timestamp: 2,
        delta_type: DeltaType::Removal,
    }];
    let existing_deltas = vec![
        Delta {
            src: my_barrel.clone(),
            trg: my_platform.clone(),
            edge_type: "At".to_string(),
            timestamp: 3,
            delta_type: DeltaType::Removal,
        },
        Delta {
            src: my_barrel.clone(),
            trg: my_platform.clone(),
            edge_type: "At".to_string(),
            timestamp: 0,
            delta_type: DeltaType::Addition,
        },
    ];
    let mut borrow_updated_deltas = vec![];
    let mut borrow_existing_deltas = vec![];
    for d in &existing_deltas {
        borrow_updated_deltas.push(d);
        borrow_existing_deltas.push(d);
    }
    for d in &new_deltas {
        borrow_updated_deltas.push(d);
    }

    let updated_edges_with_timestamp = edges_from_deltas(&borrow_updated_deltas);
    let existing_edges_with_timestamp = edges_from_deltas(&borrow_existing_deltas);
    let reprocess = find_intervals_to_reprocess(
        &updated_edges_with_timestamp,
        &existing_edges_with_timestamp,
    );
    let expected = vec![ReprocessInterval {
        from: 3,
        to: Some(3),
    }];
    assert_eq!(reprocess, expected);
}

#[test]
fn test_been_there_earlier() {
    let my_barrel = Node::material_instance_node("MyBarrel0", "Barrel");
    let my_platform = Node::object_instance_node("MyPlatform0", "Platform");
    let new_deltas = vec![Delta {
        src: my_barrel.clone(),
        trg: my_platform.clone(),
        edge_type: "At".to_string(),
        timestamp: 3,
        delta_type: DeltaType::Addition,
    }];
    let existing_deltas = vec![
        Delta {
            src: my_barrel.clone(),
            trg: my_platform.clone(),
            edge_type: "At".to_string(),
            timestamp: 2,
            delta_type: DeltaType::Removal,
        },
        Delta {
            src: my_barrel.clone(),
            trg: my_platform.clone(),
            edge_type: "At".to_string(),
            timestamp: 0,
            delta_type: DeltaType::Addition,
        },
    ];
    let mut borrow_updated_deltas = vec![];
    let mut borrow_existing_deltas = vec![];
    for d in &existing_deltas {
        borrow_updated_deltas.push(d);
        borrow_existing_deltas.push(d);
    }
    for d in &new_deltas {
        borrow_updated_deltas.push(d);
    }

    let updated_edges_with_timestamp = edges_from_deltas(&borrow_updated_deltas);
    let existing_edges_with_timestamp = edges_from_deltas(&borrow_existing_deltas);
    let reprocess = find_intervals_to_reprocess(
        &updated_edges_with_timestamp,
        &existing_edges_with_timestamp,
    );
    let expected = vec![ReprocessInterval {
        from: 3,
        to: None,
    }];
    assert_eq!(reprocess, expected);
}

#[test]
fn test_subtract_easy1_correct() {
    let repr = ReprocessInterval {
        from: 0,
        to: Some(3),
    };
    let res = repr.subtract(0, &Some(2));

    assert_eq!(res.len(), 1);
    let res0 = res.get(0).unwrap();
    assert_eq!(res0.from, 3);
    assert_eq!(res0.to, Some(3));
}

#[test]
fn test_subtract_easy2_correct() {
    let repr = ReprocessInterval {
        from: 1,
        to: Some(3),
    };
    let res = repr.subtract(0, &Some(2));

    assert_eq!(res.len(), 1);
    let res0 = res.get(0).unwrap();
    assert_eq!(res0.from, 3);
    assert_eq!(res0.to, Some(3));
}

#[test]
fn test_subtract_easy3_correct() {
    let repr = ReprocessInterval {
        from: 1,
        to: Some(3),
    };
    let res = repr.subtract(0, &None);

    assert_eq!(res.len(), 0);
}

#[test]
fn test_subtract_easy4_correct() {
    let repr = ReprocessInterval { from: 1, to: None };
    let res = repr.subtract(0, &Some(2));

    assert_eq!(res.len(), 1);
    let res0 = res.get(0).unwrap();
    assert_eq!(res0.from, 3);
    assert_eq!(res0.to, None);
}

#[test]
fn test_subtract_easy5_correct() {
    let repr = ReprocessInterval { from: 1, to: None };
    let res = repr.subtract(2, &None);

    assert_eq!(res.len(), 1);
    let res0 = res.get(0).unwrap();
    assert_eq!(res0.from, 1);
    assert_eq!(res0.to, Some(1));
}

#[test]
fn test_subtract_two_results1_correct() {
    let repr = ReprocessInterval {
        from: 0,
        to: Some(3),
    };
    let res = repr.subtract(1, &Some(2));
    assert_eq!(res.len(), 2, "Should have two results");
    let res0 = res.get(0).unwrap();
    assert_eq!(res0.from, 0);
    assert_eq!(res0.to, Some(0));

    let res1 = res.get(1).unwrap();
    assert_eq!(res1.from, 3);
    assert_eq!(res1.to, Some(3));
}

#[test]
fn test_subtract_two_results2_correct() {
    let repr = ReprocessInterval { from: 0, to: None };
    let res = repr.subtract(1, &Some(2));
    assert_eq!(res.len(), 2, "Should have two results");
    let res0 = res.get(0).unwrap();
    assert_eq!(res0.from, 0);
    assert_eq!(res0.to, Some(0));

    let res1 = res.get(1).unwrap();
    assert_eq!(res1.from, 3);
    assert_eq!(res1.to, None);
}

#[test]
fn test_find_non_redundant_intervals_all_overlapping() {
    let repr1 = ReprocessInterval { from: 1, to: Some(2) };
    let repr2 = ReprocessInterval { from: 2, to: Some(5) };
    let repr3 = ReprocessInterval { from: 3, to: None };

    let result = find_non_redundant_intervals(vec![repr1, repr2, repr3]);
    assert_eq!(result, vec![ReprocessInterval { from: 1, to: None }]);
}

#[test]
fn test_find_non_redundant_intervals_last_interval_contained() {
    let repr1 = ReprocessInterval { from: 1, to: Some(2) };
    let repr2 = ReprocessInterval { from: 2, to: Some(6) };
    let repr3 = ReprocessInterval { from: 3, to: Some(5) };

    let result = find_non_redundant_intervals(vec![repr1, repr2, repr3]);
    assert_eq!(result, vec![ReprocessInterval { from: 1, to: Some(6) }]);
}

#[test]
fn test_find_non_redundant_intervals_middle_interval_open() {
    let repr1 = ReprocessInterval { from: 1, to: Some(2) };
    let repr2 = ReprocessInterval { from: 2, to: None };
    let repr3 = ReprocessInterval { from: 3, to: Some(5) };

    let result = find_non_redundant_intervals(vec![repr1, repr2, repr3]);
    assert_eq!(result, vec![ReprocessInterval { from: 1, to: None }]);
}

#[test]
fn test_find_non_redundant_intervals_no_overlap() {
    let repr1 = ReprocessInterval { from: 1, to: Some(2) };
    let repr2 = ReprocessInterval { from: 8, to: None };
    let repr3 = ReprocessInterval { from: 3, to: Some(5) };

    let result = find_non_redundant_intervals(vec![repr1.clone(), repr2.clone(), repr3.clone()]);
    assert_eq!(result, vec![repr1, repr3, repr2]);
}

#[test]
fn test_find_non_redundant_intervals_two_open() {
    let repr1 = ReprocessInterval { from: 1, to: Some(2) };
    let repr2 = ReprocessInterval { from: 8, to: None };
    let repr3 = ReprocessInterval { from: 3, to: None };

    let result = find_non_redundant_intervals(vec![repr1.clone(), repr2, repr3]);
    assert_eq!(result, vec![repr1, ReprocessInterval{from:3, to:None}]);
}

#[test]
fn test_find_non_redundant_intervals_two_overlapping() {
    let repr1 = ReprocessInterval { from: 1, to: Some(2) };
    let repr2 = ReprocessInterval { from: 2, to: None };

    let result = find_non_redundant_intervals(vec![repr1.clone(), repr2]);
    assert_eq!(result, vec![ReprocessInterval{from:1, to:None}]);
}

#[test]
fn test_find_non_redundant_intervals_only_one() {
    let repr1 = ReprocessInterval { from: 1, to: Some(1) };

    let result = find_non_redundant_intervals(vec![repr1]);
    assert_eq!(result, vec![ReprocessInterval{from:1, to:Some(1)}]);
}