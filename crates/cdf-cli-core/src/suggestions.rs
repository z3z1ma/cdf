use std::collections::BTreeSet;

const MAX_SUGGESTIONS: usize = 3;

pub fn nearest(input: &str, candidates: impl IntoIterator<Item = String>) -> Vec<String> {
    let input = input.trim();
    if input.is_empty() {
        return Vec::new();
    }

    let normalized_input = input.to_ascii_lowercase();
    let mut seen = BTreeSet::new();
    let mut ranked = Vec::new();
    for candidate in candidates {
        if !seen.insert(candidate.clone()) {
            continue;
        }
        let normalized_candidate = candidate.to_ascii_lowercase();
        if normalized_candidate == normalized_input {
            continue;
        }
        let distance = edit_distance(&normalized_input, &normalized_candidate);
        if distance <= max_distance(input, &candidate) {
            let length_delta = input.chars().count().abs_diff(candidate.chars().count());
            ranked.push((distance, length_delta, candidate));
        }
    }

    ranked.sort_by(|left, right| {
        left.0
            .cmp(&right.0)
            .then(left.1.cmp(&right.1))
            .then(left.2.cmp(&right.2))
    });
    ranked
        .into_iter()
        .take(MAX_SUGGESTIONS)
        .map(|(_, _, candidate)| candidate)
        .collect()
}

fn max_distance(input: &str, candidate: &str) -> usize {
    let width = input.chars().count().max(candidate.chars().count());
    match width {
        0..=4 => 1,
        5..=8 => 2,
        _ => 3,
    }
}

fn edit_distance(left: &str, right: &str) -> usize {
    let right = right.chars().collect::<Vec<_>>();
    let mut previous = (0..=right.len()).collect::<Vec<_>>();
    let mut current = vec![0; right.len() + 1];

    for (left_index, left_char) in left.chars().enumerate() {
        current[0] = left_index + 1;
        for (right_index, right_char) in right.iter().enumerate() {
            let insertion = current[right_index] + 1;
            let deletion = previous[right_index + 1] + 1;
            let substitution = previous[right_index] + usize::from(left_char != *right_char);
            current[right_index + 1] = insertion.min(deletion).min(substitution);
        }
        std::mem::swap(&mut previous, &mut current);
    }

    previous[right.len()]
}
