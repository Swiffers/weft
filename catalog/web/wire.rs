//! Fitting several page texts into one wire, for the web nodes whose
//! answers are lists of page text: a port value weighs at most
//! `weft::storage::MAX_WIRE_VALUE_BYTES` as JSON, measured with
//! `weft::storage::wire_weight`, the same measure the wire's own
//! refusal uses.

use serde_json::Value;

use weft::storage::{cut_to_weight, wire_weight, MAX_WIRE_VALUE_BYTES};

/// Shorten the text under `field` of the `items` as little as it takes
/// for the whole list to fit one wire: the shortest texts are kept
/// whole, and the room left is shared evenly by the longer ones, each
/// cut to its share. Answers which items were cut. Fails, with what the
/// list weighs, only when the items weigh too much with every text
/// emptied, which no cut can fix.
pub fn fit_texts(items: &mut [Value], field: &str) -> Result<Vec<bool>, usize> {
    let mut cut = vec![false; items.len()];
    if wire_weight(&*items) <= MAX_WIRE_VALUE_BYTES {
        return Ok(cut);
    }
    let texts: Vec<String> =
        items.iter().map(|item| item[field].as_str().unwrap_or_default().to_string()).collect();
    let mut emptied = items.to_vec();
    for item in emptied.iter_mut().filter(|item| item[field].is_string()) {
        item[field] = Value::String(String::new());
    }
    let fixed = wire_weight(&emptied);
    let Some(mut room) = MAX_WIRE_VALUE_BYTES.checked_sub(fixed) else {
        return Err(fixed);
    };
    let mut order: Vec<usize> = (0..items.len()).filter(|&i| items[i][field].is_string()).collect();
    order.sort_by_key(|&i| wire_weight(texts[i].as_str()));
    let mut left = order.len();
    for i in order {
        // A text's own bytes, beyond the two quotes an empty one keeps.
        let own = wire_weight(texts[i].as_str()) - 2;
        let share = room / left;
        left -= 1;
        if own <= share {
            room -= own;
            continue;
        }
        // Every text from here on is at least this long, so each gets
        // an even share of what is left.
        let kept = cut_to_weight(&texts[i], share + 2);
        room -= wire_weight(kept) - 2;
        items[i][field] = Value::String(kept.to_string());
        cut[i] = true;
    }
    Ok(cut)
}
