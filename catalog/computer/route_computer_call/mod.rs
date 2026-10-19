//! RouteComputerCall: recognize which computer action a model's tool
//! call asks for, in Anthropic's or OpenAI's names (the shared
//! `styles.rs`), and put its arguments, in the one shape the action
//! nodes take, on that action's port. Every other port closes, so the
//! action node behind the port that emitted is the one that runs.
//!
//! This is the one place that knows how the vendors spell an action:
//! the action nodes take a single shape whichever model called.

use async_trait::async_trait;
use serde_json::{json, Map, Value};

use weft::node::NodeOutput;
use weft::{node_bail, Node, NodeManifest, RunContext, WeftResult};

use super::styles;

#[derive(NodeManifest)]
pub struct RouteComputerCallNode;

#[cfg(feature = "node-tests")]
mod tests;

/// Where a call goes: the output port, and the value it carries.
#[derive(Debug, Clone, PartialEq)]
pub struct Routed {
    pub port: &'static str,
    pub value: Value,
}

// SYNC: output ports <-> catalog/computer/route_computer_call/metadata.json (outputs)
pub const SCREENSHOT: &str = "screenshot";
pub const ZOOM: &str = "zoom";
pub const CLICK: &str = "click";
pub const DRAG: &str = "drag";
pub const MOVE: &str = "move";
pub const MOUSE_BUTTON: &str = "mouseButton";
pub const CURSOR: &str = "cursor";
pub const SCROLL: &str = "scroll";
pub const TYPE: &str = "type";
pub const KEY: &str = "key";
pub const COMMAND: &str = "command";
pub const WAIT: &str = "wait";
pub const OTHER: &str = "other";


/// How long OpenAI's `wait`, which names no duration, waits.
const OPENAI_WAIT_SECS: f64 = 1.0;

/// Route one tool call (`{ id, name, arguments }`, `arguments` the raw
/// JSON text a model wrote). A name that is no computer action goes to
/// `other`, the call untouched, so a program can route its own tools from there.
/// Arguments a known action cannot use are an error naming the tool
/// and the field, which a model reading it can correct.
pub fn route(call: &Value) -> Result<Routed, String> {
    let name = call["name"].as_str().ok_or_else(|| format!("the tool call has no name: {call}"))?;
    let raw = call["arguments"].as_str().ok_or_else(|| format!("the tool call {name} has no arguments text: {call}"))?;
    let args: Map<String, Value> = if raw.trim().is_empty() {
        Map::new()
    } else {
        match serde_json::from_str(raw) {
            Ok(Value::Object(args)) => args,
            Ok(other) => return Err(format!("{name}: the arguments must be a JSON object, got {other}")),
            Err(e) => return Err(format!("{name}: the arguments are not JSON ({e}): {raw}")),
        }
    };
    let a = Args { tool: name, args: &args };
    let routed = |port: &'static str, value: Value| Ok(Routed { port, value });
    match name {
        styles::SCREENSHOT => routed(SCREENSHOT, json!(true)),
        styles::CURSOR_POSITION => routed(CURSOR, json!(true)),
        styles::ZOOM => {
            let region = a.numbers("region", 4)?;
            routed(ZOOM, json!({ "region": region }))
        }
        styles::LEFT_CLICK => routed(CLICK, a.anthropic_click("left", 1)?),
        styles::RIGHT_CLICK => routed(CLICK, a.anthropic_click("right", 1)?),
        styles::MIDDLE_CLICK => routed(CLICK, a.anthropic_click("middle", 1)?),
        styles::TRIPLE_CLICK => routed(CLICK, a.anthropic_click("left", 3)?),
        // Both styles call it double_click: OpenAI's says where with x
        // and y (and holds keys with `keys`), Anthropic's with a
        // coordinate (or not at all). Any field only OpenAI's has makes
        // it OpenAI's, so a call missing the rest is refused naming them.
        styles::DOUBLE_CLICK if a.names_any(&["x", "y", "keys"]) => routed(CLICK, a.openai_click("left", 2)?),
        styles::DOUBLE_CLICK => routed(CLICK, a.anthropic_click("left", 2)?),
        styles::CLICK => {
            let button = a.string("button")?;
            let button = match button.as_str() {
                "wheel" => "middle",
                b @ ("left" | "right" | "back" | "forward") => b,
                other => return Err(format!("{name}: no button {other:?}; the buttons are left, right, wheel, back and forward")),
            };
            routed(CLICK, a.openai_click(button, 1)?)
        }
        styles::LEFT_CLICK_DRAG => {
            let start = a.point("start_coordinate")?;
            let end = a.point("coordinate")?;
            routed(DRAG, a.held_text(json!({ "path": [start, end] }))?)
        }
        styles::DRAG => {
            let path = a.list("path")?;
            if path.len() < 2 {
                return Err(format!("{name}: path needs at least two points, got {}", path.len()));
            }
            let points = path
                .iter()
                .map(|step| {
                    let step = step.as_object().ok_or_else(|| format!("{name}: each point of path is {{x, y}}, got {step}"))?;
                    let inner = Args { tool: name, args: step };
                    Ok(json!({ "x": inner.number("x")?, "y": inner.number("y")? }))
                })
                .collect::<Result<Vec<_>, String>>()?;
            routed(DRAG, a.held_keys(json!({ "path": points }))?)
        }
        styles::MOUSE_MOVE => {
            let [x, y] = a.point_pair("coordinate")?;
            routed(MOVE, a.held_text(json!({ "x": x, "y": y }))?)
        }
        styles::MOVE => routed(MOVE, a.held_keys(json!({ "x": a.number("x")?, "y": a.number("y")? }))?),
        styles::LEFT_MOUSE_DOWN => routed(MOUSE_BUTTON, json!({ "action": "down", "button": "left" })),
        styles::LEFT_MOUSE_UP => routed(MOUSE_BUTTON, json!({ "action": "up", "button": "left" })),
        // Both styles call it scroll: Anthropic's says a direction and
        // wheel clicks, OpenAI's pixels along each axis. Any field only
        // Anthropic's has makes it Anthropic's.
        styles::SCROLL if a.names_any(&["scroll_direction", "scroll_amount", "coordinate", "text"]) => {
            let direction = a.string("scroll_direction")?;
            if !["up", "down", "left", "right"].contains(&direction.as_str()) {
                return Err(format!("{name}: scroll_direction is up, down, left or right, got {direction:?}"));
            }
            let mut scroll = json!({ "direction": direction, "amount": a.whole("scroll_amount", 1, styles::MOST_WHEEL_CLICKS)? });
            if a.has("coordinate") {
                let [x, y] = a.point_pair("coordinate")?;
                scroll["x"] = x;
                scroll["y"] = y;
            }
            routed(SCROLL, a.held_text(scroll)?)
        }
        styles::SCROLL => {
            let (dx, dy) = (a.number_f64("scroll_x")?, a.number_f64("scroll_y")?);
            let (direction, pixels) = match (dx != 0.0, dy != 0.0) {
                (false, true) => (if dy > 0.0 { "down" } else { "up" }, dy.abs()),
                (true, false) => (if dx > 0.0 { "right" } else { "left" }, dx.abs()),
                (false, false) => return Err(format!("{name}: scroll_x and scroll_y are both 0, so there is nothing to scroll")),
                (true, true) => return Err(format!("{name}: scroll along one axis at a time (scroll_x or scroll_y, the other 0)")),
            };
            if pixels > styles::MOST_SCROLL_PIXELS as f64 {
                return Err(format!(
                    "{name}: {pixels} pixels is more than one scroll goes (at most {}); press End or Page_Down instead",
                    styles::MOST_SCROLL_PIXELS
                ));
            }
            let clicks = (pixels / styles::PIXELS_PER_WHEEL_CLICK as f64).ceil().max(1.0);
            let amount = clicks as u64;
            let scroll = json!({ "x": a.number("x")?, "y": a.number("y")?, "direction": direction, "amount": amount });
            routed(SCROLL, a.held_keys(scroll)?)
        }
        styles::TYPE => routed(TYPE, json!({ "text": a.string("text")? })),
        styles::KEY => {
            let mut key = json!({ "keys": a.string("text")? });
            if a.has("repeat") {
                key["repeat"] = json!(a.whole("repeat", 1, styles::MOST_REPEATS)?);
            }
            routed(KEY, key)
        }
        styles::HOLD_KEY => routed(KEY, json!({ "keys": a.string("text")?, "holdSecs": a.held_seconds("duration")? })),
        styles::KEYPRESS => {
            let keys = a.list("keys")?;
            if keys.is_empty() {
                return Err(format!("{name}: keys is empty, so there is nothing to press"));
            }
            let names = keys
                .iter()
                .map(|k| k.as_str().map(xdotool_key).ok_or_else(|| format!("{name}: each of keys is a key name, got {k}")))
                .collect::<Result<Vec<_>, String>>()?;
            routed(KEY, json!({ "keys": names.join("+") }))
        }
        // Both styles call it wait: Anthropic's names a duration,
        // OpenAI's names none. A `duration` written at all (null
        // included) makes it Anthropic's, which requires one.
        styles::WAIT if a.names_any(&["duration"]) => routed(WAIT, json!(a.held_seconds("duration")?)),
        styles::WAIT => routed(WAIT, json!(OPENAI_WAIT_SECS)),
        styles::BASH => {
            let mut command = Map::new();
            if a.has("command") {
                command.insert("command".into(), json!(a.string("command")?));
            }
            if a.has("restart") {
                let restart = args["restart"].as_bool().ok_or_else(|| format!("{name}: restart is true or false, got {}", args["restart"]))?;
                command.insert("restart".into(), json!(restart));
            }
            if a.has("timeout_secs") {
                command.insert("timeoutSecs".into(), json!(a.seconds("timeout_secs")?));
            }
            if !command.contains_key("command") && command.get("restart") != Some(&json!(true)) {
                return Err(format!("{name}: give a command to run, or restart: true"));
            }
            routed(COMMAND, Value::Object(command))
        }
        _ => routed(OTHER, call.clone()),
    }
}

/// An OpenAI key name as xdotool spells it: `CTRL` is `ctrl`, `ENTER` is
/// `Return`, a single letter is that letter in lower case. A name not
/// listed passes as written, and the computer refuses it if xdotool
/// does not know it either.
pub fn xdotool_key(name: &str) -> String {
    let upper = name.to_ascii_uppercase();
    let known = match upper.as_str() {
        "CTRL" | "CONTROL" => "ctrl",
        "ALT" | "OPTION" => "alt",
        "SHIFT" => "shift",
        "CMD" | "COMMAND" | "META" | "SUPER" | "WIN" => "super",
        "ENTER" | "RETURN" => "Return",
        "ESC" | "ESCAPE" => "Escape",
        "TAB" => "Tab",
        "SPACE" => "space",
        "BACKSPACE" => "BackSpace",
        "DELETE" | "DEL" => "Delete",
        "INSERT" => "Insert",
        "HOME" => "Home",
        "END" => "End",
        "PAGEUP" | "PAGE_UP" => "Page_Up",
        "PAGEDOWN" | "PAGE_DOWN" => "Page_Down",
        "UP" | "ARROWUP" => "Up",
        "DOWN" | "ARROWDOWN" => "Down",
        "LEFT" | "ARROWLEFT" => "Left",
        "RIGHT" | "ARROWRIGHT" => "Right",
        "CAPSLOCK" => "Caps_Lock",
        _ if name.chars().count() == 1 => return name.to_lowercase(),
        _ if upper.len() >= 2 && upper.starts_with('F') && upper[1..].chars().all(|c| c.is_ascii_digit()) => return upper,
        _ => return name.to_string(),
    };
    known.to_string()
}

/// One call's arguments, read with errors that name the tool and the
/// field.
struct Args<'a> {
    tool: &'a str,
    args: &'a Map<String, Value>,
}

impl Args<'_> {
    fn has(&self, field: &str) -> bool {
        self.args.get(field).is_some_and(|v| !v.is_null())
    }

    /// Whether the call wrote any of `fields`, a null included: which
    /// style a shared name is in.
    fn names_any(&self, fields: &[&str]) -> bool {
        fields.iter().any(|field| self.args.contains_key(*field))
    }

    fn missing(&self, field: &str, what: &str) -> String {
        match self.args.get(field) {
            None | Some(Value::Null) => format!("{}: {field} is missing; it is {what}", self.tool),
            Some(got) => format!("{}: {field} is {what}, got {got}", self.tool),
        }
    }

    fn string(&self, field: &str) -> Result<String, String> {
        self.args.get(field).and_then(Value::as_str).map(str::to_string).ok_or_else(|| self.missing(field, "text"))
    }

    fn number(&self, field: &str) -> Result<Value, String> {
        match self.args.get(field) {
            Some(v @ Value::Number(_)) => Ok(v.clone()),
            _ => Err(self.missing(field, "a number")),
        }
    }

    fn number_f64(&self, field: &str) -> Result<f64, String> {
        self.args.get(field).and_then(Value::as_f64).ok_or_else(|| self.missing(field, "a number"))
    }

    fn whole(&self, field: &str, low: u64, high: u64) -> Result<u64, String> {
        let value = self.args.get(field).and_then(Value::as_f64).filter(|v| v.fract() == 0.0 && (low as f64..=high as f64).contains(v));
        value.map(|v| v as u64).ok_or_else(|| self.missing(field, &format!("a whole number from {low} to {high}")))
    }

    /// Seconds above 0, with no upper bound: how long a command may run.
    fn seconds(&self, field: &str) -> Result<f64, String> {
        let value = self.args.get(field).and_then(Value::as_f64).filter(|v| *v > 0.0 && v.is_finite());
        value.ok_or_else(|| self.missing(field, "a number of seconds above 0"))
    }

    /// Seconds above 0 and at most what Anthropic's toolset takes for a
    /// wait or a key held down.
    fn held_seconds(&self, field: &str) -> Result<f64, String> {
        let most = styles::MOST_SECONDS;
        let value = self.args.get(field).and_then(Value::as_f64).filter(|v| *v > 0.0 && *v <= most);
        value.ok_or_else(|| self.missing(field, &format!("a number of seconds above 0 and at most {most}")))
    }

    fn list(&self, field: &str) -> Result<&Vec<Value>, String> {
        self.args.get(field).and_then(Value::as_array).ok_or_else(|| self.missing(field, "a list"))
    }

    fn numbers(&self, field: &str, count: usize) -> Result<Vec<Value>, String> {
        let shape = format!("a list of {count} numbers");
        let list = self.list(field).map_err(|_| self.missing(field, &shape))?;
        if list.len() != count || !list.iter().all(Value::is_number) {
            return Err(self.missing(field, &shape));
        }
        Ok(list.clone())
    }

    fn point_pair(&self, field: &str) -> Result<[Value; 2], String> {
        let shape = "[x, y], two numbers";
        let pair = self.list(field).map_err(|_| self.missing(field, shape))?;
        match pair.as_slice() {
            [x @ Value::Number(_), y @ Value::Number(_)] => Ok([x.clone(), y.clone()]),
            _ => Err(self.missing(field, shape)),
        }
    }

    fn point(&self, field: &str) -> Result<Value, String> {
        let [x, y] = self.point_pair(field)?;
        Ok(json!({ "x": x, "y": y }))
    }

    /// Anthropic's modifier keys (`text`, like `ctrl+shift`), onto `action`.
    fn held_text(&self, mut action: Value) -> Result<Value, String> {
        if self.has("text") {
            action["modifiers"] = json!(self.string("text")?);
        }
        Ok(action)
    }

    /// OpenAI's modifier keys (`keys`, like `["CTRL", "SHIFT"]`), onto `action`.
    fn held_keys(&self, mut action: Value) -> Result<Value, String> {
        if self.has("keys") {
            let keys = self.list("keys")?;
            let names = keys
                .iter()
                .map(|k| k.as_str().map(xdotool_key).ok_or_else(|| self.missing("keys", "a list of key names")))
                .collect::<Result<Vec<_>, String>>()?;
            if !names.is_empty() {
                action["modifiers"] = json!(names.join("+"));
            }
        }
        Ok(action)
    }

    fn anthropic_click(&self, button: &str, count: u64) -> Result<Value, String> {
        let mut click = json!({ "button": button, "count": count });
        if self.has("coordinate") {
            let [x, y] = self.point_pair("coordinate")?;
            click["x"] = x;
            click["y"] = y;
        }
        self.held_text(click)
    }

    fn openai_click(&self, button: &str, count: u64) -> Result<Value, String> {
        self.held_keys(json!({ "x": self.number("x")?, "y": self.number("y")?, "button": button, "count": count }))
    }
}

#[async_trait]
impl Node for RouteComputerCallNode {
    #[cfg(feature = "node-tests")]
    fn tests(&self) -> Vec<weft::NodeTest> {
        tests::tests()
    }

    async fn run(&self, ctx: RunContext) -> WeftResult<()> {
        let call: Value = ctx.inputs.get("call")?;
        let routed = match route(&call) {
            Ok(routed) => routed,
            Err(why) => node_bail!("{why}"),
        };
        ctx.pulse_downstream(NodeOutput::new().set(routed.port, routed.value)).await
    }
}
