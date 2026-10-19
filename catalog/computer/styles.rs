//! The tool names a model calls to work a computer, as each vendor's
//! own computer tool names them: `ComputerTools` declares them and
//! `RouteComputerCall` recognizes them, so the two read the same names
//! from here. Models were trained on these exact names and fields,
//! which is why they are copied rather than made up.
//!
//! - Anthropic's computer toolset (`computer_toolset_20260801`): one
//!   tool per action, coordinates as `[x, y]`, modifier keys as `text`
//!   like `ctrl+shift`.
//! - OpenAI's computer tool: actions with `x` and `y`, modifier keys as
//!   a `keys` list like `["CTRL", "SHIFT"]`.
//! - Both: `bash {command, restart}`, the name and fields of Anthropic's
//!   own bash tool, for the computer's shell, plus `timeout_secs`, weft's
//!   own addition, to stop a command after a while.

/// Which vendor's names `ComputerTools` declares. `RouteComputerCall`
/// needs no style: it recognizes a call in either.
// SYNC: style options <-> catalog/computer/computer_tools/metadata.json (style)
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Style {
    Anthropic,
    OpenAi,
}

impl Style {
    pub fn parse(name: &str) -> Option<Style> {
        match name {
            "anthropic" => Some(Style::Anthropic),
            "openai" => Some(Style::OpenAi),
            _ => None,
        }
    }
}

/// The bounds Anthropic's toolset puts on a key's `repeat` and on a
/// `duration` (a wait, a key held down).
// SYNC: bounds <-> catalog/computer/computer/images/computer/computer.py (MOST_REPEATS, MOST_SECONDS, MOST_WHEEL_CLICKS)
pub const MOST_REPEATS: u64 = 100;
pub const MOST_SECONDS: f64 = 300.0;
/// The most wheel clicks one scroll takes, a few hundred lines of text:
/// further than that, a key (End, Page_Down) is the move, and a
/// bigger number is a mistake that would keep the wheel turning for
/// minutes.
pub const MOST_WHEEL_CLICKS: u64 = 100;
/// How far one click of the wheel scrolls, to turn OpenAI's scroll in
/// pixels into wheel clicks: about three lines of text, what one click
/// scrolls in a browser on Linux.
pub const PIXELS_PER_WHEEL_CLICK: u64 = 100;
/// The farthest one OpenAI scroll goes, in pixels: as far as the most
/// wheel clicks go.
pub const MOST_SCROLL_PIXELS: u64 = MOST_WHEEL_CLICKS * PIXELS_PER_WHEEL_CLICK;

// Both styles.
pub const SCREENSHOT: &str = "screenshot";
pub const TYPE: &str = "type";
pub const SCROLL: &str = "scroll";
pub const DOUBLE_CLICK: &str = "double_click";
pub const WAIT: &str = "wait";
pub const BASH: &str = "bash";

// Anthropic's.
pub const ZOOM: &str = "zoom";
pub const LEFT_CLICK: &str = "left_click";
pub const RIGHT_CLICK: &str = "right_click";
pub const MIDDLE_CLICK: &str = "middle_click";
pub const TRIPLE_CLICK: &str = "triple_click";
pub const LEFT_CLICK_DRAG: &str = "left_click_drag";
pub const MOUSE_MOVE: &str = "mouse_move";
pub const LEFT_MOUSE_DOWN: &str = "left_mouse_down";
pub const LEFT_MOUSE_UP: &str = "left_mouse_up";
pub const CURSOR_POSITION: &str = "cursor_position";
pub const KEY: &str = "key";
pub const HOLD_KEY: &str = "hold_key";

// OpenAI's.
pub const CLICK: &str = "click";
pub const DRAG: &str = "drag";
pub const KEYPRESS: &str = "keypress";
pub const MOVE: &str = "move";
