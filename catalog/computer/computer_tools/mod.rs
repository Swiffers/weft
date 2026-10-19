//! ComputerTools: describe the computer's actions to a model, as the
//! tool declarations an LLM node takes on `tools`.
//!
//! Each declaration copies a vendor's own computer tool (its name and
//! fields, from the shared `styles.rs`), so a model calls it the way it
//! was trained to, through any provider: they are ordinary function
//! tools, never the vendor's built-in computer tool, which most
//! providers refuse to relay. `RouteComputerCall` reads the calls back.
//! The descriptions carry the vendors' own advice for working a screen.

use async_trait::async_trait;
use serde_json::{json, Value};

use weft::node::NodeOutput;
use weft::{node_bail, Node, NodeManifest, RunContext, WeftResult};

use super::styles::{self, Style};

#[derive(NodeManifest)]
pub struct ComputerToolsNode;

#[cfg(feature = "node-tests")]
mod tests;

/// Which actions the model is offered, one switch per input of the same
/// name. The screenshot is always offered: a model that cannot see the
/// screen cannot work it.
// SYNC: switches <-> catalog/computer/computer_tools/metadata.json (inputs)
#[derive(Debug, Clone, Copy)]
pub struct Offered {
    pub zoom: bool,
    pub click: bool,
    pub drag: bool,
    pub r#move: bool,
    pub mouse_buttons: bool,
    pub cursor: bool,
    pub scroll: bool,
    pub typing: bool,
    pub keys: bool,
    pub wait: bool,
    pub terminal: bool,
}

impl Offered {
    pub const ALL: Offered = Offered {
        zoom: true,
        click: true,
        drag: true,
        r#move: true,
        mouse_buttons: true,
        cursor: true,
        scroll: true,
        typing: true,
        keys: true,
        wait: true,
        terminal: true,
    };
}

const COORDINATES_ARE: &str = "pixels from the left and top edges of the last full screenshot";

fn tool(name: &str, description: &str, properties: Value, required: &[&str]) -> Value {
    json!({
        "name": name,
        "description": description,
        "parameters": { "type": "object", "properties": properties, "required": required },
    })
}

fn coordinate(what: &str) -> Value {
    json!({
        "type": "array",
        "items": { "type": "integer" },
        "minItems": 2,
        "maxItems": 2,
        "description": format!("{what} as [x, y], {COORDINATES_ARE}."),
    })
}

fn pixel(what: &str) -> Value {
    json!({ "type": "integer", "description": format!("{what}, {COORDINATES_ARE}.") })
}

fn held_text() -> Value {
    json!({ "type": "string", "description": "Keys to hold down during the action, joined by + (shift, ctrl+shift)." })
}

fn held_keys() -> Value {
    json!({
        "type": "array",
        "items": { "type": "string" },
        "description": "Keys to hold down during the action, like [\"SHIFT\"] or [\"CTRL\", \"SHIFT\"].",
    })
}

const SCREENSHOT_SAYS: &str = "Take a screenshot of the whole screen. Take one before you act, and after each action or group of actions, then say in one sentence whether it did what you meant before you go on. Every coordinate you give is a pixel of the last full screenshot.";

const BASH_SAYS: &str = "Run a command in the computer's shell (bash) and get back its output and its exit code. Your shell keeps its folder and exported variables from one command to the next, so cd and export carry over; commands run one at a time. A long output comes back cut to its start and its end, with a line saying how much was cut: to read another part, run the command again through grep, head, tail or sed -n, or, for a command not safe to run twice, send its output to a file first (cmd > out.txt 2>&1) and read the file in parts. A command that does not end by itself (a server, a watcher) must be started in the background with & and its output sent to a file, or it holds the shell and every later command waits. Windows a command opens appear on the screen. Prefer a command to the mouse and keyboard for anything a command can do.";

fn bash() -> Value {
    tool(
        styles::BASH,
        BASH_SAYS,
        json!({
            "command": { "type": "string", "description": "The command to run." },
            "restart": { "type": "boolean", "description": "true starts your shell over first, in the home folder, stopping whatever it runs and whatever its commands left running. Alone, without a command, it only restarts." },
            "timeout_secs": { "type": "number", "description": "Stop the command after this many seconds. Left out, it runs until it ends." },
        }),
        &[],
    )
}

/// The declarations for `style`, the actions `offered` switched on.
pub fn declarations(style: Style, offered: Offered) -> Vec<Value> {
    let mut tools = vec![tool(styles::SCREENSHOT, SCREENSHOT_SAYS, json!({}), &[])];
    match style {
        Style::Anthropic => {
            if offered.zoom {
                tools.push(tool(
                    styles::ZOOM,
                    "See one region of the screen enlarged, to read small text or make out a small control. Coordinates stay those of the full screenshot afterwards.",
                    json!({ "region": {
                        "type": "array",
                        "items": { "type": "integer" },
                        "minItems": 4,
                        "maxItems": 4,
                        "description": format!("[x0, y0, x1, y1], the region's top left and bottom right corners, {COORDINATES_ARE}."),
                    } }),
                    &["region"],
                ));
            }
            if offered.click {
                for (name, says) in [
                    (styles::LEFT_CLICK, "Click the left mouse button."),
                    (styles::RIGHT_CLICK, "Click the right mouse button, for a context menu."),
                    (styles::MIDDLE_CLICK, "Click the middle mouse button."),
                    (styles::DOUBLE_CLICK, "Double-click the left mouse button, to open something or select a word."),
                    (styles::TRIPLE_CLICK, "Triple-click the left mouse button, to select a line or a paragraph."),
                ] {
                    tools.push(tool(
                        name,
                        &format!("{says} Aim at the middle of the target, and name it by where it is (the blue Submit button at the bottom right). Left out, coordinate is where the mouse is."),
                        json!({ "coordinate": coordinate("Where to click"), "text": held_text() }),
                        &[],
                    ));
                }
            }
            if offered.drag {
                tools.push(tool(
                    styles::LEFT_CLICK_DRAG,
                    "Press the left button at start_coordinate, move to coordinate, and release.",
                    json!({ "start_coordinate": coordinate("Where the drag starts"), "coordinate": coordinate("Where it ends"), "text": held_text() }),
                    &["start_coordinate", "coordinate"],
                ));
            }
            if offered.r#move {
                tools.push(tool(
                    styles::MOUSE_MOVE,
                    "Move the mouse without clicking, to hover over something.",
                    json!({ "coordinate": coordinate("Where to move"), "text": held_text() }),
                    &["coordinate"],
                ));
            }
            if offered.mouse_buttons {
                tools.push(tool(
                    styles::LEFT_MOUSE_DOWN,
                    "Press the left button down where the mouse is and keep it down: with mouse_move and left_mouse_up, a drag made step by step.",
                    json!({}),
                    &[],
                ));
                tools.push(tool(styles::LEFT_MOUSE_UP, "Let the left button up where the mouse is.", json!({}), &[]));
            }
            if offered.cursor {
                tools.push(tool(styles::CURSOR_POSITION, "Tell where the mouse is, as X=..., Y=....", json!({}), &[]));
            }
            if offered.scroll {
                tools.push(tool(
                    styles::SCROLL,
                    "Turn the mouse wheel. A keyboard shortcut (Page_Down, End) is surer where one works, and a scrollbar is hard to hit.",
                    json!({
                        "scroll_direction": { "type": "string", "enum": ["up", "down", "left", "right"] },
                        "scroll_amount": { "type": "integer", "minimum": 1, "maximum": styles::MOST_WHEEL_CLICKS, "description": "How many clicks of the wheel." },
                        "coordinate": coordinate("Where to scroll (the mouse moves there first); left out, where the mouse is"),
                        "text": held_text(),
                    }),
                    &["scroll_direction", "scroll_amount"],
                ));
            }
            if offered.typing {
                tools.push(tool(
                    styles::TYPE,
                    "Type text into whatever has the focus; click the field first. A newline presses Return.",
                    json!({ "text": { "type": "string" } }),
                    &["text"],
                ));
            }
            if offered.keys {
                tools.push(tool(
                    styles::KEY,
                    "Press a key or a combination, in xdotool names: Return, Tab, Escape, BackSpace, Up, Page_Down, ctrl+s, alt+Tab, ctrl+shift+t. Shortcuts are surer than the mouse for menus, dropdowns and scrolling.",
                    json!({
                        "text": { "type": "string", "description": "The key or combination." },
                        "repeat": { "type": "integer", "minimum": 1, "maximum": styles::MOST_REPEATS, "description": "How many times to press it." },
                    }),
                    &["text"],
                ));
                tools.push(tool(
                    styles::HOLD_KEY,
                    "Hold a key or a combination down for a number of seconds, in xdotool names.",
                    json!({
                        "text": { "type": "string", "description": "The key or combination." },
                        "duration": { "type": "number", "exclusiveMinimum": 0, "maximum": styles::MOST_SECONDS, "description": "Seconds to hold it." },
                    }),
                    &["text", "duration"],
                ));
            }
            if offered.wait {
                tools.push(tool(
                    styles::WAIT,
                    "Wait for something on the screen to finish (a page loading, an install), then take a screenshot to see it.",
                    json!({ "duration": { "type": "number", "exclusiveMinimum": 0, "maximum": styles::MOST_SECONDS, "description": "Seconds to wait." } }),
                    &["duration"],
                ));
            }
        }
        Style::OpenAi => {
            if offered.click {
                tools.push(tool(
                    styles::CLICK,
                    "Click a mouse button. Aim at the middle of the target, and name it by where it is (the blue Submit button at the bottom right).",
                    json!({
                        "button": { "type": "string", "enum": ["left", "right", "wheel", "back", "forward"] },
                        "x": pixel("Where to click, across"),
                        "y": pixel("Where to click, down"),
                        "keys": held_keys(),
                    }),
                    &["button", "x", "y"],
                ));
                tools.push(tool(
                    styles::DOUBLE_CLICK,
                    "Double-click the left mouse button, to open something or select a word.",
                    json!({ "x": pixel("Where to click, across"), "y": pixel("Where to click, down"), "keys": held_keys() }),
                    &["x", "y"],
                ));
            }
            if offered.drag {
                tools.push(tool(
                    styles::DRAG,
                    "Press the left button at the first point of path, move through the others, and release at the last.",
                    json!({
                        "path": {
                            "type": "array",
                            "minItems": 2,
                            "items": {
                                "type": "object",
                                "properties": { "x": pixel("Across"), "y": pixel("Down") },
                                "required": ["x", "y"],
                            },
                        },
                        "keys": held_keys(),
                    }),
                    &["path"],
                ));
            }
            if offered.r#move {
                tools.push(tool(
                    styles::MOVE,
                    "Move the mouse without clicking, to hover over something.",
                    json!({ "x": pixel("Where to move, across"), "y": pixel("Where to move, down"), "keys": held_keys() }),
                    &["x", "y"],
                ));
            }
            if offered.scroll {
                tools.push(tool(
                    styles::SCROLL,
                    "Scroll at a point, by pixels along one axis. A keyboard shortcut (PAGEDOWN, END) is surer where one works.",
                    json!({
                        "x": pixel("Where to scroll, across"),
                        "y": pixel("Where to scroll, down"),
                        "scroll_x": { "type": "integer", "minimum": -(styles::MOST_SCROLL_PIXELS as i64), "maximum": styles::MOST_SCROLL_PIXELS, "description": "Pixels to scroll right (negative: left)." },
                        "scroll_y": { "type": "integer", "minimum": -(styles::MOST_SCROLL_PIXELS as i64), "maximum": styles::MOST_SCROLL_PIXELS, "description": "Pixels to scroll down (negative: up)." },
                        "keys": held_keys(),
                    }),
                    &["x", "y", "scroll_x", "scroll_y"],
                ));
            }
            if offered.typing {
                tools.push(tool(
                    styles::TYPE,
                    "Type text into whatever has the focus; click the field first. A newline presses Enter.",
                    json!({ "text": { "type": "string" } }),
                    &["text"],
                ));
            }
            if offered.keys {
                tools.push(tool(
                    styles::KEYPRESS,
                    "Press keys together, like [\"CTRL\", \"S\"] or [\"ENTER\"]. Shortcuts are surer than the mouse for menus, dropdowns and scrolling.",
                    json!({ "keys": { "type": "array", "items": { "type": "string" }, "minItems": 1 } }),
                    &["keys"],
                ));
            }
            if offered.wait {
                tools.push(tool(
                    styles::WAIT,
                    "Wait a second for something on the screen to finish, then take a screenshot to see it.",
                    json!({}),
                    &[],
                ));
            }
        }
    }
    if offered.terminal {
        tools.push(bash());
    }
    tools
}

#[async_trait]
impl Node for ComputerToolsNode {
    #[cfg(feature = "node-tests")]
    fn tests(&self) -> Vec<weft::NodeTest> {
        tests::tests()
    }

    async fn run(&self, ctx: RunContext) -> WeftResult<()> {
        let style_name: String = ctx.inputs.get("style")?;
        let Some(style) = Style::parse(&style_name) else {
            node_bail!("no style '{style_name}': the styles are anthropic and openai");
        };
        let on = |name: &str| ctx.inputs.get::<bool>(name);
        let offered = Offered {
            zoom: on("zoom")?,
            click: on("click")?,
            drag: on("drag")?,
            r#move: on("move")?,
            mouse_buttons: on("mouseButtons")?,
            cursor: on("cursor")?,
            scroll: on("scroll")?,
            typing: on("typing")?,
            keys: on("keys")?,
            wait: on("wait")?,
            terminal: on("terminal")?,
        };
        ctx.pulse_downstream(NodeOutput::new().set("tools", declarations(style, offered))).await
    }
}
