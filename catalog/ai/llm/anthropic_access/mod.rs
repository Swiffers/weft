//! AnthropicAccess: the ONE place the project's Anthropic connection is picked; the
//! marker passes downstream on `access` to the Anthropic Model nodes, and the
//! inference nodes open it at call time. The service recipe lives once,
//! in metadata.json.

weft::access_node!(AnthropicAccessNode);
