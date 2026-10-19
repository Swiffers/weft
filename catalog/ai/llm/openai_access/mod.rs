//! OpenAIAccess: the ONE place the project's OpenAI connection is picked; the
//! marker passes downstream on `access` to the OpenAI Model nodes, and the
//! inference nodes open it at call time. The service recipe lives once,
//! in metadata.json.

weft::access_node!(OpenAIAccessNode);
