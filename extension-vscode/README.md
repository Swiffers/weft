# Weft for VS Code

The editor side of [Weft](https://weavemindai.github.io/weft/), a
programming language and framework for AI orchestration: as flexible as
an agent, as reliable as code. Open a `.weft` file and the extension
shows it as a live graph; run it, and the graph lights up with the
run as it happens.

Weft is made by [Weavemind](https://weavemind.ai) and developed in the
open at [github.com/WeaveMindAI/weft](https://github.com/WeaveMindAI/weft).

## What you get

- **Graph view.** Every `.weft` file opens as an editable node graph
  next to the text. Edits flow both ways: change the text and the
  graph follows, change the graph and the text follows.
- **Projects panel.** The projects registered with your local weft
  runtime, with run / stop / open right there.
- **Runs panel.** Past and running runs, each one
  inspectable node by node in the graph.
- **Live diagnostics.** The weft compiler checks your file as you
  type and reports through the Problems panel.
- **AI streaming edits.** SEARCH/REPLACE blocks streamed by an AI
  chat apply to the open `.weft` file as they stream, with the graph
  updating.

## Requirements

The extension talks to the weft runtime on your machine. If you have
not installed it yet, read
[the install guide](https://weavemindai.github.io/weft/start/install.html);
it is one script. The extension asks the `weft` command for your install's
address (`http://127.0.0.1:14111` for the default install) and its key, and
picks up the new address if the install restarts on another port. If you want
it on another install of this machine, set `weft.dispatcherUrl` to that
install's address, and the extension sends that install's own key.

## What it does on your machine

It talks to the weft runtime on `localhost` and to nothing else: no
telemetry, no account, no outside service. To compile and run your
programs it starts the `weft` command that the install script put on
your PATH.

## Learn more

The full documentation lives at
[weavemindai.github.io/weft](https://weavemindai.github.io/weft/), and
questions and bug reports are welcome
[on the issue tracker](https://github.com/WeaveMindAI/weft/issues).

## License

See [LICENSE](https://github.com/WeaveMindAI/weft/blob/main/LICENSE).
