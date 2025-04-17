# burndown 🔥

Async command line task runner with checkpointing, backoff, and retries.

## Quick start

```bash
nix run github:nicolaschan/burndown --help
echo -e 'a\nb\nc\nd\ne\nf\ng\nh' | nix run github:nicolaschan/burndown -- --cmd 'nix run github:nicolaschan/burndown#flakysnail'
```

## Features
- **Async**: Run tasks concurrently using async.
- **Checkpointing**: Save task state to disk for recovery.
- **Backoff**: Automatically retry failed tasks with exponential backoff.
