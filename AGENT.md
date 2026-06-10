use uv add to add pacakges dont modify pyproject.toml directly. and use uv run to run the script. 

Sandbox notes: use escalation for git operations that write `.git` metadata,
such as `git commit`. Use escalation for `uv` commands if they need access to
the user cache under `~/.cache/uv`.
