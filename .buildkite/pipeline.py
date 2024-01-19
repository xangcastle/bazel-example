from __future__ import annotations
from buildkite import Command, Group


def pipeline(env):
    changed_paths = env.get("changed_paths", [])
    print("changed_paths", changed_paths)
    _pipeline = [
        Command(
            key=":buildkite: Trigger Pinboard Build",
        ).label("Trigger Pinboard Build")
        .run('echo "triggering pinboard build"')
    ]

    return Group(_pipeline)
