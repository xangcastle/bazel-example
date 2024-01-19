"""
Buildkite pipeline building primitives. Provides sane defaults and useful tools
to aid in pipeline construction.

If you're just getting started with Buildkite, check out the Pipeline
documentation first:

https://buildkite.com/docs/pipelines

The full pipeline config schema is also useful:

https://github.com/buildkite/pipeline-schema/blob/master/schema.json
"""
from __future__ import annotations
from typing import TYPE_CHECKING
from typing import Any
from typing import Dict
from typing import Iterable
from typing import List
from typing_extensions import NotRequired
from typing_extensions import TypedDict
import re
import subprocess
import uuid
from abc import ABC
from abc import abstractmethod
from collections import namedtuple
from enum import Enum
from yaml import dump

if TYPE_CHECKING:
    from re import Pattern


class Trigger(Enum):
    MASTER = 1
    DIFF = 2
    SCHEDULE = 3


class Queue(Enum):
    default = "default"
    high_cpu = "high-cpu"
    builders = "builders"
    publishers = "publishers"
    arm = "arm"
    test = "test"


Plugin = namedtuple("Plugin", ["name", "version"])


class BuildkiteEnvironment(TypedDict):
    commit: str
    trigger: Trigger
    changed_paths: List[str]
    merge_base: str
    repo_host: str


class AutomaticRetry(TypedDict):
    exit_status: int
    limit: int


class CommandStepRetry(TypedDict):
    manual: NotRequired[bool]
    automatic: NotRequired[List[AutomaticRetry]]


def _flatten(iterable):
    """
    Recurstively flattens nested lists into a single list.

    >>> list(_flatten(["1", ["2a", "2b", ["2b1", "2b2"]], "3"]))
    ["1", "2a", "2b", "2b1", "2b2", "3"]
    """
    for item in iterable:
        if isinstance(item, list):
            yield from _flatten(item)
        else:
            yield item


class ConcurrencyGroup(object):
    """
    Control the concurrency of particular steps. This object represents a lock across all executions of the pipeline, limiting how many concurrent builds can be run.

    https://buildkite.com/docs/pipelines/controlling-concurrency
    """

    def __init__(self, key, n=1):
        self.key = key
        self.n = n


def gen_step_key() -> str:
    # use hex as Buildkite doesn't accept UUID formatted keys
    return uuid.uuid4().hex


class Step(ABC):
    """
    A generic Buildkite step that handles dependency tracking. Common across Command, Wait, Block, Input & Trigger steps.

    Read more about step dependencies:
    https://buildkite.com/docs/pipelines/dependencies
    """

    def __init__(self, key=None):
        self._keys = [key or gen_step_key()]
        self._deps = []

    @property
    def keys(self) -> List[str]:
        return self._keys

    @property
    def deps(self):
        return self._deps

    def depends_on(self, dep):
        self.deps.extend(dep.keys)
        return self

    @abstractmethod
    def prefix_label(self, prefix: str):
        raise NotImplementedError

    @abstractmethod
    def build(self):
        raise NotImplementedError

    def __repr__(self):
        return f"{super().__repr__()}:\n{dump(self.build())}"


class Container(object):
    """
    A Container represents a describes an isolated environment for Command steps to be executed within on the host machine. Typically expected to be Docker containers however the concept could extend to other isolation mechanisms such as BSD Gaols.
    """

    def run_in_container(self, *steps):
        return NotImplementedError


class Command(Step):
    """
    A Command step is a type of step that runs a command. This implements a
    builder with some sane defaults around artifacts, timeouts and retries. It
    also allows users to target jobs to particular queues.

    Read more:
    https://buildkite.com/docs/pipelines/command-step
    """

    def __init__(self, key=None):
        super().__init__(key)
        self._prefix = None
        self._step = {
            "timeout_in_minutes": 20,
            "artifact_paths": ["artifacts/*"],
            "retry": {
                "manual": True,
                "automatic": [
                    {"exit_status": -1, "limit": 2},  # agent lost
                    {"exit_status": 255, "limit": 2},  # agent forced shutdown
                ],
            },
        }

    def prefix_label(self, prefix: str):
        self._prefix = prefix
        if self._step["label"]:
            self._step["label"] = f'{prefix} {self._step["label"]}'
        return self

    def label(self, label: str):
        self._step["label"] = f'{self._prefix} {self._step["label"]}' if self._prefix else label
        return self

    def allow_dependency_failure(self):
        self._step["allowDependencyFailure"] = True
        return self

    def artifact_paths(self, *artifact_paths):
        """
        Appends artifact paths to the current list
        """
        self._step["artifact_paths"].extend(artifact_paths)
        return self

    def retry(
            self,
            retry: CommandStepRetry,
    ):
        self._step["retry"] = {**self._step["retry"], **retry}
        return self

    def run(self, *argc):
        # Cast to a list here because otherwise tuples are formatted by pyyaml as
        #
        #   commands: !!python/tuple
        #   - make
        #
        self._step["commands"] = list(argc)
        return self

    def plugin(self, plugin, options):
        assert isinstance(plugin, Plugin)
        name = "#".join(plugin)
        self._step.setdefault("plugins", []).append({name: options})
        return self

    def soft_fail(self, all=False):
        self._step["soft_fail"] = True if all else [{"exit_status": 1}]
        return self

    def queue(self, queue):
        assert isinstance(queue, Queue)
        self._step["agents"] = {"queue": queue.value}
        return self

    def parallelism(self, n):
        self._step["parallelism"] = int(n)
        return self

    def concurrency(self, group):
        assert isinstance(group, ConcurrencyGroup)
        self._step["concurrency_group"] = group.key
        self._step["concurrency"] = group.n
        return self

    def env(self, env):
        self._step["env"] = env
        return self

    def timeout(self, timeout_in_minutes):
        self._step["timeout_in_minutes"] = timeout_in_minutes
        return self

    def container(self, container, docker_compose_envs=None, docker_compose_options=None):
        return container.run_in_container(
            command=self, docker_compose_envs=docker_compose_envs, docker_compose_options=docker_compose_options
        )

    def build(self) -> List[Dict[str, Any]]:
        return [{**self._step, "key": key, "depends_on": list(set(self.deps))} for key in self.keys]


class Group(Step):
    """
    A group step waits for all steps to have successfully completed before allowing dependent steps to continue. It's based on the Buildkite wait step but designed to be more explict.

    Read more about wait steps: https://buildkite.com/docs/pipelines/wait-step
    """

    def __init__(self, steps: Iterable[Step]):
        super().__init__()
        self._steps: Iterable[Step] = steps

    @property
    def keys(self) -> List[str]:
        return list(_flatten(step.keys for step in self._steps))

    def prefix_label(self, prefix: str):
        for step in self._steps:
            step.prefix_label(prefix)
        return self

    def depends_on(self, dep):
        for step in self._steps:
            step.depends_on(dep)
        return self

    def build(self) -> List[Dict[str, Any]]:
        return list(_flatten(step.build() for step in self._steps))


class Pipeline(object):
    """
    A model of a Buildkite Pipeline that supports exporting itself to YAML and
    drawing a Graphviz diagram of it's execution flow.

    Read more:
    https://buildkite.com/docs/pipelines
    """

    def __init__(self, graph, branch, merge_base, source, commit, repo_host):
        self.graph = graph
        self.branch = branch
        self.merge_base = merge_base
        self.source = source
        self.commit = commit
        self.repo_host = repo_host

    def env(self) -> BuildkiteEnvironment:
        if self.branch == "refs/heads/master" or self.branch == "master":
            trigger = Trigger.MASTER
        elif self.source == "schedule":
            trigger = Trigger.SCHEDULE
        else:
            trigger = Trigger.DIFF

        changed_paths = [
            p.decode() for p in subprocess.check_output(["git", "diff", self.merge_base, "--name-only"]).splitlines()
        ]

        return {
            "commit": self.commit,
            "trigger": trigger,
            "changed_paths": changed_paths,
            "merge_base": self.merge_base,
            "repo_host": self.repo_host,
        }

    def to_yaml(self):
        g = self.graph(self.env())
        # Dedupes the final steps based on their key property
        steps = {}
        for step in g.build():
            steps[step["key"]] = step
        return dump({"steps": list(steps.values())}, default_flow_style=False)


class ConditionalGroup(Group):
    """Execute a group conditionally based on what paths have changed"""

    PIPELINE_PATH_PATTERNS = (re.compile(r'^\.buildkite'),)

    def __init__(self, env: BuildkiteEnvironment, path_patterns: Iterable[Pattern], steps: Iterable[Step]):
        """
        Create a conditional group step

        :param env: Provide the Buildkite environment values
        :param path_patterns: A list of regex patterns.
            At least one of the changed paths must match at least one of these patterns for the group to be executed.
        :param steps: The list of steps that should be executed if the paths match
        """
        super().__init__(steps)
        self.path_patterns = self.PIPELINE_PATH_PATTERNS + tuple(path_patterns)
        self.changed_paths = env.get("changed_paths", [])
        self.should_run = self.paths_matches(self.changed_paths, self.path_patterns)

    @staticmethod
    def paths_matches(paths: Iterable[str], patterns: Iterable[Pattern]) -> bool:
        return any(any(pattern.search(path) for path in paths) for pattern in patterns)

    @property
    def keys(self) -> List[str]:
        # Nothing should depend on this step if it won't be running
        return super().keys if self.should_run else []

    def build(self) -> List[Dict[str, Any]]:
        return super().build() if self.should_run else []


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
