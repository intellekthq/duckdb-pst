#!/usr/bin/env python3
"""Run ARM64 SQL tests in the build container (the harness skips this arch)."""

import sys
from pathlib import Path

sys.path.insert(
    0, str(Path(__file__).resolve().parents[1] / "extension-ci-tools" / "scripts")
)
from ci_phase import PhaseRunner


def main():
    runner = PhaseRunner()
    if runner.platform != "linux" or runner.architecture != "linux_arm64":
        return
    if runner.enabled("CI_SKIP_TESTS"):
        raise RuntimeError("release proof must not skip ARM64 tests")
    command = [
        "env",
        "-u",
        "SKIP_TESTS",
        "-u",
        "LINUX_CI_IN_DOCKER",
        "make",
        "test_release",
        "T=test/sql/*",
    ]
    print("Running supplemental linux_arm64 SQL suites", flush=True)
    runner.run([*runner.docker_arguments(), *command])


if __name__ == "__main__":
    main()
