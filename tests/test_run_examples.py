import argparse
import importlib.machinery
import importlib.util
import shutil
import subprocess
import tempfile
import unittest
from pathlib import Path
from unittest import mock


REPO_ROOT = Path(__file__).resolve().parents[1]
RUN_EXAMPLES_PATH = REPO_ROOT / "examples" / "run_examples"


def load_run_examples():
    loader = importlib.machinery.SourceFileLoader("run_examples", str(RUN_EXAMPLES_PATH))
    spec = importlib.util.spec_from_loader(loader.name, loader)
    module = importlib.util.module_from_spec(spec)
    loader.exec_module(module)
    return module


run_examples = load_run_examples()


def make_args(**overrides):
    values = {
        "host": "127.0.0.1",
        "port": 3000,
        "user": None,
        "password": None,
        "auth": None,
        "namespace": "test",
        "set_name": "eg-set",
        "key": "eg-key",
        "multikey": 20,
        "event_lib": None,
        "build": False,
        "tag": [],
        "exclude_tag": [],
        "targets": ["all"],
        "enterprise": False,
        "community": False,
        "strong_consistency": False,
        "no_strong_consistency": False,
        "ttl_support": False,
        "no_ttl_support": False,
        "server_version": None,
        "tls_enable": False,
        "tls_ca_file": None,
        "tls_ca_path": None,
        "tls_protocols": None,
        "tls_cipher_suite": None,
        "tls_crl_check": False,
        "tls_crl_check_all": False,
        "tls_cert_blacklist": None,
        "tls_log_session_info": False,
        "tls_key_file": None,
        "tls_cert_file": None,
        "tls_login_only": False,
        "tls_name": None,
    }
    values.update(overrides)
    return argparse.Namespace(**values)


class ParseProbeOutputTests(unittest.TestCase):
    def test_parse_probe_output_accepts_contract(self):
        facts = run_examples.parse_probe_output(
            "\n".join(
                [
                    "SERVER_VERSION=8.1.2.3",
                    "SERVER_ENTERPRISE=true",
                    "NAMESPACE_STRONG_CONSISTENCY=false",
                    "NAMESPACE_TTL_SUPPORT=true",
                ]
            )
        )

        self.assertEqual(
            facts,
            {
                "server_version": "8.1.2.3",
                "enterprise": True,
                "strong_consistency": False,
                "ttl_support": True,
            },
        )

    def test_parse_probe_output_rejects_missing_fields(self):
        with self.assertRaisesRegex(ValueError, "missing required fields"):
            run_examples.parse_probe_output("SERVER_VERSION=8.1.2.3\nSERVER_ENTERPRISE=true\n")


class DeriveFactsTests(unittest.TestCase):
    def test_derive_facts_merges_probe_results_and_manual_overrides(self):
        args = make_args(server_version="9.0.0", ttl_support=True, event_lib="libuv")
        probe_result = {
            "status": "passed",
            "message": "server facts auto-detected",
            "stdout_path": Path("/tmp/stdout"),
            "stderr_path": Path("/tmp/stderr"),
            "command": "probe",
            "facts": {
                "server_version": "8.1.2.3",
                "enterprise": False,
                "strong_consistency": True,
                "ttl_support": False,
            },
        }

        with mock.patch.object(run_examples, "run_fact_probe", return_value=probe_result):
            facts, returned_probe_result = run_examples.derive_facts(args, Path("/tmp/out"))

        self.assertEqual(returned_probe_result, probe_result)
        self.assertEqual(facts["server_version"], "9.0.0")
        self.assertFalse(facts["enterprise"])
        self.assertTrue(facts["strong_consistency"])
        self.assertTrue(facts["ttl_support"])
        self.assertEqual(facts["event_lib"], "libuv")

    def test_derive_facts_falls_back_to_manual_values_when_probe_fails(self):
        args = make_args(enterprise=True, no_strong_consistency=True)
        probe_result = {
            "status": "failed",
            "message": "fact probe execution failed",
            "stdout_path": Path("/tmp/stdout"),
            "stderr_path": Path("/tmp/stderr"),
            "command": "probe",
            "facts": None,
        }

        with mock.patch.object(run_examples, "run_fact_probe", return_value=probe_result):
            facts, returned_probe_result = run_examples.derive_facts(args, Path("/tmp/out"))

        self.assertEqual(returned_probe_result, probe_result)
        self.assertTrue(facts["enterprise"])
        self.assertFalse(facts["strong_consistency"])
        self.assertIsNone(facts["ttl_support"])
        self.assertIsNone(facts["server_version"])


class PrerequisiteTests(unittest.TestCase):
    def test_unknown_probeable_requirement_mentions_auto_probe_and_override(self):
        example = {
            "requires": {
                "event_lib": False,
                "ttl_support": True,
                "enterprise": False,
                "strong_consistency": False,
                "min_server_version": None,
                "udf": False,
                "secondary_index": False,
            }
        }
        facts = {
            "server_version": None,
            "enterprise": None,
            "strong_consistency": None,
            "ttl_support": None,
            "event_lib": None,
        }

        reason = run_examples.evaluate_prerequisites(example, facts)
        self.assertEqual(
            reason,
            "requires TTL support; auto-probe unavailable, pass --ttl-support or --no-ttl-support",
        )

    def test_min_server_version_requirement_skips_older_server(self):
        example = {
            "requires": {
                "event_lib": False,
                "ttl_support": False,
                "enterprise": False,
                "strong_consistency": False,
                "min_server_version": "8.1.3",
                "udf": False,
                "secondary_index": False,
            }
        }
        facts = {
            "server_version": "8.1.2.3",
            "enterprise": False,
            "strong_consistency": False,
            "ttl_support": True,
            "event_lib": None,
        }

        reason = run_examples.evaluate_prerequisites(example, facts)

        self.assertEqual(reason, "requires server version >= 8.1.3")

    def test_requirement_boundary_constants_match_plan(self):
        self.assertEqual(
            run_examples.PROBEABLE_REQUIREMENTS,
            ("event_lib", "ttl_support", "enterprise", "strong_consistency", "min_server_version"),
        )
        self.assertEqual(run_examples.EXAMPLE_MANAGED_REQUIREMENTS, ("udf", "secondary_index"))


class SelectionTests(unittest.TestCase):
    def test_select_examples_preserves_tag_filtering(self):
        examples = [
            {"id": "basic.get", "group": "basic", "name": "get", "tags": ["basic", "single-key"]},
            {"id": "query.simple", "group": "query", "name": "simple", "tags": ["query", "multi-key", "secondary-index"]},
            {"id": "query.aggregate", "group": "query", "name": "aggregate", "tags": ["query", "multi-key", "udf", "secondary-index"]},
        ]
        args = make_args(targets=["all"], tag=["query", "secondary-index"], exclude_tag=["udf"])

        selected = run_examples.select_examples(examples, args)

        self.assertEqual([example["id"] for example in selected], ["query.simple"])


class RunExampleEnvironmentTests(unittest.TestCase):
    def test_run_example_uses_platform_separator_for_resource_roots(self):
        example = {
            "id": "basic.udf",
            "source_dir": "examples/basic_examples/udf",
            "resource_roots": ["examples/basic_examples/udf", "examples/shared"],
            "working_dir": "examples/basic_examples/udf",
        }
        captured_env: dict[str, str] = {}

        def fake_run_command(command: list[str], cwd: Path, env: dict | None = None):
            captured_env.update(env or {})
            return subprocess.CompletedProcess(command, 0, "", "")

        with tempfile.TemporaryDirectory() as tmp:
            out_dir = Path(tmp)
            with (
                mock.patch.object(run_examples, "evaluate_prerequisites", return_value=None),
                mock.patch.object(run_examples, "ensure_build", return_value=(True, "")),
                mock.patch.object(run_examples, "build_example_command", return_value=["example"]),
                mock.patch.object(run_examples.os, "pathsep", ";"),
                mock.patch.object(run_examples, "run_command", side_effect=fake_run_command),
            ):
                result = run_examples.run_example(
                    example,
                    make_args(),
                    {},
                    out_dir,
                    {"backend": None, "completed": False},
                )

        self.assertEqual(result["status"], "passed")
        expected = ";".join(
            [
                str(REPO_ROOT / "examples/basic_examples/udf"),
                str(REPO_ROOT / "examples/shared"),
            ]
        )
        self.assertEqual(captured_env["EXAMPLE_RESOURCE_ROOTS"], expected)


class RunFactProbeTests(unittest.TestCase):
    def make_probe_output(self) -> str:
        return "\n".join(
            [
                "SERVER_VERSION=8.1.2.3",
                "SERVER_ENTERPRISE=true",
                "NAMESPACE_STRONG_CONSISTENCY=true",
                "NAMESPACE_TTL_SUPPORT=true",
            ]
        )

    def test_run_fact_probe_uses_backend_aware_root_and_probe_builds(self):
        args = make_args(event_lib="libev")
        calls: list[tuple[list[str], Path]] = []

        def fake_run_command(command: list[str], cwd: Path, env: dict | None = None):
            calls.append((command, cwd))

            if cwd == repo_root and "build" in command:
                root_lib = repo_root / "target" / "Linux-test" / "lib" / "libaerospike.a"
                root_header = repo_root / "target" / "Linux-test" / "include" / "aerospike" / "aerospike.h"
                root_lib.parent.mkdir(parents=True, exist_ok=True)
                root_header.parent.mkdir(parents=True, exist_ok=True)
                root_lib.write_text("library")
                root_header.write_text("header")
                return subprocess.CompletedProcess(command, 0, "root-build\n", "")

            if cwd == probe_dir and "build" in command:
                probe_binary.parent.mkdir(parents=True, exist_ok=True)
                probe_binary.write_text("probe")
                return subprocess.CompletedProcess(command, 0, "probe-build\n", "")

            return subprocess.CompletedProcess(command, 0, self.make_probe_output(), "")

        with tempfile.TemporaryDirectory() as tmp:
            repo_root = Path(tmp)
            probe_dir = repo_root / "examples" / "tools" / "probe_server_facts"
            probe_binary = probe_dir / "target" / "probe_server_facts"
            out_dir = repo_root / "out"

            with (
                mock.patch.object(run_examples, "REPO_ROOT", repo_root),
                mock.patch.object(run_examples, "FACT_PROBE_DIR", probe_dir),
                mock.patch.object(run_examples, "FACT_PROBE_BINARY", probe_binary),
                mock.patch.object(run_examples, "run_command", side_effect=fake_run_command),
            ):
                result = run_examples.run_fact_probe(args, out_dir)

        self.assertEqual(result["status"], "passed")
        self.assertEqual(result["facts"]["server_version"], "8.1.2.3")
        self.assertEqual(
            calls,
            [
                (["make", "EVENT_LIB=libev", "build", "prepare"], repo_root),
                (["make", "EVENT_LIB=libev", "build"], probe_dir),
                (
                    [
                        str(probe_binary),
                        "--host",
                        "127.0.0.1",
                        "--port",
                        "3000",
                        "--namespace",
                        "test",
                    ],
                    probe_dir,
                ),
            ],
        )

    def test_run_fact_probe_rebuilds_root_and_probe_when_backend_changes(self):
        args = make_args(event_lib="libuv")
        calls: list[tuple[list[str], Path]] = []

        def fake_run_command(command: list[str], cwd: Path, env: dict | None = None):
            calls.append((command, cwd))

            if command[-1] == "clean":
                shutil.rmtree(cwd / "target", ignore_errors=True)
                return subprocess.CompletedProcess(command, 0, "clean\n", "")

            if cwd == repo_root and "build" in command:
                root_lib = repo_root / "target" / "Linux-test" / "lib" / "libaerospike.a"
                root_header = repo_root / "target" / "Linux-test" / "include" / "aerospike" / "aerospike.h"
                root_lib.parent.mkdir(parents=True, exist_ok=True)
                root_header.parent.mkdir(parents=True, exist_ok=True)
                root_lib.write_text("library")
                root_header.write_text("header")
                return subprocess.CompletedProcess(command, 0, "root-build\n", "")

            if cwd == probe_dir and "build" in command:
                probe_binary.parent.mkdir(parents=True, exist_ok=True)
                probe_binary.write_text("probe")
                return subprocess.CompletedProcess(command, 0, "probe-build\n", "")

            return subprocess.CompletedProcess(command, 0, self.make_probe_output(), "")

        with tempfile.TemporaryDirectory() as tmp:
            repo_root = Path(tmp)
            probe_dir = repo_root / "examples" / "tools" / "probe_server_facts"
            probe_binary = probe_dir / "target" / "probe_server_facts"
            out_dir = repo_root / "out"

            root_lib = repo_root / "target" / "Linux-test" / "lib" / "libaerospike.a"
            root_header = repo_root / "target" / "Linux-test" / "include" / "aerospike" / "aerospike.h"
            root_lib.parent.mkdir(parents=True, exist_ok=True)
            root_header.parent.mkdir(parents=True, exist_ok=True)
            root_lib.write_text("old-library")
            root_header.write_text("old-header")
            run_examples.build_state_path(repo_root).write_text("libev")

            probe_binary.parent.mkdir(parents=True, exist_ok=True)
            probe_binary.write_text("old-probe")
            run_examples.build_state_path(probe_dir).write_text("libev")

            with (
                mock.patch.object(run_examples, "REPO_ROOT", repo_root),
                mock.patch.object(run_examples, "FACT_PROBE_DIR", probe_dir),
                mock.patch.object(run_examples, "FACT_PROBE_BINARY", probe_binary),
                mock.patch.object(run_examples, "run_command", side_effect=fake_run_command),
            ):
                result = run_examples.run_fact_probe(args, out_dir)

        self.assertEqual(result["status"], "passed")
        self.assertEqual(
            calls,
            [
                (["make", "EVENT_LIB=libuv", "clean"], repo_root),
                (["make", "EVENT_LIB=libuv", "build", "prepare"], repo_root),
                (["make", "EVENT_LIB=libuv", "clean"], probe_dir),
                (["make", "EVENT_LIB=libuv", "build"], probe_dir),
                (
                    [
                        str(probe_binary),
                        "--host",
                        "127.0.0.1",
                        "--port",
                        "3000",
                        "--namespace",
                        "test",
                    ],
                    probe_dir,
                ),
            ],
        )


class EnsureBuildTests(unittest.TestCase):
    def make_example(self) -> dict:
        return {
            "binary_path": "examples/async_examples/async_get/target/example",
            "source_dir": "examples/async_examples/async_get",
        }

    def test_ensure_build_skips_rebuild_when_backend_stamps_match(self):
        example = self.make_example()

        with tempfile.TemporaryDirectory() as tmp:
            repo_root = Path(tmp)
            example_dir = repo_root / example["source_dir"]
            binary = repo_root / example["binary_path"]
            root_lib = repo_root / "target" / "Linux-test" / "lib" / "libaerospike.a"
            root_header = repo_root / "target" / "Linux-test" / "include" / "aerospike" / "aerospike.h"

            binary.parent.mkdir(parents=True, exist_ok=True)
            binary.write_text("example")
            root_lib.parent.mkdir(parents=True, exist_ok=True)
            root_lib.write_text("library")
            root_header.parent.mkdir(parents=True, exist_ok=True)
            root_header.write_text("header")

            root_state = run_examples.build_state_path(repo_root)
            root_state.parent.mkdir(parents=True, exist_ok=True)
            root_state.write_text("libuv")

            example_state = run_examples.build_state_path(example_dir)
            example_state.parent.mkdir(parents=True, exist_ok=True)
            example_state.write_text("libuv")

            with (
                mock.patch.object(run_examples, "REPO_ROOT", repo_root),
                mock.patch.object(run_examples, "run_command") as run_command,
            ):
                ok, output = run_examples.ensure_build(
                    example,
                    make_args(event_lib="libuv"),
                    {"backend": None, "completed": False},
                )

        self.assertTrue(ok)
        self.assertEqual(output, "")
        run_command.assert_not_called()

    def test_ensure_build_rebuilds_root_and_example_when_backend_changes(self):
        example = self.make_example()
        calls: list[tuple[list[str], Path]] = []

        def fake_run_command(command: list[str], cwd: Path, env: dict | None = None):
            calls.append((command, cwd))

            if command[-1] == "clean":
                shutil.rmtree(cwd / "target", ignore_errors=True)
                return subprocess.CompletedProcess(command, 0, "clean\n", "")

            if cwd == repo_root:
                root_lib = repo_root / "target" / "Linux-test" / "lib" / "libaerospike.a"
                root_header = repo_root / "target" / "Linux-test" / "include" / "aerospike" / "aerospike.h"
                root_lib.parent.mkdir(parents=True, exist_ok=True)
                root_header.parent.mkdir(parents=True, exist_ok=True)
                root_lib.write_text("library")
                root_header.write_text("header")
            else:
                binary = cwd / "target" / "example"
                binary.parent.mkdir(parents=True, exist_ok=True)
                binary.write_text("example")

            return subprocess.CompletedProcess(command, 0, "build\n", "")

        with tempfile.TemporaryDirectory() as tmp:
            repo_root = Path(tmp)
            example_dir = repo_root / example["source_dir"]
            binary = repo_root / example["binary_path"]
            root_lib = repo_root / "target" / "Linux-test" / "lib" / "libaerospike.a"
            root_header = repo_root / "target" / "Linux-test" / "include" / "aerospike" / "aerospike.h"

            binary.parent.mkdir(parents=True, exist_ok=True)
            binary.write_text("old-example")
            root_lib.parent.mkdir(parents=True, exist_ok=True)
            root_lib.write_text("old-library")
            root_header.parent.mkdir(parents=True, exist_ok=True)
            root_header.write_text("old-header")

            root_state = run_examples.build_state_path(repo_root)
            root_state.parent.mkdir(parents=True, exist_ok=True)
            root_state.write_text("libev")

            example_state = run_examples.build_state_path(example_dir)
            example_state.parent.mkdir(parents=True, exist_ok=True)
            example_state.write_text("libev")

            with (
                mock.patch.object(run_examples, "REPO_ROOT", repo_root),
                mock.patch.object(run_examples, "run_command", side_effect=fake_run_command),
            ):
                ok, output = run_examples.ensure_build(
                    example,
                    make_args(event_lib="libuv"),
                    {"backend": None, "completed": False},
                )

            self.assertTrue(ok)
            self.assertIn("clean", output)
            self.assertEqual(
                calls,
                [
                    (["make", "EVENT_LIB=libuv", "clean"], repo_root),
                    (["make", "EVENT_LIB=libuv", "build", "prepare"], repo_root),
                    (["make", "EVENT_LIB=libuv", "clean"], example_dir),
                    (["make", "EVENT_LIB=libuv", "build"], example_dir),
                ],
            )
            self.assertEqual(run_examples.build_state_path(repo_root).read_text(), "libuv")
            self.assertEqual(run_examples.build_state_path(example_dir).read_text(), "libuv")

    def test_ensure_build_rebuilds_root_when_generated_headers_are_missing(self):
        example = self.make_example()
        calls: list[tuple[list[str], Path]] = []

        def fake_run_command(command: list[str], cwd: Path, env: dict | None = None):
            calls.append((command, cwd))

            if cwd == repo_root:
                root_lib = repo_root / "target" / "Linux-test" / "lib" / "libaerospike.a"
                root_header = repo_root / "target" / "Linux-test" / "include" / "aerospike" / "aerospike.h"
                root_lib.parent.mkdir(parents=True, exist_ok=True)
                root_header.parent.mkdir(parents=True, exist_ok=True)
                root_lib.write_text("library")
                root_header.write_text("header")
                return subprocess.CompletedProcess(command, 0, "root-build\n", "")

            binary = cwd / "target" / "example"
            binary.parent.mkdir(parents=True, exist_ok=True)
            binary.write_text("example")
            return subprocess.CompletedProcess(command, 0, "example-build\n", "")

        with tempfile.TemporaryDirectory() as tmp:
            repo_root = Path(tmp)
            example_dir = repo_root / example["source_dir"]
            binary = repo_root / example["binary_path"]
            root_lib = repo_root / "target" / "Linux-test" / "lib" / "libaerospike.a"

            binary.parent.mkdir(parents=True, exist_ok=True)
            binary.write_text("example")
            root_lib.parent.mkdir(parents=True, exist_ok=True)
            root_lib.write_text("library")

            root_state = run_examples.build_state_path(repo_root)
            root_state.parent.mkdir(parents=True, exist_ok=True)
            root_state.write_text("libuv")

            example_state = run_examples.build_state_path(example_dir)
            example_state.parent.mkdir(parents=True, exist_ok=True)
            example_state.write_text("libuv")

            with (
                mock.patch.object(run_examples, "REPO_ROOT", repo_root),
                mock.patch.object(run_examples, "run_command", side_effect=fake_run_command),
            ):
                ok, output = run_examples.ensure_build(
                    example,
                    make_args(event_lib="libuv"),
                    {"backend": None, "completed": False},
                )

        self.assertTrue(ok)
        self.assertIn("root-build", output)
        self.assertEqual(
            calls,
            [
                (["make", "EVENT_LIB=libuv", "build", "prepare"], repo_root),
            ],
        )


if __name__ == "__main__":
    unittest.main()
