import importlib.machinery
import importlib.util
import unittest
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
VALIDATOR_PATH = REPO_ROOT / "scripts" / "validate_examples_inventory"


def load_validator():
    loader = importlib.machinery.SourceFileLoader("validate_examples_inventory", str(VALIDATOR_PATH))
    spec = importlib.util.spec_from_loader(loader.name, loader)
    module = importlib.util.module_from_spec(spec)
    loader.exec_module(module)
    return module


validator = load_validator()


class XcodeTargetMappingTests(unittest.TestCase):
    def test_expected_xcode_target_matches_manifest_conventions(self):
        self.assertEqual(
            validator.expected_xcode_target({"group": "basic", "name": "connect"}),
            "connect",
        )
        self.assertEqual(
            validator.expected_xcode_target({"group": "async", "name": "async_transaction"}),
            "async_transaction",
        )
        self.assertEqual(
            validator.expected_xcode_target({"group": "batch", "name": "get"}),
            "batch_get",
        )
        self.assertEqual(
            validator.expected_xcode_target({"group": "geospatial", "name": "filter"}),
            "geo_filter",
        )
        self.assertEqual(
            validator.expected_xcode_target({"group": "query", "name": "projection"}),
            "query_projection",
        )
        self.assertEqual(
            validator.expected_xcode_target({"group": "scan", "name": "projection"}),
            "scan_projection",
        )

    def test_parse_xcode_project_targets_uses_project_target_list(self):
        pbxproj_text = """
		AAAAAAAAAAAAAAAAAAAAAAAA /* append */ = {
			isa = PBXNativeTarget;
			buildPhases = (
			);
			productReference = BBBBBBBBBBBBBBBBBBBBBBBB /* append */;
		};
		CCCCCCCCCCCCCCCCCCCCCCCC /* transaction */ = {
			isa = PBXNativeTarget;
			buildPhases = (
			);
			productReference = DDDDDDDDDDDDDDDDDDDDDDDD /* transaction */;
		};
		BFC65C651C9234A90079DF5A /* Project object */ = {
			isa = PBXProject;
			targets = (
				AAAAAAAAAAAAAAAAAAAAAAAA /* append */,
			);
		};
"""

        self.assertEqual(
            validator.parse_xcode_project_targets(pbxproj_text),
            {"append": "append"},
        )

    def test_source_reference_alone_does_not_count_as_xcode_support(self):
        pbxproj_text = """
		111111111111111111111111 /* example.c */ = {isa = PBXFileReference; path = ../examples/basic_examples/transaction/src/main/example.c; };
		222222222222222222222222 /* transaction */ = {
			isa = PBXNativeTarget;
			productReference = 333333333333333333333333 /* transaction */;
		};
		444444444444444444444444 /* append */ = {
			isa = PBXNativeTarget;
			productReference = 555555555555555555555555 /* append */;
		};
		BFC65C651C9234A90079DF5A /* Project object */ = {
			isa = PBXProject;
			targets = (
				444444444444444444444444 /* append */,
			);
		};
"""

        self.assertTrue(
            validator.xcode_contains_example(
                pbxproj_text,
                "examples/basic_examples/transaction",
            )
        )
        self.assertNotIn("transaction", validator.parse_xcode_project_targets(pbxproj_text))


if __name__ == "__main__":
    unittest.main()
