import os
from pathlib import Path
import shlex
import subprocess
import sys
import tempfile
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from build_release import build_environment
from release_privacy import PrivacyError, assert_clean, inspect_artifact


class ReleasePrivacyTests(unittest.TestCase):
    def test_ascii_unicode_wide_and_mangled_private_paths_are_rejected_without_echo(self):
        root = "C:/Users/Example Account/private project"
        for text in [root + "/source.rs", root.replace("/", "\\") + "\\source.rs",
                     "C__Users_Example_Account_private_project_release"]:
            for encoding in ("utf-8", "utf-16-le", "utf-16-be"):
                with tempfile.TemporaryDirectory() as directory:
                    file = Path(directory) / "fixture.bin"
                    file.write_bytes(text.encode(encoding))
                    with self.assertRaises(PrivacyError) as error:
                        assert_clean([file], [root])
                    self.assertNotIn("Example Account", str(error.exception))

    def test_unknown_user_profile_and_secret_material_are_caught(self):
        samples = [b"C:\\Users\\OtherAccount\\.cargo\\source.rs", b"/home/other-account/project/main.rs",
                   b"ghp_" + b"a1B2c3D4" * 5,
                   b"-----BEGIN OPENSSH PRIVATE KEY-----\n" + b"A" * 64 + b"\n"]
        for sample in samples:
            with tempfile.TemporaryDirectory() as directory:
                file = Path(directory) / "fixture.bin"; file.write_bytes(sample)
                self.assertTrue(inspect_artifact(file))

    def test_anonymous_paths_and_nonfunctional_ui_fixture_are_allowed(self):
        with tempfile.TemporaryDirectory() as directory:
            file = Path(directory) / "fixture.bin"
            file.write_bytes(b"/lrw-build/2/registry/src/dependency.rs\x00ui-validation-only\x00192.0.2.1")
            assert_clean([file], ["C:/Users/Example Account"])

    def test_flags_preserve_static_runtime_and_spaces(self):
        with tempfile.TemporaryDirectory(prefix="lrw build ") as directory:
            root = Path(directory)
            env, _ = build_environment(root, root / "target space", "x86_64-pc-windows-msvc",
                                       {"RUSTFLAGS": "-C target-feature=+crt-static", "CFLAGS": "-DKEEP=1"})
            flags = env["CARGO_ENCODED_RUSTFLAGS"].split("\x1f")
            self.assertIn("target-feature=+crt-static", flags)
            self.assertTrue(any(str(root) in flag and "--remap-path-prefix=" in flag for flag in flags))
            native = shlex.split(env["CFLAGS"])
            self.assertIn("-DKEEP=1", native)
            self.assertTrue(any(str(root) in flag and flag.startswith("/pathmap:") for flag in native))
            self.assertEqual(env["CARGO_PROFILE_RELEASE_STRIP"], "symbols")

    def test_packaging_refuses_dirty_binary_before_creating_output(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            for name in ("rw.exe", "lrworkspace.exe", "lrworkspace-mcp.exe"):
                (root / name).write_bytes(b"C:/Users/ExampleAccount/private/main.rs")
            output = root / "output"
            result = subprocess.run([sys.executable, str(Path(__file__).resolve().parents[1] / "package_release.py"),
                                     "--bin-dir", str(root), "--out-dir", str(output), "--platform", "windows",
                                     "--arch", "x86_64", "--source-commit", "0" * 40], capture_output=True)
            self.assertNotEqual(result.returncode, 0)
            self.assertIn(b"Artifact privacy check failed", result.stderr)
            self.assertNotIn(b"ExampleAccount", result.stderr)
            self.assertFalse(output.exists())


if __name__ == "__main__":
    unittest.main()
