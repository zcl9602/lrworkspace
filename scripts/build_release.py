"""Build native release binaries with anonymous Rust/C source paths."""
import argparse
import os
from pathlib import Path
import shlex
import shutil
import subprocess
import json
import hashlib

from release_privacy import PrivacyError, assert_clean, private_roots


def build_environment(repository, target_dir, host, environ=None):
    env = dict(os.environ if environ is None else environ)
    roots = private_roots(repository, target_dir)
    # Later rules win. Map general roots first, then more specific locations.
    mappings = []
    for index, root in enumerate(roots):
        spellings = {root, root.replace("\\", "/"), root.replace("/", "\\")}
        mappings.extend((spelling, f"/lrw-build/{index}") for spelling in spellings)
    mappings.sort(key=lambda item: (len(item[0]), item[0]))
    if env.get("CARGO_ENCODED_RUSTFLAGS"):
        rustflags = env["CARGO_ENCODED_RUSTFLAGS"].split("\x1f")
    else:
        rustflags = shlex.split(env.get("RUSTFLAGS", ""), posix=True)
    rustflags.extend("--remap-path-prefix=" + before + "=" + after for before, after in mappings)
    env["CARGO_ENCODED_RUSTFLAGS"] = "\x1f".join(rustflags)
    env.pop("RUSTFLAGS", None)
    env["CARGO_PROFILE_RELEASE_DEBUG"] = "0"
    env["CARGO_PROFILE_RELEASE_STRIP"] = "symbols"
    native = [(("/pathmap:" if "msvc" in host else "-ffile-prefix-map=") + before + "=" + after)
              for before, after in mappings]
    if "msvc" in host:
        native.insert(0, "/experimental:deterministic")
    # cc-rs and OpenSSL's make rules must preserve paths containing spaces.
    if "windows" in host:
        for variable in ("CFLAGS", "CXXFLAGS"):
            env[variable] = (env.get(variable, "") + " " + shlex.join(native)).strip()
    else:
        # OpenSSL embeds CFLAGS in its version string. Inject mappings only at
        # compiler execution time, through a basename-only wrapper on PATH.
        tools = Path(target_dir) / ".private-build-tools"
        tools.mkdir(parents=True, exist_ok=True)
        source = Path(__file__).with_name("native_compiler.py")
        revision = hashlib.sha256(source.read_bytes()).hexdigest()[:8]
        def compiler(value):
            return [value] if Path(value).is_file() else shlex.split(value)
        env["LRW_NATIVE_CC_CONFIG"] = json.dumps({"cc": compiler(env.get("CC", "cc")), "cxx": compiler(env.get("CXX", "c++")), "flags": native, "roots": roots})
        cc_name, cxx_name = f"lrw-{revision}-cc", f"lrw-{revision}-cxx"
        for name in (cc_name, cxx_name):
            wrapper = tools / name
            shutil.copyfile(source, wrapper)
            wrapper.chmod(0o700)
        env["CC"] = cc_name
        env["CXX"] = cxx_name
        env["PATH"] = str(tools) + os.pathsep + env.get("PATH", os.defpath)
    env["CC_SHELL_ESCAPED_FLAGS"] = "1"
    return env, roots


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--cargo", default=os.environ.get("CARGO", "cargo"))
    parser.add_argument("--target-dir", type=Path, default=Path(os.environ.get("CARGO_TARGET_DIR", "target")))
    parser.add_argument("--verify-install", action="store_true")
    parser.add_argument("--verify-password", action="store_true", help="requires test-only Paramiko")
    args = parser.parse_args()
    repository = Path(__file__).resolve().parents[1]
    target = args.target_dir.resolve()
    cargo = shutil.which(args.cargo) or args.cargo
    rustc = os.environ.get("RUSTC") or shutil.which("rustc") or str(Path(cargo).with_name("rustc.exe" if os.name == "nt" else "rustc"))
    info = subprocess.check_output([rustc, "-vV"], text=True)
    host = next(line.split(": ", 1)[1] for line in info.splitlines() if line.startswith("host: "))
    env, roots = build_environment(repository, target, host)
    command = [cargo, "build", "--release", "--locked", "--target-dir", str(target), "-p", "lrworkspace-cli", "-p", "lrworkspace-mcp"]
    subprocess.run(command, env=env, cwd=repository, check=True)
    for enabled, test, extra in [(args.verify_install, "portable", ["--include-ignored"]),
                                 (args.verify_password, "password_native", ["--ignored"])]:
        if enabled:
            subprocess.run([cargo, "test", "--release", "--locked", "--target-dir", str(target), "-p", "lrworkspace-cli", "--test", test, "--"] + extra,
                           env=env, cwd=repository, check=True)
    suffix = ".exe" if os.name == "nt" else ""
    binaries = [target / "release" / (name + suffix) for name in ("rw", "lrworkspace", "lrworkspace-mcp")]
    assert_clean(binaries, roots)
    print("Release build and artifact privacy checks passed")


if __name__ == "__main__":
    try:
        main()
    except PrivacyError as error:
        raise SystemExit(str(error)) from None
