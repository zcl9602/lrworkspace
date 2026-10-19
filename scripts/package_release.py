#!/usr/bin/env python3
"""Build-time packaging only; release users do not need Python."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import tarfile
import zipfile


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--bin-dir", type=Path, required=True)
    parser.add_argument("--out-dir", type=Path, required=True)
    parser.add_argument("--platform", choices=["windows", "macos", "linux"], required=True)
    parser.add_argument("--arch", choices=["x86_64", "aarch64"], required=True)
    parser.add_argument("--source-commit", required=True)
    args = parser.parse_args()
    repository = Path(__file__).resolve().parents[1]
    suffix = ".exe" if args.platform == "windows" else ""
    binary_dir = args.bin_dir.resolve()
    versions = []
    for name in ("rw", "lrworkspace-mcp"):
        line = subprocess.check_output([str(binary_dir / (name + suffix)), "--version"], text=True).strip()
        versions.append(line.rsplit(" ", 1)[-1])
    if versions[0] != versions[1]:
        raise SystemExit("CLI/MCP version mismatch")
    name = f"lrworkspace-{versions[0]}-{args.platform}-{args.arch}"
    args.out_dir.mkdir(parents=True, exist_ok=True)
    package = args.out_dir / name
    package.mkdir()  # Never overwrite a previous package.
    for executable in ("rw", "lrworkspace", "lrworkspace-mcp"):
        shutil.copy2(binary_dir / (executable + suffix), package / (executable + suffix))
    documents = [Path("README.md"), Path("usage.md")]
    documents += [path.relative_to(repository) for path in (repository / "docs").rglob("*") if path.is_file()]
    for document in documents:
        destination = package / document
        destination.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(repository / document, destination)
    files = {str(p.relative_to(package)).replace(os.sep, "/"): hashlib.sha256(p.read_bytes()).hexdigest()
             for p in sorted(package.rglob("*")) if p.is_file()}
    (package / "build-info.json").write_text(json.dumps({
        "version": versions[0], "platform": args.platform, "arch": args.arch,
        "source_commit": args.source_commit, "sha256": files,
        "macos_deployment_target": os.environ.get("MACOSX_DEPLOYMENT_TARGET") if args.platform == "macos" else None,
    }, indent=2) + "\n", encoding="utf-8")
    if suffix:
        archive = args.out_dir / (name + ".zip")
        with zipfile.ZipFile(archive, "x", zipfile.ZIP_DEFLATED) as stream:
            for path in sorted(package.rglob("*")):
                if path.is_file():
                    stream.write(path, path.relative_to(args.out_dir))
    else:
        archive = args.out_dir / (name + ".tar.gz")
        with tarfile.open(archive, "x:gz") as stream:
            stream.add(package, arcname=name)
    digest = hashlib.sha256(archive.read_bytes()).hexdigest()
    archive.with_name(archive.name + ".sha256").write_text(f"{digest}  {archive.name}\n", encoding="ascii")
    print(archive.resolve())


if __name__ == "__main__":
    main()
