"""Build-time artifact gate. Candidate secrets/paths are never echoed."""
import mmap
import os
from pathlib import Path
import re
import tempfile


class PrivacyError(ValueError):
    pass


def private_roots(repository, target):
    paths = [Path.home(), Path(repository), Path(target)]
    paths += [Path(value) for name in ("USERPROFILE", "HOME", "CARGO_HOME", "RUSTUP_HOME")
              if (value := os.environ.get(name))]
    paths += [Path.home() / ".cargo", Path.home() / ".rustup", Path(tempfile.gettempdir())]
    roots = set()
    for path in paths:
        for item in (path.absolute(), path.resolve()):
            # Shared filesystem/temp roots are runtime constants, not identity.
            if len(str(item)) > 4 and str(item) not in ("/tmp", "/var/tmp"):
                roots.add(str(item))
    return sorted(roots, key=lambda value: (len(value), value))


def path_spellings(path):
    original = str(path).rstrip("/\\")
    forward = original.replace("\\", "/")
    variants = {original, forward, forward.replace("/", "\\")}
    if re.match(r"^[A-Za-z]:/", forward):
        variants.add("/" + forward[0].lower() + forward[2:])
        variants.add("/cygdrive/" + forward[0].lower() + forward[2:])
    # GNU COFF import library symbols encode absolute output directories.
    variants |= {re.sub(r"[^A-Za-z0-9_]", "_", value) for value in tuple(variants)}
    return {value for value in variants if len(value) > 4}


PROFILE_PATHS = [
    re.compile(rb"[A-Za-z]:[\\/]+Users[\\/]+[^\\/\x00\r\n]{1,100}[\\/]", re.I),
    re.compile(rb"/(?:Users|home)/[^/\x00\r\n]{1,100}/", re.I),
]
CREDENTIALS = [
    re.compile(rb"(?:gh[pousr]_[A-Za-z0-9]{30,}|github_pat_[A-Za-z0-9_]{30,})"),
    re.compile(rb"(?:AKIA|ASIA)[A-Z0-9]{16}"),
    re.compile(rb"(?:sk-(?:proj-|ant-api\d+-)?[A-Za-z0-9_-]{24,}|hf_[A-Za-z0-9]{30,}|xox[baprs]-[A-Za-z0-9-]{20,})"),
    re.compile(rb"-----BEGIN (?:RSA |EC |OPENSSH |DSA |ENCRYPTED )?PRIVATE KEY-----[\s\r\n]+[A-Za-z0-9+/=\r\n]{40,}"),
]
PUBLIC_KEY_ALGORITHMS = [b"sk-ecdsa-sha2-nistp256-cert-v01@openssh.com"]


def credential_material(data):
    for pattern in CREDENTIALS:
        for match in pattern.finditer(data):
            # An OpenSSH security-key algorithm is public protocol metadata,
            # not an API secret that happens to share the sk- prefix.
            if any(data[match.start():match.start()+len(name)] == name for name in PUBLIC_KEY_ALGORITHMS):
                continue
            return True
    return False


def inspect_artifact(path, roots=(), *, binary=True):
    """Return category names only; mmap avoids reading each executable twice."""
    path = Path(path)
    if path.stat().st_size == 0:
        return ["empty artifact"]
    findings = set()
    with path.open("rb") as file, mmap.mmap(file.fileno(), 0, access=mmap.ACCESS_READ) as data:
        if credential_material(data):
            findings.add("credential-shaped material")
        for pattern, offset in [(rb"(?:[\x09\x0a\x0d\x20-\x7e]\x00){12,}", 0),
                                (rb"(?:\x00[\x09\x0a\x0d\x20-\x7e]){12,}", 1)]:
            for match in re.finditer(pattern, data):
                for start in range(match.start(), match.end(), 65536):
                    # Bounded windows overlap to catch split tokens/paths.
                    plain = data[start:min(start+73728, match.end())][offset::2]
                    if credential_material(plain):
                        findings.add("credential-shaped material")
                    if binary and any(item.search(plain) for item in PROFILE_PATHS):
                        findings.add("unmapped user profile path")
        if binary and any(pattern.search(data) for pattern in PROFILE_PATHS):
            findings.add("unmapped user profile path")
        for root in roots:
            for spelling in path_spellings(root):
                for encoding in ("utf-8", "utf-16-le", "utf-16-be"):
                    if re.search(re.escape(spelling.encode(encoding)), data, re.I):
                        findings.add("unmapped build or user directory")
    return sorted(findings)


def assert_clean(paths, roots=(), *, binary=True):
    failures = []
    for path in paths:
        reasons = inspect_artifact(path, roots, binary=binary)
        if reasons:
            failures.append(Path(path).name + ": " + ", ".join(reasons))
    if failures:
        raise PrivacyError("Artifact privacy check failed (matched values suppressed): " + "; ".join(failures))


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("files", nargs="+", type=Path)
    parser.add_argument("--private-root", action="append", default=[])
    args = parser.parse_args()
    roots = private_roots(Path(__file__).resolve().parents[1], Path(args.files[0]).parent)
    try:
        assert_clean(args.files, roots + args.private_root)
    except PrivacyError as error:
        raise SystemExit(str(error))
    print("Artifact privacy check passed")
