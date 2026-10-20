#!/usr/bin/env python3
"""Ephemeral POSIX compiler wrapper; not part of installed LRW."""
import json
import os
import sys


def private_arguments(arguments, flags, roots):
    result = []
    for argument in arguments:
        # openssl-src already gives OPENSSLDIR a system default. Its unused
        # static-build plugin paths otherwise retain the temporary install root.
        for macro, default in [("ENGINESDIR", "/usr/local/lib/engines-3"),
                               ("MODULESDIR", "/usr/local/lib/ossl-modules")]:
            if argument.startswith("-D" + macro + "=") and any(root.replace("\\", "/") in argument.replace("\\", "/") for root in roots):
                argument = '-D' + macro + '="' + default + '"'
        result.append(argument)
    return flags + result


def main():
    config = json.loads(os.environ["LRW_NATIVE_CC_CONFIG"])
    compiler = config["cxx" if sys.argv[0].endswith("cxx") else "cc"]
    arguments = private_arguments(sys.argv[1:], config["flags"], config["roots"])
    os.execvp(compiler[0], compiler + arguments)


if __name__ == "__main__":
    main()
