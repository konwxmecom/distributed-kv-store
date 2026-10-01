#!/usr/bin/env python3
"""Create or update a password-hashed gateway user."""

import argparse
import getpass
import json
import os
import re
import tempfile
from pathlib import Path

from gateway import create_password_record


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("users_file", type=Path)
    parser.add_argument("username")
    args = parser.parse_args()

    if not re.fullmatch(r"[A-Za-z0-9_.-]{1,64}", args.username):
        parser.error("username must use 1-64 letters, digits, dots, underscores, or hyphens")

    password = getpass.getpass("Password (12+ characters): ")
    confirmation = getpass.getpass("Confirm password: ")
    if len(password) < 12:
        parser.error("password must contain at least 12 characters")
    if password != confirmation:
        parser.error("passwords do not match")

    path = args.users_file
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists():
        os.chmod(path, 0o600)
        users = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(users, dict):
            parser.error("users file must contain a JSON object")
    else:
        users = {}
    users[args.username] = create_password_record(password)

    with tempfile.NamedTemporaryFile("w", encoding="utf-8", dir=path.parent, delete=False) as output:
        temporary = Path(output.name)
        os.chmod(temporary, 0o600)
        json.dump(users, output, indent=2)
        output.write("\n")
    os.replace(temporary, path)
    print(f"Updated user {args.username!r} in {path} (password hash only).")


if __name__ == "__main__":
    main()
