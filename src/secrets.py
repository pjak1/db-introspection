"""Command-line manager for DB secrets held in the OS keychain.

Usage:
    python -m src.secrets set <name> [--value V]   # prompts hidden if omitted
    python -m src.secrets get <name>
    python -m src.secrets delete <name>
    python -m src.secrets list
    python -m src.secrets import-env [--file PATH]

Secrets stored here are referenced from a connection's db_conn.txt as
`credential://<name>` (see plugins/README.md and README.md). This module holds
no keychain logic itself — it delegates everything to SecretStore.
"""

from __future__ import annotations

import argparse
import getpass
import sys
from pathlib import Path

from dotenv import dotenv_values

from src.errors import ConfigError
from src.secret_store import SecretStore

# Non-secret env keys (server behavior, not credentials) skipped by import-env.
_NON_SECRET_ENV_PREFIX = "DB_INTROSPECTION_"


def _default_env_file() -> Path:
    # src/secrets.py -> project root (directory containing server.py / .env)
    return Path(__file__).resolve().parent.parent / ".env"


def _cmd_set(store: SecretStore, args: argparse.Namespace) -> int:
    value = args.value
    if value is None:
        value = getpass.getpass(f"Value for {args.name}: ")
    if not value:
        print("Aborted: empty value.", file=sys.stderr)
        return 1
    store.set(args.name, value)
    print(f"Stored '{args.name}'. Reference it as: credential://{args.name}")
    return 0


def _cmd_get(store: SecretStore, args: argparse.Namespace) -> int:
    value = store.get(args.name)
    if value is None:
        print(f"'{args.name}' is not set.", file=sys.stderr)
        return 1
    print(value)
    return 0


def _cmd_delete(store: SecretStore, args: argparse.Namespace) -> int:
    store.delete(args.name)
    print(f"Deleted '{args.name}' (if it existed).")
    return 0


def _cmd_list(store: SecretStore, _args: argparse.Namespace) -> int:
    names = store.list_names()
    if not names:
        print("No secrets stored.")
        return 0
    for name in names:
        print(name)
    return 0


def _cmd_import_env(store: SecretStore, args: argparse.Namespace) -> int:
    env_file = Path(args.file) if args.file else _default_env_file()
    if not env_file.exists():
        print(f"No .env file at {env_file}.", file=sys.stderr)
        return 1

    imported: list[str] = []
    for name, value in dotenv_values(env_file).items():
        if value is None or name.startswith(_NON_SECRET_ENV_PREFIX):
            continue
        store.set(name, value)
        imported.append(name)

    if not imported:
        print(f"No secrets to import from {env_file}.")
        return 0

    print(f"Imported {len(imported)} secret(s) from {env_file} into the OS keychain.")
    print("Switch each db_conn.txt reference to the keychain like this:")
    for name in imported:
        print(f"  ${{{name}}}  ->  credential://{name}")
    print("Then remove the plaintext values from .env.")
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="python -m src.secrets",
        description="Manage DB secrets in the OS keychain (Windows Credential "
        "Manager / macOS Keychain / Linux Secret Service).",
    )
    sub = parser.add_subparsers(dest="command", required=True)

    p_set = sub.add_parser("set", help="Store a secret (prompts if --value omitted).")
    p_set.add_argument("name")
    p_set.add_argument("--value", help="Value; omit to be prompted without echo.")
    p_set.set_defaults(func=_cmd_set)

    p_get = sub.add_parser("get", help="Print a stored secret.")
    p_get.add_argument("name")
    p_get.set_defaults(func=_cmd_get)

    p_del = sub.add_parser("delete", help="Delete a stored secret.")
    p_del.add_argument("name")
    p_del.set_defaults(func=_cmd_delete)

    p_list = sub.add_parser("list", help="List stored secret names.")
    p_list.set_defaults(func=_cmd_list)

    p_imp = sub.add_parser(
        "import-env",
        help="Copy secrets from .env into the keychain (does not modify files).",
    )
    p_imp.add_argument("--file", help="Path to the .env file (default: project root).")
    p_imp.set_defaults(func=_cmd_import_env)

    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    store = SecretStore()
    try:
        return args.func(store, args)
    except ConfigError as err:
        print(str(err), file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
