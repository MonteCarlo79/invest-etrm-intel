"""GB knowledge base ingestion orchestrator.

Fetches articles, reports, and market commentary from:
  - Elexon (market notices, system warnings)
  - ENTSO-E (generation data, forecasts, reports)
  - Timera Energy (blog / research articles)
  - Modo Energy (research / insights)
  - Meteologica (forecasts, commentary)

Usage:
    python -m services.gb_knowledge.ingest                    # all sources
    python -m services.gb_knowledge.ingest --only elexon,timera
"""
import argparse
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))


def run_knowledge_ingest(only: list[str] | None = None, verbose: bool = True) -> dict[str, int]:
    from dotenv import load_dotenv
    load_dotenv(
        os.path.join(os.path.dirname(__file__), "..", "..", "config", ".env"),
        override=False,
    )

    from services.gb_knowledge.base import get_db_conn, ensure_table

    conn = get_db_conn()
    ensure_table(conn)

    connectors = []

    try:
        from services.gb_knowledge.elexon import ElexonConnector
        connectors.append(("elexon", "Elexon (notices + news)", ElexonConnector()))
    except ImportError as e:
        if verbose:
            print(f"  [skip] elexon: {e}")

    try:
        from services.gb_knowledge.entso_e import EntsoEConnector
        connectors.append(("entso_e", "ENTSO-E (generation + reports)", EntsoEConnector()))
    except ImportError as e:
        if verbose:
            print(f"  [skip] entso_e: {e}")

    try:
        from services.gb_knowledge.timera import TimeraConnector
        connectors.append(("timera", "Timera Energy (blog + reports)", TimeraConnector()))
    except ImportError as e:
        if verbose:
            print(f"  [skip] timera: {e}")

    try:
        from services.gb_knowledge.modo_reports import ModoReportsConnector
        connectors.append(("modo", "Modo Energy (insights)", ModoReportsConnector()))
    except ImportError as e:
        if verbose:
            print(f"  [skip] modo: {e}")

    try:
        from services.gb_knowledge.meteologica import MeteologicaConnector
        connectors.append(("meteologica", "Meteologica (forecasts)", MeteologicaConnector()))
    except ImportError as e:
        if verbose:
            print(f"  [skip] meteologica: {e}")

    results = {}
    for key, label, connector in connectors:
        if only and key not in only:
            continue
        if verbose:
            print(f"  {label} ... ", end="", flush=True)
        try:
            n = connector.run(conn)
            results[key] = n
            if verbose:
                print(f"{n} new docs")
        except Exception as exc:
            results[key] = 0
            if verbose:
                print(f"ERROR: {exc}")

    conn.close()
    return results


if __name__ == "__main__":
    _SOURCE_KEYS = ["elexon", "entso_e", "timera", "modo", "meteologica"]

    parser = argparse.ArgumentParser(description="Ingest GB knowledge base")
    parser.add_argument(
        "--only",
        help=f"Comma-separated sources to run: {', '.join(_SOURCE_KEYS)}",
        default=None,
    )
    args = parser.parse_args()

    only_list = [s.strip() for s in args.only.split(",")] if args.only else None
    if only_list:
        invalid = [s for s in only_list if s not in _SOURCE_KEYS]
        if invalid:
            parser.error(f"Unknown source(s): {invalid}")

    print("Ingesting GB knowledge base" + (f"  [only: {only_list}]" if only_list else ""))
    results = run_knowledge_ingest(only=only_list)
    total = sum(results.values())
    print(f"Done. Total new docs: {total}")
