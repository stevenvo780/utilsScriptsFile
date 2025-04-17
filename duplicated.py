#!/usr/bin/env python3
"""
Duplicate‑file cleaner con validación exhaustiva.

*   Agrupa por tamaño → hash parcial → hash completo (BLAKE2‑256).
*   Modo seguro por defecto (dry‑run). Solo elimina/mueve si se pasa `--apply`.
*   Genera un CSV (`duplicate_report.csv`) con cada decisión.
*   Opcional: mueve duplicados a una carpeta de cuarentena (`--quarantine DIR`).
*   Usa ProcessPoolExecutor para explotar todos los núcleos.
"""

import argparse
import csv
import hashlib
import logging
import os
import shutil
import sys
from concurrent.futures import ProcessPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path
from typing import List, Tuple

ROOT_DIR = Path('/mnt/Biblioteca/LibrosBiblioteca')
PARTIAL = 4096  # bytes leídos al inicio y al final
CPU = os.cpu_count() or 4
WORKERS = CPU * 2
REPORT_FILE = 'duplicate_report.csv'

logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] %(levelname)s: %(message)s',
    datefmt='%H:%M:%S'
)
log = logging.getLogger(__name__)

###############################################################################
# Hash helpers
###############################################################################

def quick_hash(path: Path) -> Tuple[str, int, str, str]:
    """Devuelve (ruta, tamaño, hash_parcial_hex, error)"""
    try:
        size = path.stat().st_size
        h = hashlib.blake2b(digest_size=16)
        with path.open('rb') as f:
            h.update(f.read(PARTIAL))
            if size > PARTIAL * 2:
                f.seek(-PARTIAL, os.SEEK_END)
                h.update(f.read(PARTIAL))
        return str(path), size, h.hexdigest(), ''
    except Exception as e:
        return str(path), -1, '', str(e)


def full_hash(path: Path) -> Tuple[str, str, str]:
    """Devuelve (ruta, hash_completo_hex, error)"""
    try:
        h = hashlib.blake2b(digest_size=32)
        with path.open('rb') as f:
            for chunk in iter(lambda: f.read(1024 * 1024), b''):
                h.update(chunk)
        return str(path), h.hexdigest(), ''
    except Exception as e:
        return str(path), '', str(e)

###############################################################################
# Core helpers
###############################################################################

def walk_files(root: Path) -> List[Path]:
    """Devuelve todos los archivos dentro de root."""
    return [p for p in root.rglob('*') if p.is_file()]


def group_by_size(paths: List[Path]):
    size_map: dict[int, List[Path]] = {}
    for p in paths:
        try:
            size_map.setdefault(p.stat().st_size, []).append(p)
        except Exception as e:
            log.warning(f'Size error {p}: {e}')
    return [v for v in size_map.values() if len(v) > 1]


def with_pool(func, iterable, desc: str):
    results = []
    total = len(iterable)
    with ProcessPoolExecutor(max_workers=WORKERS) as ex:
        futures = {ex.submit(func, item): item for item in iterable}
        for i, fut in enumerate(as_completed(futures), 1):
            results.append(fut.result())
            if i % 200 == 0 or i == total:
                log.info(f"{desc}: {i}/{total}")
    return results


def build_hash_maps(groups):
    # Quick hash
    qh_results = with_pool(quick_hash, [p for g in groups for p in g], 'quick‑hash')
    qmap: dict[tuple[int, str], List[Path]] = {}
    for path, size, qh, err in qh_results:
        if err:
            log.error(f'Quick‑hash error {path}: {err}')
            continue
        qmap.setdefault((size, qh), []).append(Path(path))

    # Candidatos tras hash parcial
    candidates = [v for v in qmap.values() if len(v) > 1]

    # Full hash
    fh_results = with_pool(full_hash, [p for g in candidates for p in g], 'full‑hash')
    fmap: dict[str, List[Path]] = {}
    for path, fh, err in fh_results:
        if err:
            log.error(f'Full‑hash error {path}: {err}')
            continue
        fmap.setdefault(fh, []).append(Path(path))
    return fmap

###############################################################################
# Deletion / quarantine helpers
###############################################################################

def ensure_dir(d: Path):
    d.mkdir(parents=True, exist_ok=True)


def remove_empty_dirs(root: Path):
    """Elimina directorios vacíos en cascada."""
    for dp, dirs, files in os.walk(root, topdown=False):
        if not dirs and not files:
            try:
                Path(dp).rmdir()
                log.info(f"Removed empty directory: {dp}")
            except Exception as e:
                log.debug(f'Skip rmdir {dp}: {e}')


def delete_or_quarantine(group: List[Path], apply: bool, quarantine_dir: Path | None):
    victims = group[1:]  # Mantén el primero
    for v in victims:
        try:
            if apply:
                if quarantine_dir:
                    ensure_dir(quarantine_dir)
                    shutil.move(v, quarantine_dir / v.name)
                else:
                    v.unlink()
            log.info(f"Removed duplicate: {v}")
        except Exception as e:
            log.error(f'Error removing {v}: {e}')

###############################################################################
# Reporting
###############################################################################

def write_report(rows):
    with open(REPORT_FILE, 'w', newline='', encoding='utf‑8') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(['timestamp', 'action', 'original', 'duplicate'])
        writer.writerows(rows)
    log.info(f'Report written to {REPORT_FILE}')

###############################################################################
# Main
###############################################################################

def main():
    parser = argparse.ArgumentParser(description='Clean duplicate files safely')
    parser.add_argument('--apply', action='store_true', help='PERMANENTLY delete / move duplicates')
    parser.add_argument('--quarantine', type=Path, help='Directory to move duplicates instead of deleting')
    args = parser.parse_args()

    start = datetime.now()
    files = walk_files(ROOT_DIR)
    log.info(f'Indexed {len(files)} files in {ROOT_DIR}')

    size_groups = group_by_size(files)
    log.info(f'{len(size_groups)} size groups >1')

    hash_map = build_hash_maps(size_groups)
    duplicates = [v for v in hash_map.values() if len(v) > 1]
    log.info(f'Found {sum(len(v)-1 for v in duplicates)} duplicates in {len(duplicates)} groups')

    report_rows = []
    for group in duplicates:
        kept = str(group[0])
        for dup in group[1:]:
            action = 'deleted' if args.apply else 'will‑delete'
            report_rows.append([datetime.now().isoformat(), action, kept, str(dup)])
        delete_or_quarantine(group, args.apply, args.quarantine)

    write_report(report_rows)

    # Limpiar directorios vacíos solo si realmente eliminamos/movimos
    if args.apply:
        remove_empty_dirs(ROOT_DIR)

    if not args.apply:
        log.warning('Dry‑run complete – nothing was deleted. Re‑run with --apply to commit changes.')

    log.info(f'Finished in {datetime.now() - start}')

###############################################################################

if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        log.warning('Interrupted by user')
