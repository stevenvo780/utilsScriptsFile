#!/usr/bin/env python3
import os
import sys
import hashlib
import logging
from concurrent.futures import ProcessPoolExecutor, as_completed

ROOT_DIR = '/mnt/Biblioteca/LibrosBiblioteca'
PARTIAL_READ = 4096
CPU_COUNT = os.cpu_count() or 4
WORKERS = CPU_COUNT * 2

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] %(levelname)s: %(message)s',
    datefmt='%H:%M:%S'
)
logger = logging.getLogger()


def iter_files(root):
    for dirpath, _, filenames in os.walk(root):
        for f in filenames:
            yield os.path.join(dirpath, f)


def partial_hash(path):
    try:
        size = os.path.getsize(path)
        h = hashlib.blake2b(digest_size=16)
        with open(path, 'rb') as f:
            data = f.read(PARTIAL_READ)
            h.update(data)
            if size > PARTIAL_READ * 2:
                f.seek(-PARTIAL_READ, os.SEEK_END)
                h.update(f.read(PARTIAL_READ))
        return (path, h.hexdigest(), None)
    except Exception as e:
        return (path, None, e)


def full_hash(path):
    try:
        h = hashlib.blake2b(digest_size=16)
        with open(path, 'rb') as f:
            while True:
                chunk = f.read(1024 * 1024)
                if not chunk:
                    break
                h.update(chunk)
        return (path, h.hexdigest(), None)
    except Exception as e:
        return (path, None, e)


def remove_empty_dirs(root):
    for dp, dirs, files in os.walk(root, topdown=False):
        if not dirs and not files:
            try:
                os.rmdir(dp)
                logger.info(f"Removed empty directory: {dp}")
            except Exception:
                pass


def main():
    if not os.path.isdir(ROOT_DIR):
        logger.error(f"Root directory not found: {ROOT_DIR}")
        sys.exit(1)

    files = list(iter_files(ROOT_DIR))
    logger.info(f"Found {len(files)} files")

    # group by size
    size_map = {}
    for p in files:
        try:
            size_map.setdefault(os.path.getsize(p), []).append(p)
        except Exception as e:
            logger.error(f"Size error {p}: {e}")

    groups = [lst for lst in size_map.values() if len(lst) > 1]
    logger.info(f"{len(groups)} groups by size")

    # partial hashing
    partial_map = {}
    with ProcessPoolExecutor(max_workers=WORKERS) as ex:
        futures = {ex.submit(partial_hash, p): p for grp in groups for p in grp}
        for i, fut in enumerate(as_completed(futures), 1):
            path = futures[fut]
            p, h, err = fut.result()
            if err:
                logger.error(f"Partial-hash error {p}: {err}")
            else:
                partial_map.setdefault((os.path.getsize(p), h), []).append(p)
            logger.info(f"Processed partial [{i}/{len(futures)}]: {path}")

    filtered = [lst for lst in partial_map.values() if len(lst) > 1]
    logger.info(f"{len(filtered)} candidate groups after partial hash")

    # full hashing and delete
    hash_map = {}
    total_full = sum(len(lst) for lst in filtered)
    with ProcessPoolExecutor(max_workers=WORKERS) as ex:
        futures = {ex.submit(full_hash, p): p for grp in filtered for p in grp}
        for i, fut in enumerate(as_completed(futures), 1):
            p = futures[fut]
            p, h, err = fut.result()
            if err:
                logger.error(f"Full-hash error {p}: {err}")
            else:
                hash_map.setdefault(h, []).append(p)
            logger.info(f"Processed full [{i}/{total_full}]: {p}")

    dup = kept = 0
    for paths in hash_map.values():
        if len(paths) > 1:
            kept += 1
            for d in paths[1:]:
                try:
                    os.remove(d)
                    dup += 1
                    logger.info(f"Deleted duplicate: {d}")
                except Exception as e:
                    logger.error(f"Delete error {d}: {e}")
        else:
            kept += 1

    remove_empty_dirs(ROOT_DIR)
    logger.info(f"Done: kept {kept}, removed {dup} duplicates")


if __name__ == '__main__':
    main()