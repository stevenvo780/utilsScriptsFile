#!/usr/bin/env python3
import os
import sys
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed

ROOT_DIR = '/mnt/Biblioteca/LibrosBiblioteca'

ALLOWED_EXTS = {
    '.pdf', '.epub', '.mobi', '.azw', '.azw3', '.djvu', '.fb2', '.txt'
}

def delete_file(path):
    try:
        os.remove(path)
        return path, None
    except Exception as e:
        return path, e

def gather_files(root):
    for dirpath, _, filenames in os.walk(root):
        for name in filenames:
            if os.path.splitext(name)[1].lower() not in ALLOWED_EXTS:
                yield os.path.join(dirpath, name)

def main():
    to_delete = list(gather_files(ROOT_DIR))
    total = len(to_delete)
    if total == 0:
        print("No files to delete.")
        return

    success = failure = 0
    lock = threading.Lock()
    workers = min(32, os.cpu_count() or 4)

    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = {executor.submit(delete_file, p): p for p in to_delete}
        try:
            for future in as_completed(futures):
                path, error = future.result()
                with lock:
                    success += error is None
                    failure += error is not None
                    idx = success + failure
                if error is None:
                    print(f"[{idx}/{total}] Deleted: {path}")
                else:
                    print(f"[{idx}/{total}] Error deleting {path}: {error}", file=sys.stderr)
        except KeyboardInterrupt:
            executor.shutdown(wait=False)
            print("\nInterrupted.", file=sys.stderr)
            sys.exit(1)

    for dirpath, dirs, files in os.walk(ROOT_DIR, topdown=False):
        if not dirs and not files:
            try:
                os.rmdir(dirpath)
                print(f"Removed empty directory: {dirpath}")
            except Exception:
                pass

    print(f"\nDone. {success} deleted, {failure} errors.")

if __name__ == '__main__':
    main()
