#!/usr/bin/env python3
import os
import sys
import re
import unicodedata
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
import zipfile

try:
    from PyPDF2 import PdfReader
except ImportError:
    PdfReader = None

ROOT_DIR = '/mnt/Biblioteca/LibrosBiblioteca'

INVALID_CHARS = re.compile(r'[^0-9A-Za-zñÑáéíóúÁÉÍÓÚüÜ._-]')

def sanitize_name(name):
    name = unicodedata.normalize('NFKC', name)
    base, ext = os.path.splitext(name)
    base = INVALID_CHARS.sub('_', base).strip('_')
    return f"{base}{ext.lower()}"

def check_integrity(path):
    ext = os.path.splitext(path)[1].lower()
    if os.path.getsize(path) == 0:
        return False, "zero-byte file"
    if ext == '.pdf' and PdfReader:
        try:
            PdfReader(path)
        except Exception as e:
            return False, f"PDF error: {e}"
    if ext in ('.epub',):
        try:
            with zipfile.ZipFile(path) as z:
                if 'mimetype' not in z.namelist():
                    return False, "missing mimetype"
        except Exception as e:
            return False, f"EPUB error: {e}"
    return True, None

def process(path):
    dirpath, filename = os.path.split(path)
    new_name = sanitize_name(filename)
    new_path = os.path.join(dirpath, new_name)
    if new_path != path:
        try:
            os.rename(path, new_path)
            path = new_path
        except Exception as e:
            return path, False, f"rename failed: {e}"
    ok, err = check_integrity(path)
    return path, ok, err

def main():
    files = []
    for dp, _, fnames in os.walk(ROOT_DIR):
        for f in fnames:
            files.append(os.path.join(dp, f))
    total = len(files)
    if total == 0:
        print("No files found.")
        return

    lock = threading.Lock()
    count = {"done": 0}

    with ThreadPoolExecutor(max_workers=min(32, os.cpu_count() or 4)) as ex:
        futures = {ex.submit(process, p): p for p in files}
        for fut in as_completed(futures):
            path, ok, err = fut.result()
            with lock:
                count["done"] += 1
                i = count["done"]
            if ok:
                print(f"[{i}/{total}] OK: {path}")
            else:
                print(f"[{i}/{total}] ERROR: {path} → {err}", file=sys.stderr)

if __name__ == '__main__':
    main()
