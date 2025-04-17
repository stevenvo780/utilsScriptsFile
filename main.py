#!/usr/bin/env python3
"""
Clean a library directory by:
1. Normalising filenames (remove invalid chars, lowercase extensions).
2. Validating book‑like files (PDF or ZIP‑based formats).
3. Deleting zero‑byte, unreadable or corrupted files.
4. Removing empty directories afterwards.
Progress and status prints help debugging hangs or errors.
"""
import os
import sys
import re
import unicodedata
import zipfile
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Tuple

try:
    from PyPDF2 import PdfReader, PdfReadError  # type: ignore
except ImportError:
    PdfReader = None  # type: ignore
    PdfReadError = Exception  # type: ignore

INVALID_CHARS = re.compile(r"[^0-9A-Za-zñÑáéíóúÁÉÍÓÚüÜ._-]")
ZIP_EXTS = {
    ".epub", ".cbz", ".cbr", ".mobi", ".azw", ".azw3",
    ".fb2", ".djvu", ".odt", ".ods", ".docx", ".xlsx",
}
CHUNK = 1 << 20  # 1 MB
MAX_WORKERS = max(4, (os.cpu_count() or 4) * 4)


def sanitise(name: str) -> str:
    base, ext = os.path.splitext(name)
    base = unicodedata.normalize("NFKC", base)
    base = INVALID_CHARS.sub("_", base).strip("_")
    return f"{base}{ext.lower()}"


def unique_path(path: Path) -> Path:
    if not path.exists():
        return path
    stem, ext = path.stem, path.suffix
    counter = 1
    while True:
        cand = path.with_name(f"{stem}_{counter}{ext}")
        if not cand.exists():
            return cand
        counter += 1


def validate_pdf(fp: Path) -> Tuple[bool, str | None]:
    if not PdfReader:
        return True, None
    try:
        PdfReader(str(fp), strict=False)
        return True, None
    except PdfReadError as e:
        return False, f"PDF err: {e}"
    except Exception as e:
        return False, f"PDF err: {type(e).__name__} - {e}"


def validate_zip(fp: Path) -> Tuple[bool, str | None]:
    try:
        with zipfile.ZipFile(fp, "r") as z:
            bad = z.testzip()
            if bad:
                return False, f"ZIP corrupt @ {bad}"
        return True, None
    except zipfile.BadZipFile as e:
        return False, f"BadZipFile - {e}"
    except Exception as e:
        return False, f"ZIP err: {type(e).__name__} - {e}"


def validate_other(fp: Path) -> Tuple[bool, str | None]:
    try:
        with fp.open("rb") as f:
            while f.read(CHUNK):
                pass
        return True, None
    except OSError as e:
        return False, f"Read err: {e.strerror}"
    except Exception as e:
        return False, f"Read err: {type(e).__name__} - {e}"


def validate_file(fp: Path) -> Tuple[bool, str | None]:
    if fp.stat().st_size == 0:
        return False, "empty"
    ext = fp.suffix.lower()
    if ext == ".pdf":
        return validate_pdf(fp)
    if ext in ZIP_EXTS:
        return validate_zip(fp)
    return validate_other(fp)


def process(fp: Path) -> Tuple[Path, bool, str | None]:
    print(f"[STEP] Processing file: {fp}")
    new_name = sanitise(fp.name)
    if new_name != fp.name:
        target = unique_path(fp.with_name(new_name))
        try:
            fp.rename(target)
            fp = target
            print(f"[RENAMED] -> {fp}")
        except Exception as e:
            print(f"[ERROR] rename failed: {fp} -> {target} : {e}")
            return fp, False, f"rename: {e}"
    ok, err = validate_file(fp)
    if not ok:
        print(f"[DELETING] {fp}: reason {err}")
        try:
            fp.unlink()
        except Exception as e:
            print(f"[ERROR] delete failed: {fp} : {e}")
        return fp, False, err
    return fp, True, None


def remove_empty_dirs(root: Path) -> int:
    print(f"[STEP] Removing empty directories under {root}")
    removed = 0
    for dpath, dirnames, filenames in os.walk(root, topdown=False):
        if not dirnames and not filenames:
            try:
                Path(dpath).rmdir()
                removed += 1
                print(f"[REMOVED DIR] {dpath}")
            except OSError:
                pass
    return removed


def main() -> None:
    root = Path(sys.argv[1]) if len(sys.argv) > 1 else Path("/mnt/Biblioteca/LibrosBiblioteca")
    print(f"[STEP] Scanning files under {root}")
    paths = [Path(dp) / f for dp, _, fs in os.walk(root) for f in fs]
    total = len(paths)
    print(f"[FOUND] {total} files to process")
    if not total:
        print("[INFO] No files found")
        return

    print(f"[STEP] Starting processing with {MAX_WORKERS} threads")
    from tqdm import tqdm
    counters = {"done": 0, "succ": 0, "fail": 0}
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        futures = {ex.submit(process, p): p for p in paths}
        with tqdm(total=total, desc="Processing", unit="file") as bar:
            for fut in as_completed(futures):
                p, ok, err = fut.result()
                counters["done"] += 1
                counters["succ" if ok else "fail"] += 1
                status = 'OK' if ok else 'ERR'
                tqdm.write(f"[{counters['done']}/{total}] {status}: {p}{(' -> '+err) if err else ''}")
                bar.update()

    print(f"[SUMMARY] {counters['succ']} OK, {counters['fail']} ERR of {total}")
    removed = remove_empty_dirs(root)
    print(f"[SUMMARY] Removed {removed} empty dirs")


if __name__ == "__main__":
    if sys.platform.startswith("win"):
        from multiprocessing import freeze_support
        freeze_support()
    main()