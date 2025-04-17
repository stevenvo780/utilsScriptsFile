#!/usr/bin/env python3
import os
import sys
import re
import unicodedata
import threading
import queue
import zipfile
from concurrent.futures import ThreadPoolExecutor

try:
    from PyPDF2 import PdfReader
except ImportError:
    PdfReader = None

ROOT_DIR = '/mnt/Biblioteca/LibrosBiblioteca'
INVALID_CHARS = re.compile(r'[^0-9A-Za-zñÑáéíóúÁÉÍÓÚüÜ._-]')
ZIP_EXTS = {'.epub', '.docx', '.xlsx', '.ods', '.odt'}
NUM_WORKERS = min(64, (os.cpu_count() or 4) * 2)
QUEUE_SIZE = 10000

def sanitize_filename(name):
    base, ext = os.path.splitext(name)
    base = unicodedata.normalize('NFKC', base)
    base = INVALID_CHARS.sub('_', base).strip('_')
    return base + ext.lower()

def validate_file(path):
    try:
        size = os.path.getsize(path)
        if size == 0:
            return False, 'empty'
        ext = os.path.splitext(path)[1].lower()
        if ext == '.pdf' and PdfReader:
            try:
                # Intenta abrir y leer metadatos básicos del PDF
                reader = PdfReader(path, strict=False) # Usar strict=False para ser más tolerante
                # Opcional: intentar leer la primera página si es necesario una validación más profunda
                # if len(reader.pages) > 0:
                #     reader.pages[0].extract_text()
            except Exception as e:
                # Captura errores específicos de PyPDF2 si es posible, o errores genéricos
                return False, f'PDF err: {type(e).__name__} - {e}'
        elif ext in ZIP_EXTS:
            try:
                with zipfile.ZipFile(path, 'r') as z:
                    # testzip() comprueba CRC y cabeceras de todos los archivos en el ZIP
                    bad_file = z.testzip()
                    if bad_file:
                        return False, f'ZIP corrupt @ {bad_file}'
                    # Opcional: intentar leer un archivo específico dentro del ZIP si es necesario
                    # info_list = z.infolist()
                    # if info_list:
                    #     z.read(info_list[0].filename)
            except zipfile.BadZipFile as e:
                 return False, f'ZIP err: BadZipFile - {e}'
            except Exception as e:
                return False, f'ZIP err: {type(e).__name__} - {e}'
        else:
            # Para otros tipos de archivo, intenta leer el archivo completo
            try:
                with open(path, 'rb') as f:
                    # Leer en bloques para manejar archivos grandes eficientemente
                    chunk_size = 1024 * 1024 # Leer en bloques de 1MB
                    while f.read(chunk_size):
                        pass
            except OSError as e:
                return False, f'Read err: {e.strerror}'
            except Exception as e:
                 return False, f'Read err: {type(e).__name__} - {e}'
        # Si todas las comprobaciones pasan
        return True, None
    except FileNotFoundError:
        return False, 'not found'
    except PermissionError:
        return False, 'denied'
    except OSError as e: # Captura otros errores de OS como disco lleno, etc.
        return False, f'OS err: {e.strerror}'
    except Exception as e: # Captura cualquier otra excepción inesperada
        return False, f'General err: {type(e).__name__} - {e}'

def process(path):
    dirpath, name = os.path.split(path)
    new_name = sanitize_filename(name)
    if new_name != name:
        new_path = os.path.join(dirpath, new_name)
        try:
            os.rename(path, new_path)
            path = new_path
        except Exception as e:
            return path, False, f'rename: {e}'
    ok, err = validate_file(path)
    return path, ok, err

def producer(q):
    for dp, _, fnames in os.walk(ROOT_DIR):
        for f in fnames:
            q.put(os.path.join(dp, f))
    for _ in range(NUM_WORKERS):
        q.put(None)

def worker(q, lock, counters, total):
    while True:
        path = q.get()
        if path is None:
            break
        p, ok, err = process(path)
        with lock:
            counters['done'] += 1
            if ok: counters['succ'] += 1
            else: counters['fail'] += 1
            i = counters['done']
        if ok:
            print(f"[{i}/{total}] OK: {p}", flush=True)
        else:
            print(f"[{i}/{total}] ERR: {p} → {err}", file=sys.stderr, flush=True)
        q.task_done()

def main():
    all_files = []
    for dp, _, fn in os.walk(ROOT_DIR):
        for f in fn:
            all_files.append(os.path.join(dp, f))
    total = len(all_files)
    if total == 0:
        print("No files")
        return

    q = queue.Queue(maxsize=QUEUE_SIZE)
    lock = threading.Lock()
    counters = {'done': 0, 'succ': 0, 'fail': 0}

    prod = threading.Thread(target=producer, args=(q,), daemon=True)
    prod.start()

    workers = []
    for _ in range(NUM_WORKERS):
        t = threading.Thread(target=worker, args=(q, lock, counters, total), daemon=True)
        t.start()
        workers.append(t)

    prod.join()
    q.join()
    for t in workers:
        t.join()

    print(f"\nSummary: {counters['succ']} OK, {counters['fail']} ERR of {total}")

if __name__ == '__main__':
    main()
