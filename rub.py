import os
import re
import json
import time
import asyncio
from pathlib import Path
from typing import Optional

from dotenv import load_dotenv
from rubpy import Client as RubikaClient
import requests
import pyzipper
from urllib.parse import urlparse

load_dotenv()

SESSION = os.getenv("RUBIKA_SESSION", "rubika_session").strip()

BASE_DIR = Path(__file__).resolve().parent
DOWNLOAD_DIR = BASE_DIR / "downloads"
QUEUE_DIR = BASE_DIR / "queue"
QUEUE_FILE = QUEUE_DIR / "tasks.jsonl"
PROCESSING_FILE = QUEUE_DIR / "processing.json"
FAILED_FILE = QUEUE_DIR / "failed.jsonl"
STATUS_FILE = QUEUE_DIR / "status.jsonl"
URL_DIR = DOWNLOAD_DIR / "url"
CANCEL_FILE = QUEUE_DIR / "cancelled.jsonl"
RESET_FILE = QUEUE_DIR / "reset.flag"

MAX_RETRIES = 5
UPLOAD_TIMEOUT = 1800   # 30 min total wall-clock limit
TARGET = "me"

DOWNLOAD_DIR.mkdir(parents=True, exist_ok=True)
QUEUE_DIR.mkdir(parents=True, exist_ok=True)
URL_DIR.mkdir(parents=True, exist_ok=True)


# ─────────────────────────── helpers ────────────────────────────

def safe_filename(name: Optional[str]) -> str:
    name = (name or "file").strip()
    name = re.sub(r'[<>:"/\\|?*\x00-\x1F]', "_", name)
    name = name.rstrip(". ")
    return name[:200] or "file"


def pretty_size(size) -> str:
    size = float(size or 0)
    units = ["B", "KB", "MB", "GB"]
    index = 0
    while size >= 1024 and index < len(units) - 1:
        size /= 1024
        index += 1
    return f"{size:.2f} {units[index]}"


def get_per_attempt_timeout(file_path: str) -> int:
    """Seconds to allow for a single upload attempt, based on file size."""
    size_mb = Path(file_path).stat().st_size / (1024 * 1024)
    if size_mb < 100:
        return 300
    elif size_mb < 500:
        return 600
    elif size_mb < 1000:
        return 900
    else:
        return 1500


def eta_text(seconds) -> str:
    if not seconds or seconds <= 0:
        return "نامشخص"
    seconds = int(seconds)
    h = seconds // 3600
    m = (seconds % 3600) // 60
    s = seconds % 60
    if h:
        return f"{h}h {m}m"
    if m:
        return f"{m}m {s}s"
    return f"{s}s"


def push_status(task: dict, text: str, status: str = "working", percent: float | None = None):
    payload = {
        "chat_id": task.get("chat_id"),
        "message_id": task.get("status_message_id"),
        "job_id": task.get("job_id"),
        "status": status,
        "text": text,
        "percent": percent,
        "time": time.time(),
    }
    with open(STATUS_FILE, "a", encoding="utf-8") as f:
        f.write(json.dumps(payload, ensure_ascii=False) + "\n")


def is_cancelled(task: dict) -> bool:
    job_id = str(task.get("job_id", ""))
    if not job_id or not CANCEL_FILE.exists():
        return False
    with open(CANCEL_FILE, "r", encoding="utf-8") as f:
        for line in f:
            if not line.strip():
                continue
            item = json.loads(line)
            if str(item.get("job_id")) == job_id:
                return True
    return False


def should_reset() -> bool:
    return RESET_FILE.exists()


def clear_reset_flag():
    try:
        if RESET_FILE.exists():
            RESET_FILE.unlink()
    except Exception:
        pass


def unique_path(path: Path) -> Path:
    if not path.exists():
        return path
    stem, suffix, index = path.stem, path.suffix, 1
    while True:
        candidate = path.with_name(f"{stem}_{index}{suffix}")
        if not candidate.exists():
            return candidate
        index += 1


def has_session(session_name: str) -> bool:
    return any(
        Path(p).exists()
        for p in [session_name, f"{session_name}.session", f"{session_name}.sqlite"]
    )


# ─────────────────────────── first-run login (sync) ────────────────────────────

def ensure_session():
    """Interactive login before asyncio starts — only runs once."""
    if has_session(SESSION):
        return
    print("No session found. Starting login...")
    client = RubikaClient(name=SESSION)
    try:
        client.start()
        print("Login successful.")
    finally:
        try:
            client.disconnect()
        except Exception:
            pass


# ─────────────────────────── upload ────────────────────────────

async def upload_once(file_path: str, caption: str, timeout_s: int):
    """
    Create a brand-new RubikaClient for each upload attempt.

    Why: rubpy creates an aiohttp.ClientSession in __init__ (synchronous),
    so the session is bound to whatever event loop is running at construction
    time. Reusing a client across attempts risks 'Session is closed' errors.
    A fresh client per attempt is the safest pattern.

    The client's own timeout is set to timeout_s so aiohttp's internal
    asyncio.timeout() fires inside a proper Task context.
    """
    async with RubikaClient(name=SESSION, timeout=timeout_s) as client:
        await client.send_document(TARGET, file_path, caption=caption or "")


async def send_with_retry(file_path: str, caption: str = "", task: dict | None = None):
    last_error = None
    start_time = time.time()

    for attempt in range(1, MAX_RETRIES + 1):

        # Total wall-clock guard
        elapsed = time.time() - start_time
        if elapsed > UPLOAD_TIMEOUT:
            raise RuntimeError("آپلود بیشتر از حد مجاز طول کشید و لغو شد.")

        if task and is_cancelled(task):
            raise RuntimeError("ارسال لغو شد.")

        if task:
            push_status(
                task,
                f"🔼 در حال آپلود در روبیکا...\n\n"
                f"تلاش {attempt} از {MAX_RETRIES}\n\n"
                f"برای لغو:\n"
                f"`/del {task.get('job_id')}`",
                "uploading",
            )

        remaining = UPLOAD_TIMEOUT - (time.time() - start_time)
        per_attempt = min(get_per_attempt_timeout(file_path), int(remaining))

        try:
            await upload_once(file_path, caption, per_attempt)
            return  # ✅ success

        except asyncio.CancelledError:
            raise RuntimeError("ارسال لغو شد.")

        except Exception as e:
            last_error = e
            print(f"Upload attempt {attempt} failed: {e}")

        # Decide whether to retry
        err_lower = str(last_error).lower()
        transient = any(k in err_lower for k in [
            "502", "503", "bad gateway", "timeout", "timed out",
            "cannot connect", "connection reset", "temporarily unavailable",
            "error uploading chunk", "unexpected mimetype",
        ])

        if transient and attempt < MAX_RETRIES:
            if task and is_cancelled(task):
                raise RuntimeError("ارسال لغو شد.")
            if task:
                push_status(
                    task,
                    f"ارتباط با روبیکا ناپایدار بود...\n"
                    f"دوباره تلاش می‌کنم ({attempt + 1} از {MAX_RETRIES})",
                    "uploading",
                )
            await asyncio.sleep(5)
            continue

        # Non-transient error — stop retrying
        break

    raise last_error if last_error else RuntimeError("Upload failed.")


# ─────────────────────────── download url ────────────────────────────

async def download_url(task: dict) -> Path:
    url = task.get("url", "").strip()
    if not url:
        raise RuntimeError("URL خالیه")

    push_status(task, "در حال دانلود ...", "downloading", 0)
    loop = asyncio.get_event_loop()

    def _do_download():
        try:
            resp = requests.get(url, stream=True, timeout=(10, 60), allow_redirects=True)
            resp.raise_for_status()
        except requests.exceptions.Timeout:
            raise RuntimeError("لینک جواب نداد")
        except requests.exceptions.ConnectionError:
            raise RuntimeError("مشکل شبکه")
        except requests.exceptions.HTTPError as e:
            code = e.response.status_code if e.response else "نامشخص"
            raise RuntimeError(f"دانلود انجام نشد. کد خطا: {code}")

        cd = resp.headers.get("content-disposition", "")
        match = re.findall(r'filename="(.+?)"', cd)
        name = match[0] if match else Path(urlparse(url).path).name
        name = safe_filename(name or f"file_{int(time.time())}")
        if "." not in name:
            name += ".bin"

        target = unique_path(URL_DIR / name)
        total = int(resp.headers.get("content-length") or 0)
        downloaded_bytes, last_update, started = 0, 0, time.time()

        with open(target, "wb") as f:
            for chunk in resp.iter_content(1024 * 1024):
                if not chunk:
                    continue
                f.write(chunk)
                downloaded_bytes += len(chunk)

                now = time.time()
                if now - last_update < 3 and downloaded_bytes < total:
                    continue
                last_update = now

                speed = downloaded_bytes / max(now - started, 1)
                eta = (total - downloaded_bytes) / speed if total and speed else None
                percent = downloaded_bytes * 100 / total if total else None

                text = f"داره دانلود میکنه...\n\n{pretty_size(downloaded_bytes)}"
                if total:
                    text += f" از {pretty_size(total)}"
                text += f"\nسرعت: {pretty_size(speed)}/s"
                if eta:
                    text += f"\nمونده: {eta_text(eta)}"

                push_status(task, text, "downloading", percent)

        if not target.exists() or target.stat().st_size == 0:
            raise RuntimeError("فایل دانلود نشد")

        return target

    target = await loop.run_in_executor(None, _do_download)
    task["file_name"] = target.name
    task["file_size"] = target.stat().st_size
    return target


# ─────────────────────────── zip ────────────────────────────

async def make_zip_with_password(file_path: Path, password: str) -> Path:
    loop = asyncio.get_event_loop()

    def _zip():
        zip_path = unique_path(file_path.with_suffix(file_path.suffix + ".zip"))
        with pyzipper.AESZipFile(zip_path, "w",
                                  compression=pyzipper.ZIP_STORED,
                                  encryption=pyzipper.WZ_AES) as zf:
            zf.setpassword(password.encode("utf-8"))
            zf.write(file_path, arcname=file_path.name)
        return zip_path

    return await loop.run_in_executor(None, _zip)


# ─────────────────────────── queue helpers ────────────────────────────

def pop_first_task() -> dict | None:
    if not QUEUE_FILE.exists():
        return None
    with open(QUEUE_FILE, "r", encoding="utf-8") as f:
        lines = [l for l in f if l.strip()]
    if not lines:
        return None
    task = json.loads(lines[0])
    with open(QUEUE_FILE, "w", encoding="utf-8") as f:
        f.writelines(lines[1:])
    return task


def save_processing(task: dict) -> None:
    with open(PROCESSING_FILE, "w", encoding="utf-8") as f:
        json.dump(task, f, ensure_ascii=False, indent=2)


def clear_processing() -> None:
    if PROCESSING_FILE.exists():
        PROCESSING_FILE.unlink()


def append_failed(task: dict, error: str) -> None:
    with open(FAILED_FILE, "a", encoding="utf-8") as f:
        f.write(json.dumps({"task": task, "error": error}, ensure_ascii=False) + "\n")


# ─────────────────────────── task processor ────────────────────────────

async def process_task(task: dict):
    task_type = task.get("type")
    caption = task.get("caption", "")
    safe_mode = task.get("safe_mode", False)
    zip_password = task.get("zip_password", "")

    if task_type == "local_file":
        local_path = Path(task.get("path", ""))
        if not local_path.exists():
            raise RuntimeError(f"فایل پیدا نشد: {local_path.name}")

    elif task_type == "direct_url":
        local_path = await download_url(task)

    else:
        raise RuntimeError("Unknown task type.")

    send_path = local_path

    if safe_mode and zip_password:
        push_status(task, "در حال تبدیل به فایل ZIP ...", "processing")
        try:
            send_path = await make_zip_with_password(local_path, zip_password)
        finally:
            try:
                local_path.unlink(missing_ok=True)
            except Exception:
                pass

    try:
        if is_cancelled(task):
            raise RuntimeError("ارسال لغو شد.")

        await send_with_retry(str(send_path), caption, task)
        push_status(task, "✅ فایل با موفقیت در روبیکا آپلود شد.", "done")

    finally:
        try:
            send_path.unlink(missing_ok=True)
        except Exception:
            pass


# ─────────────────────────── worker loop ────────────────────────────

async def worker_loop():
    print("Rubika worker started.")

    while True:
        if should_reset():
            print("Reset flag detected — clearing state.")
            clear_reset_flag()
            clear_processing()
            await asyncio.sleep(0.5)
            continue

        task = pop_first_task()

        if not task:
            await asyncio.sleep(0.2)
            continue

        save_processing(task)

        try:
            await process_task(task)
        except Exception as e:
            err_msg = str(e)
            if "لغو" not in err_msg:
                append_failed(task, err_msg)
            push_status(task, f"❌ {err_msg}", "failed")
        finally:
            clear_processing()


# ─────────────────────────── entry point ────────────────────────────

async def main():
    # Create a proper Task so rubpy's internal asyncio.timeout() works correctly
    await asyncio.create_task(worker_loop())


if __name__ == "__main__":
    ensure_session()
    asyncio.run(main())
