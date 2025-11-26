import os
import time
from contextlib import suppress
from ftplib import FTP, error_perm, error_proto
from pathlib import Path
from typing import Any, cast


def download_report(ftp_config: dict[str, str], remote_path: str, destination: Path) -> None:
    try:
        retry_count = max(1, int(os.getenv("FTP_MAX_RETRIES", "3")))
    except ValueError:
        retry_count = 3

    try:
        retry_delay = float(os.getenv("FTP_RETRY_DELAY_SECONDS", "5"))
    except ValueError:
        retry_delay = 5.0

    last_exc: Exception | None = None

    for attempt in range(1, retry_count + 1):
        try:
            _download_report_once(ftp_config, remote_path, destination)
            return
        except FileNotFoundError:
            raise
        except Exception as exc:
            last_exc = exc
            if attempt < retry_count:
                wait_seconds = max(0.0, retry_delay)
                print(
                    f"  - FTP attempt {attempt} failed ({exc}); retrying in {wait_seconds:.1f}s..."
                )
                if wait_seconds > 0:
                    time.sleep(wait_seconds)
            else:
                raise exc

    if last_exc:
        raise last_exc


def _fetch_report(
    ftp_config: dict[str, str],
    remote_path: str,
    destination: Path,
    *,
    use_epsv: bool,
    passive: bool,
) -> None:
    host = ftp_config["host"]
    port = int(ftp_config["port"])
    username = ftp_config["username"]
    password = ftp_config["password"]

    if destination.exists():
        destination.unlink()

    with FTP() as ftp:
        ftp.connect(host=host, port=port, timeout=30)
        ftp.login(user=username, passwd=password)

        ftp_any = cast(Any, ftp)
        if hasattr(ftp_any, "use_epsv"):
            ftp_any.use_epsv = use_epsv
        ftp.set_pasv(passive)

        path_parts = remote_path.strip("/").split("/")
        directories = path_parts[:-1]
        filename = path_parts[-1]

        def reset_root() -> None:
            with suppress(error_perm):
                ftp.cwd("/")

        reset_root()

        try:
            for directory in directories:
                if directory:
                    ftp.cwd(directory)

            with destination.open("wb") as file_obj:
                ftp.retrbinary(f"RETR {filename}", file_obj.write)
        except error_perm as exc:
            if str(exc).startswith("550"):
                raise FileNotFoundError(f"FTP file not found: {remote_path}") from exc
            raise
        finally:
            reset_root()


def _download_report_once(ftp_config: dict[str, str], remote_path: str, destination: Path) -> None:
    modes = [
        (False, True),
        (False, False),
        (True, True),
    ]

    last_proto_exc: Exception | None = None

    for use_epsv, passive in modes:
        try:
            _fetch_report(
                ftp_config,
                remote_path,
                destination,
                use_epsv=use_epsv,
                passive=passive,
            )
            return
        except error_proto as exc:
            if "Extended Passive Mode" in str(exc):
                last_proto_exc = exc
                continue
            raise

    if last_proto_exc:
        raise last_proto_exc
