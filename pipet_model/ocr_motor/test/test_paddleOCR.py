"""테스트 스크립트에서 PaddleOCR worker를 호출하기 위한 단독 헬퍼."""

import subprocess
import json
import sys

OCR_TIMEOUT = 30

def read_ocr_volume_paddle(camera_index=0, rotate=1, auto_rois=True, debug_save=False) -> int:
    """Paddle worker를 1회 실행하고 JSON 출력에서 용량 값을 꺼내 반환한다."""
    cmd = [
        sys.executable, "-m", "worker.worker_paddle",
        "--ocr",
        f"--camera={camera_index}",
        f"--rotate={rotate}",
    ]
    if auto_rois:
        cmd.append("--ocr-auto-rois")
    if debug_save:
        cmd.append("--debug-save")

    p = subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )

    stdout, stderr = p.communicate(timeout=OCR_TIMEOUT)

    # Paddle 초기화 로그가 stderr로 나와 디버깅에 유용하므로 그대로 출력한다.
    print("[stderr]\n", stderr)

    for line in stdout.splitlines():
        try:
            msg = json.loads(line)
            if msg.get("ok"):
                return int(msg["volume"])
        except Exception:
            continue

    raise RuntimeError(f"PaddleOCR worker failed.\nstdout:\n{stdout}\nstderr:\n{stderr}")


if __name__ == "__main__":
    vol = read_ocr_volume_paddle(camera_index=0, rotate=1, auto_rois=True, debug_save=True)
    print(f"[PADDLE OCR VOLUME] {vol}")
