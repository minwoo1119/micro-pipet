"""PaddleOCR 경로를 별도로 실행할 때 사용하는 worker 진입점."""

import argparse
import json
import os
import cv2

from worker.paths import (
    ensure_state_dir,
    FRAME_JPG_PATH,
    ROIS_JSON_PATH,
)
from worker.camera import capture_one_frame
from worker.yolo_worker import run_yolo_on_frame
from worker.ocr_paddle import read_volume_paddle

print("[WORKER] worker_paddle.py entry", flush=True)

def rotate_frame(frame, rotate_code: int):
    """PaddleOCR이 항상 같은 숫자 배치를 보도록 프레임 방향을 통일한다."""
    if rotate_code == 1:
        return cv2.rotate(frame, cv2.ROTATE_90_CLOCKWISE)
    if rotate_code == 2:
        return cv2.rotate(frame, cv2.ROTATE_90_COUNTERCLOCKWISE)
    if rotate_code == 3:
        return cv2.rotate(frame, cv2.ROTATE_180)
    return frame


def main():
    """Paddle 기반 작업 하나를 수행하고 호출부가 읽을 최종 JSON만 출력한다."""
    ap = argparse.ArgumentParser()

    ap.add_argument("--camera", type=int, default=0)
    ap.add_argument("--rotate", type=int, default=1)

    ap.add_argument("--capture", action="store_true")
    ap.add_argument("--yolo", action="store_true")
    ap.add_argument("--reset-rois", action="store_true")

    ap.add_argument("--ocr", action="store_true")
    ap.add_argument("--ocr-auto-rois", action="store_true")
    ap.add_argument("--debug-save", action="store_true")

    args = ap.parse_args()
    ensure_state_dir()

    if args.reset_rois and os.path.exists(ROIS_JSON_PATH):
        try:
            os.remove(ROIS_JSON_PATH)
        except Exception:
            pass

    def capture_rotated():
        """캡처 후 바로 회전을 적용해 저장/추론 단계가 같은 기준 프레임을 쓰게 한다."""
        frame = capture_one_frame(args.camera)
        return rotate_frame(frame, args.rotate)

    # Capture
    if args.capture:
        frame = capture_rotated()
        cv2.imwrite(FRAME_JPG_PATH, frame)
        print(json.dumps({"ok": True, "frame_path": FRAME_JPG_PATH}))
        return

    # YOLO
    if args.yolo:
        frame = capture_rotated()
        cv2.imwrite(FRAME_JPG_PATH, frame)
        rois, annotated_path = run_yolo_on_frame(frame)
        print(json.dumps({"ok": True, "rois": rois, "annotated_path": annotated_path}))
        return

    # OCR (PaddleOCR)
    if args.ocr:
        frame = capture_rotated()
        cv2.imwrite(FRAME_JPG_PATH, frame)

        if args.ocr_auto_rois and not os.path.exists(ROIS_JSON_PATH):
            # Paddle 경로도 ROI 재사용을 전제로 하므로, 최초 실행 시에만 YOLO를 보조적으로 사용한다.
            run_yolo_on_frame(frame)

        volume = read_volume_paddle(frame, debug_save=args.debug_save)

        print(json.dumps({"ok": True, "volume": int(volume)}))
        return

    print(json.dumps({"ok": False, "error": "no action specified"}))


if __name__ == "__main__":
    main()
