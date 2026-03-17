"""GUI나 테스트 코드에서 단발성 작업을 호출할 때 사용하는 공용 worker 진입점."""

import argparse
import json
import os
import cv2

from worker.paths import (
    ensure_state_dir,
    FRAME_JPG_PATH,
    ROIS_JSON_PATH,
    OCR_TRT_PATH,
)
from worker.camera import capture_one_frame
from worker.yolo_worker import run_yolo_on_frame
from worker.ocr_trt import TRTWrapper, read_volume_trt
from worker.control_worker import run_to_target

print("[WORKER] worker.py entry", flush=True)

# ==================================================
# Utils
# ==================================================
def rotate_frame(frame, rotate_code: int):
    """카메라 방향 차이로 ROI 좌표계가 흔들리지 않도록 프레임 방향을 통일한다."""
    if rotate_code == 1:
        return cv2.rotate(frame, cv2.ROTATE_90_CLOCKWISE)
    if rotate_code == 2:
        return cv2.rotate(frame, cv2.ROTATE_90_COUNTERCLOCKWISE)
    if rotate_code == 3:
        return cv2.rotate(frame, cv2.ROTATE_180)
    return frame


def main():
    """인자로 지정된 작업 하나만 수행하고, 호출부가 읽기 쉽게 JSON 한 줄로 반환한다."""
    ap = argparse.ArgumentParser()

    # -------------------------------------------------
    # Vision only
    # -------------------------------------------------
    ap.add_argument("--camera", type=int, default=0)
    ap.add_argument("--rotate", type=int, default=1)
    ap.add_argument("--capture", action="store_true")
    ap.add_argument("--yolo", action="store_true")
    ap.add_argument("--reset-rois", action="store_true")
    ap.add_argument("--ocr", action="store_true")
    ap.add_argument("--ocr-auto-rois", action="store_true")
    ap.add_argument("--run-target", action="store_true")
    ap.add_argument("--target", type=int, default=0)

    args = ap.parse_args()
    ensure_state_dir()

    # -------------------------------------------------
    # Reset ROIs
    # -------------------------------------------------
    if args.reset_rois and os.path.exists(ROIS_JSON_PATH):
        try:
            os.remove(ROIS_JSON_PATH)
        except Exception:
            pass

    def capture_rotated():
        """캡처 직후 회전을 적용해 이후 YOLO/OCR이 항상 같은 방향의 프레임을 받게 한다."""
        frame = capture_one_frame(args.camera)
        return rotate_frame(frame, args.rotate)

    # -------------------------------------------------
    # Capture
    # -------------------------------------------------
    if args.capture:
        frame = capture_rotated()
        cv2.imwrite(FRAME_JPG_PATH, frame)
        print(json.dumps({"ok": True, "frame_path": FRAME_JPG_PATH}))
        return

    # -------------------------------------------------
    # YOLO
    # -------------------------------------------------
    if args.yolo:
        frame = capture_rotated()
        cv2.imwrite(FRAME_JPG_PATH, frame)

        rois, annotated_path = run_yolo_on_frame(frame)
        print(json.dumps({
            "ok": True,
            "rois": rois,
            "annotated_path": annotated_path,
        }))
        return

    # -------------------------------------------------
    # OCR
    # -------------------------------------------------
    if args.ocr:
        frame = capture_rotated()
        cv2.imwrite(FRAME_JPG_PATH, frame)

        if args.ocr_auto_rois and not os.path.exists(ROIS_JSON_PATH):
            # OCR은 기존 ROI를 재사용하는 구조이므로, 없을 때만 YOLO로 최초 ROI를 만든다.
            run_yolo_on_frame(frame)

        trt_model = TRTWrapper(OCR_TRT_PATH)
        volume = read_volume_trt(frame, trt_model)

        print(json.dumps({
            "ok": True,
            "volume": int(volume),
        }))
        return

    # -------------------------------------------------
    # Run to target (vision based)
    # -------------------------------------------------
    if args.run_target:
        # 여기서는 제어 판단 결과만 내보내고, 실제 모터 구동은 시리얼 세션을 가진 GUI가 맡는다.
        run_to_target(target=args.target, camera_index=args.camera)
        print(json.dumps({"ok": True}))
        return

    print(json.dumps({"ok": False, "error": "no action specified"}))


if __name__ == "__main__":
    main()
