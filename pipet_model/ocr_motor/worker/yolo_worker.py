"""OCR에서 재사용할 숫자 ROI를 검출하고 저장하는 YOLO 모듈."""

import json
import cv2
from ultralytics import YOLO

from worker.paths import (
    YOLO_MODEL_PATH,
    ROIS_JSON_PATH,
    YOLO_JPG_PATH,
    ensure_state_dir,
)


def _sorted_rois_from_results(results, frame_shape):
    """YOLO 결과를 상단 기준 정렬된 ROI 리스트로 바꿔 후속 OCR이 그대로 쓰게 한다."""
    boxes = results.boxes
    if boxes is None or len(boxes) == 0:
        return []

    sorted_boxes = sorted(boxes, key=lambda b: float(b.xyxy[0][1]))

    rois = []
    h, w = frame_shape[:2]
    for b in sorted_boxes[:4]:
        x1, y1, x2, y2 = map(int, b.xyxy[0].tolist())
        x1 = max(0, min(w - 1, x1))
        x2 = max(0, min(w - 1, x2))
        y1 = max(0, min(h - 1, y1))
        y2 = max(0, min(h - 1, y2))
        rois.append([x1, y1, max(1, x2 - x1), max(1, y2 - y1)])
    return rois


def run_yolo_on_frame(frame, conf: float = 0.2, iou: float = 0.5):
    """프레임 한 장에서 ROI를 찾고, 후속 모듈이 쓸 좌표/미리보기 파일을 함께 저장한다."""
    ensure_state_dir()

    model = YOLO(YOLO_MODEL_PATH)
    result = model(frame, conf=conf, iou=iou, verbose=False)[0]

    rois = _sorted_rois_from_results(result, frame.shape)

    vis = frame.copy()
    for i, (x, y, w, h) in enumerate(rois):
        cv2.rectangle(vis, (x, y), (x + w, y + h), (0, 255, 0), 2)
        cv2.putText(
            vis,
            f"ROI{i}",
            (x, max(0, y - 8)),
            cv2.FONT_HERSHEY_SIMPLEX,
            0.7,
            (0, 255, 0),
            2,
        )

    cv2.imwrite(YOLO_JPG_PATH, vis)

    with open(ROIS_JSON_PATH, "w", encoding="utf-8") as f:
        json.dump(rois, f, ensure_ascii=False, indent=2)

    # 현 구조에서는 OpenCV 창을 띄우지 않고 GUI가 저장된 결과 이미지를 다시 읽어 보여준다.
    return rois, YOLO_JPG_PATH
