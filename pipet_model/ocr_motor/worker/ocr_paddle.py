"""TensorRT 대신 PaddleOCR 경로를 사용할 때의 대체 OCR 모듈."""

import json
import os
import re
from typing import List, Optional, Tuple

import cv2
import numpy as np
from paddleocr import PaddleOCR

from worker.paths import ROIS_JSON_PATH

# (중요) 호스트 체크 끄기: 너 로그에 그대로 뜨고 있음
os.environ["PADDLE_PDX_DISABLE_MODEL_SOURCE_CHECK"] = "True"

# ✅ ROI crop 기반이므로 det=False / rec=True 로 rec-only 사용
_ocr = PaddleOCR(
    use_angle_cls=False,
    lang="en",
    det=False,
    rec=True,
)

# ----------------------------------------------------------
# ROI utils
# ----------------------------------------------------------
def load_rois() -> List[List[float]]:
    """YOLO가 마지막으로 저장한 ROI 목록을 읽어온다."""
    if not os.path.exists(ROIS_JSON_PATH):
        raise FileNotFoundError(f"ROIs not found: {ROIS_JSON_PATH}")
    with open(ROIS_JSON_PATH, "r", encoding="utf-8") as f:
        rois = json.load(f)
    return rois


# ----------------------------------------------------------
# OCR result parsing (robust)
# ----------------------------------------------------------
def _extract_digits_from_paddle_result(result) -> str:
    """버전별로 형태가 달라질 수 있는 Paddle 결과를 평탄화해 숫자만 추린다."""
    texts: List[str] = []

    def walk(x):
        if x is None:
            return
        if isinstance(x, str):
            texts.append(x)
            return
        if isinstance(x, (list, tuple)):
            # ('text', score) 형태
            if len(x) >= 1 and isinstance(x[0], str):
                texts.append(x[0])
            for it in x:
                walk(it)

    walk(result)

    merged = " ".join(texts)
    digits = re.findall(r"\d", merged)
    return "".join(digits)


# ----------------------------------------------------------
# Preprocess variants for 7-seg / low-contrast digits
# ----------------------------------------------------------
def _preprocess_variants(roi_bgr: np.ndarray) -> List[Tuple[str, np.ndarray]]:
    """현장에서 자주 나오는 저대비/7세그 케이스를 대비해 전처리 버전을 여러 개 만든다."""
    variants: List[Tuple[str, np.ndarray]] = []

    # 1) raw
    variants.append(("raw", roi_bgr))

    # gray
    g = cv2.cvtColor(roi_bgr, cv2.COLOR_BGR2GRAY)

    # blur + equalize + otsu
    g_blur = cv2.GaussianBlur(g, (3, 3), 0)
    g_eq = cv2.equalizeHist(g_blur)
    _, th = cv2.threshold(g_eq, 0, 255, cv2.THRESH_BINARY + cv2.THRESH_OTSU)
    th_bgr = cv2.cvtColor(th, cv2.COLOR_GRAY2BGR)
    variants.append(("otsu", th_bgr))

    # inverted
    th_inv = 255 - th
    th_inv_bgr = cv2.cvtColor(th_inv, cv2.COLOR_GRAY2BGR)
    variants.append(("otsu_inv", th_inv_bgr))

    # 2x upsample (small digits에 효과 큼)
    def up2(img: np.ndarray) -> np.ndarray:
        return cv2.resize(img, None, fx=2.0, fy=2.0, interpolation=cv2.INTER_CUBIC)

    variants.append(("raw_up2", up2(roi_bgr)))
    variants.append(("otsu_up2", up2(th_bgr)))
    variants.append(("otsu_inv_up2", up2(th_inv_bgr)))

    return variants


# ----------------------------------------------------------
# OCR one digit with fallbacks
# ----------------------------------------------------------
def ocr_one_digit(roi_bgr: np.ndarray, debug_save: bool = False, idx: int = -1) -> Optional[int]:
    """한 자리 ROI에서 성공할 때까지 여러 전처리 버전을 순차적으로 시도한다."""
    for tag, img in _preprocess_variants(roi_bgr):
        try:
            # ✅ rec-only (det=False)
            result = _ocr.ocr(img, det=False, rec=True)
            ds = _extract_digits_from_paddle_result(result)
            if ds:
                return int(ds[0])
        except Exception:
            continue

        if debug_save and idx >= 0:
            # 실패 케이스도 저장하면 튜닝에 도움됨
            cv2.imwrite(f"/tmp/paddle_roi_{idx}_{tag}.jpg", img)

    return None


# ----------------------------------------------------------
# Main read volume
# ----------------------------------------------------------
def read_volume_paddle(frame: np.ndarray, debug_save: bool = False) -> int:
    """ROI 4개를 읽어 TRT 경로와 같은 방식의 최종 용량값으로 합친다."""
    rois = load_rois()
    rois = sorted(rois, key=lambda r: r[1])  # 위→아래

    h, w = frame.shape[:2]
    digits: List[int] = []

    for i, (x, y, rw, rh) in enumerate(rois[:4]):
        x1 = max(0, min(w - 1, int(x)))
        y1 = max(0, min(h - 1, int(y)))
        x2 = max(0, min(w, x1 + int(rw)))
        y2 = max(0, min(h, y1 + int(rh)))

        crop = frame[y1:y2, x1:x2]
        if crop.size == 0:
            raise RuntimeError(f"Empty ROI{i}")

        if debug_save:
            cv2.imwrite(f"/tmp/paddle_roi_{i}.jpg", crop)

        d = ocr_one_digit(crop, debug_save=debug_save, idx=i)
        if d is None:
            raise RuntimeError(f"PaddleOCR failed on ROI{i}")
        digits.append(d)

    return digits[0] * 1000 + digits[1] * 100 + digits[2] * 10 + digits[3]
