"""GUI, worker, test 모듈이 함께 쓰는 파일 경로 상수 모음."""

import os

ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
MODELS_DIR = os.path.join(ROOT_DIR, "models")
STATE_DIR = os.path.join(ROOT_DIR, "state")

YOLO_MODEL_PATH = os.path.join(MODELS_DIR, "yolo", "best_rotate_yolo.pt")
# OCR_TRT_PATH    = os.path.join(MODELS_DIR, "ocr", "efficientnet_b0_fp16_dynamic.trt")
OCR_TRT_PATH    = os.path.join(MODELS_DIR, "ocr", "finetuned_efficientnet_b0_trtmatch_fp16_dynamic.trt")
# OCR_TRT_PATH    = os.path.join(MODELS_DIR, "ocr", "finetuned_efficientnet_b0_fp16_dynamic.trt")

ROIS_JSON_PATH  = os.path.join(STATE_DIR, "rois.json")
FRAME_JPG_PATH  = os.path.join(STATE_DIR, "last_frame.jpg")
YOLO_JPG_PATH   = os.path.join(STATE_DIR, "last_yolo.jpg")

def ensure_state_dir():
    """캡처 프레임과 ROI JSON을 저장하는 공용 state 디렉터리를 만든다."""
    os.makedirs(STATE_DIR, exist_ok=True)
