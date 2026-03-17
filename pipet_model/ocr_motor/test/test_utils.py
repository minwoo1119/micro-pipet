"""임시 OCR / 모터 테스트 스크립트에서 함께 쓰는 공용 헬퍼."""

import random
import os
import cv2
from datetime import datetime
from worker.camera import capture_one_frame

SNAPSHOT_DIR = "snapshots"

def ensure_dirs():
    """수동 테스트 스크립트에서 쓰는 snapshot, log 디렉터리를 만든다."""
    os.makedirs(SNAPSHOT_DIR, exist_ok=True)
    os.makedirs("logs", exist_ok=True)

def generate_random_target():
    """0.500 mL부터 5.000 mL 사이의 임의 목표값을 만든다."""
    value_ml = round(random.uniform(0.5, 5.0), 3)
    return int(value_ml * 1000), value_ml

def take_snapshot(order: int, value_ml: float):
    """테스트 순서와 목표 용량이 포함된 파일명으로 프레임을 저장한다."""
    frame = capture_one_frame(0)
    fname = f"{order:04d}_{value_ml:.3f}.jpg"
    path = os.path.join(SNAPSHOT_DIR, fname)
    cv2.imwrite(path, frame)
    return path
