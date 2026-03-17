"""YOLO와 OCR worker에서 공통으로 쓰는 단일 프레임 캡처 헬퍼."""

import cv2
import time

def capture_one_frame(camera_index: int = 0, warmup_frames: int = 10):
    """카메라를 열고 잠시 워밍업한 뒤 마지막으로 정상 읽힌 프레임을 반환한다."""
    cap = cv2.VideoCapture(camera_index)
    if not cap.isOpened():
        raise RuntimeError(f"Camera open failed: index={camera_index}")
    
    # ROI 좌표와 이후 OCR 프레임이 어긋나지 않도록 캡처 해상도를 고정한다.
    cap.set(cv2.CAP_PROP_FRAME_WIDTH, 1280)
    cap.set(cv2.CAP_PROP_FRAME_HEIGHT, 800)

    frame = None
    for _ in range(max(1, warmup_frames)):
        ok, fr = cap.read()
        if ok:
            frame = fr
        time.sleep(0.01)

    cap.release()

    if frame is None:
        raise RuntimeError("Failed to capture frame.")
    

    return frame
