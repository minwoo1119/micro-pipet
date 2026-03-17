"""카메라 프레임 1장을 worker 공용 state 디렉터리에 저장하는 유틸 스크립트."""

import os
import cv2

from worker.camera import capture_one_frame

OUTPUT_PATH = os.path.join(
    os.path.dirname(__file__),
    "..",
    "state",
    "last_frame.jpg"
)

def capture_one_frame_to_disk(camera_index=0):
    """프레임 1장을 캡처해 GUI가 최신 preview로 읽는 위치에 저장한다."""
    frame = capture_one_frame(camera_index)
    os.makedirs(os.path.dirname(OUTPUT_PATH), exist_ok=True)
    cv2.imwrite(OUTPUT_PATH, frame)
    print(f"[worker] frame saved to {OUTPUT_PATH}")


if __name__ == "__main__":
    capture_one_frame_to_disk()
