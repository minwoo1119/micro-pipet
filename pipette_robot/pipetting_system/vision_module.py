"""
Vision Layer - AprilTag detection + pose estimation

이 모듈은 카메라 프레임에서 AprilTag를 검출하고, 포즈(R, t)를 추정하여
다른 계층(`AutomationController`)이 사용할 수 있는 형태로 제공합니다.

핵심 클래스:
- `TagDetectorWrapper`: 카메라에서 프레임을 가져와 태그 검출/포즈추정/오버레이를 수행하고,
  결과를 `latest_result`에 저장합니다.

좌표/보정 주의:
- `pupil_apriltags`가 추정한 `pose_R`에 대해, 현장 부착 오차를 보정하는
  `WellPlateConfig.TAG_*_OFFSET_DEG`(yaw/pitch/roll)를 추가 적용합니다.
"""

import math
import cv2
import numpy as np
from scipy.spatial.transform import Rotation as R
from pupil_apriltags import Detector
from config import WellPlateConfig

def draw_axes(img, K, dist, rvec, tvec, length=0.03):
    """3D 축을 이미지에 그리기"""
    axes_3d = np.float32([[0, 0, 0], [length, 0, 0], [0, length, 0], [0, 0, length]]).reshape(-1, 3)
    try:
        pts, _ = cv2.projectPoints(axes_3d, rvec, tvec, K, dist)
        p0, px, py, pz = [tuple(map(int, p.ravel())) for p in pts]
        cv2.line(img, p0, px, (0, 0, 255), 3)  # X: Red
        cv2.line(img, p0, py, (0, 255, 0), 3)  # Y: Green
        cv2.line(img, p0, pz, (255, 0, 0), 3)  # Z: Blue
        cv2.circle(img, p0, 4, (255, 255, 255), -1)
    except cv2.error:
        pass

def rmat_to_euler_zyx(R_mat):
    """회전 행렬을 ZYX 오일러 각도로 변환"""
    if abs(R_mat[2,0]) < 1.0:
        pitch = -math.asin(R_mat[2,0])
        roll = math.atan2(R_mat[2,1], R_mat[2,2])
        yaw = math.atan2(R_mat[1,0], R_mat[0,0])
    else:
        pitch = math.pi/2 if R_mat[2,0] <= -1 else -math.pi/2
        roll = 0.0
        yaw = math.atan2(-R_mat[0,1], R_mat[1,1])
    return np.rad2deg([yaw, pitch, roll])

def draw_text_with_bg(img, text, pos, font_scale=0.6, text_color=(255, 255, 255), bg_color=(0, 0, 0)):
    """배경이 있는 텍스트 그리기"""
    font = cv2.FONT_HERSHEY_SIMPLEX
    thickness = 2
    (text_w, text_h), baseline = cv2.getTextSize(text, font, font_scale, thickness)
    x, y = pos
    cv2.rectangle(img, (x - 5, y - text_h - 5), (x + text_w + 5, y + baseline + 5), bg_color, -1)
    cv2.putText(img, text, (x, y), font, font_scale, text_color, thickness)

class TagDetectorWrapper:
    def __init__(self, camera):
        """AprilTag 검출기 래퍼

        - `camera`는 `get_frame()` / `get_intrinsics()`를 제공해야 합니다.
        - Alvium 사용 시: `alvium_driver.AlviumCamera`
        - 테스트/대체 시: `main.py`에서 정의한 fallback 카메라
        """
        self.cam = camera
        self.detector = Detector(families=WellPlateConfig.TAG_FAMILY, nthreads=4, quad_decimate=2.0, refine_edges=1, decode_sharpening=0.25)
        self.latest_result = None

    @staticmethod
    def apply_tag_rotation_offset(r_mat: np.ndarray) -> np.ndarray:
        """AprilTag 회전 오프셋 보정 적용"""
        R_offset = R.from_euler('zyx', [WellPlateConfig.TAG_YAW_OFFSET_DEG, WellPlateConfig.TAG_PITCH_OFFSET_DEG, WellPlateConfig.TAG_ROLL_OFFSET_DEG], degrees=True).as_matrix()
        return r_mat @ R_offset

    def process_latest_frame(self):
        """최신 프레임을 처리하여 태그 포즈 정보 반환

        반환(dict) 주요 키:
        - `frame`: 원본 BGR 프레임
        - `display_frame`: 디버그 오버레이가 포함된 프레임(축/텍스트/태그 박스)
        - `K`, `dist`: 카메라 내참/왜곡(현재 구현은 dist=0 가정)
        - `tag_pose_info`: 태그가 있으면 dict, 없으면 None
        - `timestamp`: 카메라에서 읽은 시점(time.time 기반)

        인수인계/운영 전제:
        - 여러 태그가 검출되는 경우에도 현재 구현은 `detections[0]` 1개만 사용합니다.
          (현장에서는 "시야 내 태그는 1개"라는 전제를 두는 편이 안전합니다.)
        - 결과 공유 방식은 `self.latest_result`에 "마지막 처리 결과"를 덮어쓰는 형태입니다.
          다른 계층은 이를 폴링하므로, 타임스탬프/프레임 신선도 조건(`min_timestamp`)이 중요합니다.
        """
        if not hasattr(self.cam, 'get_frame'):
            return None
        frame, timestamp = self.cam.get_frame()
        if frame is None:
            return None

        K, dist = self.cam.get_intrinsics(WellPlateConfig.LENS_FOCAL_LENGTH_MM, WellPlateConfig.SENSOR_PIXEL_SIZE_MM)
        camera_params = [float(K[0,0]), float(K[1,1]), float(K[0,2]), float(K[1,2])]

        gray = cv2.cvtColor(frame, cv2.COLOR_BGR2GRAY)
        detections = self.detector.detect(gray, estimate_tag_pose=True, camera_params=camera_params, tag_size=WellPlateConfig.TAG_SIZE_M)

        tag_pose_info = None
        display_frame = frame.copy()

        if detections:
            det = detections[0]
            for i in range(4):
                cv2.line(display_frame, tuple(det.corners[i].astype(int)), tuple(det.corners[(i+1)%4].astype(int)), (0, 255, 255), 3)

            r_mat_corr = self.apply_tag_rotation_offset(det.pose_R)
            t_vec = det.pose_t.reshape(3, 1)

            r_vec, _ = cv2.Rodrigues(r_mat_corr)
            draw_axes(display_frame, K, dist, r_vec, t_vec, length=WellPlateConfig.TAG_SIZE_M * 2.0)

            tag_pose_info = {
                'id': det.tag_id, 'R_mat': r_mat_corr, 't_vec': t_vec,
                'center_px': (float(det.center[0]), float(det.center[1]))
            }

            cX, cY = map(int, det.center)
            x_mm, y_mm, z_mm = t_vec.flatten() * 1000.0
            yaw, pitch, roll = rmat_to_euler_zyx(r_mat_corr)

            draw_text_with_bg(display_frame, f"ID: {det.tag_id}", (cX - 20, cY - 40), text_color=(255, 255, 0))
            draw_text_with_bg(display_frame, f"X:{x_mm:.1f} Y:{y_mm:.1f} Z:{z_mm:.1f} mm", (cX - 60, cY + 40), text_color=(0, 255, 0))
            draw_text_with_bg(display_frame, f"Yaw:{yaw:.1f} P:{pitch:.1f} R:{roll:.1f} deg", (cX - 60, cY + 70), text_color=(100, 200, 255))

        self.latest_result = {'frame': frame, 'display_frame': display_frame, 'K': K, 'dist': dist, 'tag_pose_info': tag_pose_info, 'timestamp': timestamp}
        return self.latest_result