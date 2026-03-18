"""
Alvium Camera Driver (vmbpy)

이 모듈은 Allied Vision Alvium 카메라를 `vmbpy`로 구동하는 최소 래퍼입니다.

특징:
- 별도 스레드에서 스트리밍 루프를 돌며, 가장 최신 프레임을 메모리에 보관합니다.
- 상위 계층(`vision_module.TagDetectorWrapper`)은 `get_frame()`으로 최신 프레임을 가져갑니다.

주의:
- `vmbpy` 및 카메라 SDK/권한 설정이 되어있지 않으면 ImportError/RuntimeError가 발생할 수 있습니다.
- 프레임은 deepcopy로 반환하므로(안전), 고프레임레이트 환경에선 성능에 영향이 있을 수 있습니다.
"""

import threading
import copy
import time
import numpy as np
import cv2
from vmbpy import *

class AlviumCamera:
    def __init__(self, camera_index=0, width=2464, height=2064):
        """Alvium 카메라 스트리밍 래퍼

        - `camera_index`: VmbSystem에서 발견된 카메라 배열 인덱스
        - `width`, `height`: 요청 해상도(카메라가 지원하지 않으면 실패/무시될 수 있음)
        """
        self.camera_index = camera_index
        self.req_width = width
        self.req_height = height
        
        # 스레드 제어 변수
        self.running = False
        self.thread = None
        self.lock = threading.Lock()
        
        # 데이터 변수
        self.frame = None
        self.vmb = VmbSystem.get_instance()
        
    def start(self):
        """카메라 스트리밍 스레드 시작"""
        if self.running:
            return
        
        self.running = True
        self.thread = threading.Thread(target=self._run_camera_loop)
        self.thread.daemon = True  # 메인 프로그램 종료 시 함께 종료
        self.thread.start()
        print(f"[AlviumDriver] Camera index {self.camera_index} started.")

    def stop(self):
        """카메라 스트리밍 중지"""
        self.running = False
        if self.thread and self.thread.is_alive():
            self.thread.join()
        print("[AlviumDriver] Camera stopped.")

    def get_frame(self):
        """
        가장 최근 프레임을 반환 (Thread-safe)
        Return: (frame_image, timestamp) or (None, 0)
        """
        with self.lock:
            if self.frame is not None:
                return copy.deepcopy(self.frame), time.time()
            return None, 0

    def get_intrinsics(self, focal_length_mm=8.0, pixel_size_mm=0.00274):
        """
        렌즈 스펙 기반으로 내부 파라미터(K, dist) 반환
        """
        # 현재 프레임이 없으면 요청 해상도 기준, 있으면 실제 해상도 기준
        w, h = self.req_width, self.req_height
        if self.frame is not None:
            h, w = self.frame.shape[:2]

        fx = focal_length_mm / pixel_size_mm
        fy = fx
        cx = w / 2.0
        cy = h / 2.0
        
        K = np.array([[fx, 0, cx], [0, fy, cy], [0, 0, 1]], dtype=np.float32)
        dist = np.zeros(5, dtype=np.float32) # 왜곡은 0으로 가정
        return K, dist

    def _setup_camera(self, cam: Camera):
        """카메라 세부 설정 (내부 호출용)"""
        with cam:
            # 1. Binning/Decimation 해제 (해상도 확보)
            try:
                # BinningHorizontal 등이 있으면 1로 리셋
                if 'BinningHorizontal' in cam.get_features():
                    cam.get_feature_by_name('BinningHorizontal').set(1)
                if 'BinningVertical' in cam.get_features():
                    cam.get_feature_by_name('BinningVertical').set(1)
            except Exception:
                pass

            # 2. 해상도 설정
            try:
                cam.get_feature_by_name('Width').set(self.req_width)
                cam.get_feature_by_name('Height').set(self.req_height)
            except Exception as e:
                print(f"[Warning] Resolution setup failed: {e}")

            # 3. 픽셀 포맷 (BayerRG8 -> PC에서 변환이 가장 빠름)
            try:
                cam.set_pixel_format(PixelFormat.BayerRG8)
            except:
                print("[Warning] BayerRG8 not supported. Using default.")

            # 4. 자동 노출 및 화이트 밸런스
            try:
                if cam.get_feature_by_name('ExposureAuto').get_access_mode()[1]:
                    cam.get_feature_by_name('ExposureAuto').set('Continuous')
                if cam.get_feature_by_name('BalanceWhiteAuto').get_access_mode()[1]:
                    cam.get_feature_by_name('BalanceWhiteAuto').set('Continuous')
            except:
                pass

    def _run_camera_loop(self):
        """실제 카메라 데이터를 받아오는 루프 (별도 스레드)"""
        with self.vmb:
            cameras = self.vmb.get_all_cameras()
            if not cameras:
                print("[Error] No cameras found!")
                self.running = False
                return

            try:
                cam = cameras[self.camera_index]
            except IndexError:
                print(f"[Error] Camera index {self.camera_index} out of range.")
                self.running = False
                return

            print(f"[AlviumDriver] Connected to ID: {cam.get_id()}")
            self._setup_camera(cam)

            with cam:
                # 스트리밍 시작
                try:
                    for frame in cam.get_frame_generator(limit=None, timeout_ms=2000):
                        if not self.running:
                            break
                        
                        if frame.get_status() == FrameStatus.Complete:
                            # 포맷 변환 (Bayer -> BGR)
                            frame_color = frame.convert_pixel_format(PixelFormat.Bgr8)
                            img = frame_color.as_opencv_image()
                            
                            # 최신 프레임 업데이트 (Lock 사용)
                            with self.lock:
                                self.frame = img
                        else:
                            # 프레임 드랍/에러 시 처리 (필요하면 추가)
                            pass
                except Exception as e:
                    print(f"[Error] Streaming error: {e}")
                    self.running = False