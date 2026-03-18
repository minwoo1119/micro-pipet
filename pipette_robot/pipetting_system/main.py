#!/usr/bin/env python3

"""
UR Robot Well Plate Automation - Tkinter GUI Entry

이 파일은 운영자가 실행하는 단일 진입점입니다.

아키텍처 개요(한 프로세스):
- Tkinter GUI(`CalibrationApp`): 버튼/상태/영상 표시
- ROS2 노드(`URInterface`): TF 조회, MoveIt IK 서비스(/compute_ik), Trajectory Action 전송
- 카메라(`AlviumCamera`): 별도 스레드 스트리밍, 최신 프레임 제공
- 비전(`TagDetectorWrapper`): AprilTag 검출/포즈추정, 최신 결과 공유
- 자동화(`AutomationController`): 센터링, Well 이동, 피펫 시퀀스 로직

스레딩 주의:
- ROS2 executor는 별도 스레드에서 spin 합니다.
- 카메라 스트리밍은 별도 스레드에서 동작합니다.
- Tkinter는 메인 스레드에서만 UI 업데이트를 해야 합니다(`_ui_tick`).
"""

import os
import math
import time
import threading
import cv2
import rclpy
from rclpy.executors import SingleThreadedExecutor
import tkinter as tk
from tkinter import ttk
from PIL import Image, ImageTk

from robot_interface import URInterface
from vision_module import TagDetectorWrapper
from automation_controller import AutomationController
from config import RobotConfig, WellPlateConfig, ControlParams, PipetteConfig

# 카메라 드라이버 로드
try:
    from alvium_driver import AlviumCamera
except ImportError:
    print("Warning: 'alvium_driver.py' not found. Camera will not work.")
    class AlviumCamera:
        def start(self): pass
        def stop(self): pass
        def get_frame(self): return None, 0
        def get_intrinsics(self, *args):
            import numpy as np
            return np.array([[600.0, 0, 320.0], [0, 600.0, 240.0], [0, 0, 1]]), np.zeros((5,))

class CalibrationApp:
    def __init__(self, robot: URInterface, detector: TagDetectorWrapper, controller: AutomationController):
        self.robot = robot
        self.detector = detector
        self.controller = controller
        self.controller.status_callback = self._update_status

        if not os.path.exists(RobotConfig.SAVE_DIR):
            os.makedirs(RobotConfig.SAVE_DIR)

        self._exec = SingleThreadedExecutor(context=robot.context)
        self._exec.add_node(robot)
        self._spin_th = threading.Thread(target=self._exec.spin, daemon=True)
        self._spin_th.start()

        self.root = tk.Tk()
        self.root.title("UR 통합 제어 시스템 (Revolver Robot)")
        
        # --- [추가] 전체 창 크기 설정 ---
        self.root.geometry("1400x850") 
        self._going_down = False
        self._setup_ui()
        self._ui_tick()
        self.root.protocol("WM_DELETE_WINDOW", self._on_close)

    def _update_status(self, message):
        self.status.set(message)

    def _setup_ui(self):
        """좌측: 카메라 / 우측: 제어 패널 레이아웃 구성"""
        
        # 1. 메인 컨테이너 (PanedWindow로 좌우 조절 가능하게 설정)
        self.paned = ttk.PanedWindow(self.root, orient=tk.HORIZONTAL)
        self.paned.pack(fill="both", expand=True, padx=10, pady=10)

        # ----------------- [좌측: 카메라 섹션] -----------------
        self.video_container = ttk.LabelFrame(self.paned, text="실시간 카메라 피드", padding=5)
        self.paned.add(self.video_container, weight=3) # 가중치 더 높게

        self.video_label = ttk.Label(self.video_container)
        self.video_label.pack(expand=True, fill="both")

        # ----------------- [우측: 제어 섹션] -----------------
        self.main_frame = ttk.Frame(self.paned, padding=10)
        self.paned.add(self.main_frame, weight=1)

        # --- 상태 표시줄 ---
        self.status = tk.StringVar(value="시스템 준비 완료")
        ttk.Label(self.main_frame, textvariable=self.status, foreground="blue", font=("Arial", 11, "bold")).grid(row=0, column=0, columnspan=6, sticky="w", pady=(0, 10))

        # --- Well Plate Navigation (기존 코드와 동일하지만 위치는 main_frame) ---
        group_nav = ttk.LabelFrame(self.main_frame, text="Well Plate 이동 (4x6 Grid)", padding=10)
        group_nav.grid(row=1, column=0, columnspan=6, sticky="ew", pady=5)
        row_labels = ['A', 'B', 'C', 'D']
        for c in range(WellPlateConfig.WELL_COLS):
            ttk.Label(group_nav, text=f"{c+1}", font=("Arial", 8, "bold")).grid(row=0, column=c+1, padx=3)
        for r in range(WellPlateConfig.WELL_ROWS):
            ttk.Label(group_nav, text=row_labels[r], font=("Arial", 8, "bold")).grid(row=r+1, column=0, padx=5)
            for c in range(WellPlateConfig.WELL_COLS):
                ttk.Button(group_nav, text=f"{row_labels[r]}{c+1}", width=5, 
                           command=lambda row=r, col=c: self.controller.move_to_well(row, col)).grid(row=r+1, column=c+1, padx=2, pady=2)

        # --- 수동 제어 도구 (Manual Control) ---
        group_tools = ttk.LabelFrame(self.main_frame, text="로봇 및 툴 수동 제어", padding=10)
        group_tools.grid(row=2, column=0, columnspan=6, sticky="ew", pady=5)
        
        # Joint Control
        self.joint_rows, self.joint_step = [], tk.DoubleVar(value=RobotConfig.DEFAULT_JOINT_STEP_RAD)
        self.time_sec = tk.DoubleVar(value=RobotConfig.DEFAULT_TIME_SEC)
        for i in range(6):
            ttk.Label(group_tools, text=f"J{i+1}").grid(row=i, column=0, sticky="e")
            v = tk.StringVar(value="--")
            ttk.Label(group_tools, textvariable=v, width=6).grid(row=i, column=1)
            self.joint_rows.append(v)
            ttk.Button(group_tools, text="-", width=2, command=lambda k=i: self._jog_joint(k, -1)).grid(row=i, column=2)
            ttk.Button(group_tools, text="+", width=2, command=lambda k=i: self._jog_joint(k, 1)).grid(row=i, column=3)

        # Cartesian Control (X, Y, Z)
        self.tcp_vars, self.cart_step = {k: tk.StringVar(value="0.00") for k in ['x','y','z']}, tk.DoubleVar(value=RobotConfig.DEFAULT_CART_STEP_M)
        for i, axis in enumerate(['X', 'Y', 'Z']):
            ttk.Label(group_tools, text=axis).grid(row=i, column=4, sticky="e", padx=5)
            ttk.Label(group_tools, textvariable=self.tcp_vars[axis.lower()], width=6).grid(row=i, column=5)
            ttk.Button(group_tools, text="-", width=2, command=lambda a=axis: self._jog_pose(a, -1)).grid(row=i, column=6)
            ttk.Button(group_tools, text="+", width=2, command=lambda a=axis: self._jog_pose(a, 1)).grid(row=i, column=7)

        # Home & Center
        self.home_lift_z = tk.DoubleVar(value=0.21)
        ttk.Label(group_tools, text="Lift Z(m):").grid(row=7, column=0, sticky="e", pady=5)
        ttk.Entry(group_tools, textvariable=self.home_lift_z, width=5).grid(row=7, column=1, sticky="w")
        ttk.Button(group_tools, text="홈 포지션", command=self._go_home_sequence).grid(row=7, column=2, columnspan=2, sticky="ew")
        tk.Button(group_tools, text="태그 중앙 정렬", bg="#DDDDDD", command=self._track_tag_once).grid(row=7, column=4, columnspan=4, sticky="ew")

        # Tag Pose Status
        self.tag_status, self.tag_pos_x, self.tag_pos_y, self.tag_rot_yaw = tk.StringVar(value="Waiting..."), tk.StringVar(value="X: ---"), tk.StringVar(value="Y: ---"), tk.StringVar(value="Yaw: ---")
        ttk.Label(self.main_frame, textvariable=self.tag_status, foreground="orange", font=("Arial", 9, "bold")).grid(row=3, column=0, columnspan=6, sticky="w", pady=(10, 0))
        ttk.Label(self.main_frame, textvariable=self.tag_pos_x, width=15).grid(row=4, column=0, columnspan=2)
        ttk.Label(self.main_frame, textvariable=self.tag_pos_y, width=15).grid(row=4, column=2, columnspan=2)
        ttk.Label(self.main_frame, textvariable=self.tag_rot_yaw, width=15).grid(row=4, column=4, columnspan=2)

        # Pipette Control
        group_pip = ttk.LabelFrame(self.main_frame, text="피펫 분주 시퀀스", padding=10)
        group_pip.grid(row=5, column=0, columnspan=6, sticky="ew", pady=10)
        self.pip_offset_x, self.pip_offset_y, self.pip_down_z = tk.DoubleVar(value=-PipetteConfig.PIPETTE_OFFSET_X), tk.DoubleVar(value=PipetteConfig.PIPETTE_OFFSET_Y), tk.DoubleVar(value=PipetteConfig.PIPETTE_DOWN_Z)
        ttk.Label(group_pip, text="X 오프셋:").grid(row=0, column=0)
        ttk.Entry(group_pip, textvariable=self.pip_offset_x, width=6).grid(row=0, column=1)
        ttk.Label(group_pip, text="Y 오프셋:").grid(row=0, column=2)
        ttk.Entry(group_pip, textvariable=self.pip_offset_y, width=6).grid(row=0, column=3)
        ttk.Label(group_pip, text="하강 Z:").grid(row=0, column=4)
        ttk.Entry(group_pip, textvariable=self.pip_down_z, width=6).grid(row=0, column=5)
        ttk.Button(group_pip, text="▶ 시퀀스 실행 (오프셋 -> 하강 -> 복귀)", command=self._run_pipette_sequence).grid(row=1, column=0, columnspan=6, sticky="ew", pady=5)
    


    def _jog_joint(self, idx, sign):
        """관절 조그"""
        if not self.robot.last_q:
            return
        q = list(self.robot.last_q)
        q[idx] += sign * float(self.joint_step.get())
        self.robot.send_traj(q, float(self.time_sec.get()))

    def _jog_pose(self, axis, sign):
        """TCP 포즈 조그"""
        try:
            t = self.robot.tf_buffer.lookup_transform(RobotConfig.BASE_LINK, RobotConfig.EE_LINK, rclpy.time.Time())
            pos, step = [t.transform.translation.x, t.transform.translation.y, t.transform.translation.z], float(self.cart_step.get())
            if axis == 'X':
                pos[0] += sign * step
            elif axis == 'Y':
                pos[1] += sign * step
            elif axis == 'Z':
                pos[2] += sign * step
            self.robot.solve_ik_and_move(pos, t.transform.rotation)
        except:
            pass

    def _track_tag_once(self):
        """태그 중앙 정렬만 수행"""
        threading.Thread(target=self.controller.center_tag_and_get_latest_pose, args=(5.0,), daemon=True).start()

    def _go_home_sequence(self):
        """홈 포즈로 이동"""
        def seq():
            self.robot.send_traj(RobotConfig.HOME_JOINTS, 2.0)
            time.sleep(2.5)
            tf = self.robot.tf_buffer.lookup_transform(RobotConfig.BASE_LINK, RobotConfig.EE_LINK, rclpy.time.Time())
            pos = [tf.transform.translation.x, tf.transform.translation.y, tf.transform.translation.z + float(self.home_lift_z.get())]
            self.robot.solve_ik_and_move(pos, tf.transform.rotation)
        threading.Thread(target=seq, daemon=True).start()

    def _run_pipette_sequence(self):
        """피펫 시퀀스 실행"""
        self.controller.run_pipette_sequence(
            self.pip_offset_x.get(),
            self.pip_offset_y.get(),
            self.pip_down_z.get()
        )

    def _ui_tick(self):
        """UI 루프: 관절값 업데이트 및 OpenCV 영상을 Tkinter 라벨에 투영

        주의:
        - Tkinter 위젯 업데이트는 반드시 메인 스레드에서 수행해야 합니다.
        - 비전 처리는 `TagDetectorWrapper.process_latest_frame()` 호출로 최신 프레임 1장을 처리합니다.
        """
        # 1. 로봇 관절 및 TCP 좌표 업데이트 (기존 로직)
        if self.robot.joint_order and self.robot.last_q:
            for i in range(len(self.robot.joint_order.names)):
                self.joint_rows[i].set(f"{math.degrees(self.robot.last_q[i]):.1f}")
        try:
            t = self.robot.tf_buffer.lookup_transform(RobotConfig.BASE_LINK, RobotConfig.EE_LINK, rclpy.time.Time())
            self.tcp_vars['x'].set(f"{t.transform.translation.x:.3f}")
            self.tcp_vars['y'].set(f"{t.transform.translation.y:.3f}")
            self.tcp_vars['z'].set(f"{t.transform.translation.z:.3f}")
        except: pass

        # 2. 카메라 영상 처리 (가변 리사이징 적용)
        res = self.detector.process_latest_frame()
        if res:
            display_img = res['display_frame'].copy()
            
            # 중앙 조준선 그리기 (원본 해상도 기준)
            h, w, _ = display_img.shape
            cx, cy = w // 2, h // 2
            cv2.line(display_img, (cx-20, cy), (cx+20, cy), (0, 0, 255), 2)
            cv2.line(display_img, (cx, cy-20), (cx, cy+20), (0, 0, 255), 2)

            # [핵심] 현재 비디오 컨테이너의 실시간 크기 가져오기
            # 위젯이 아직 렌더링 전이면 1을 반환하므로 최소값 1 설정
            win_w = max(self.video_container.winfo_width() - 20, 1) 
            win_h = max(self.video_container.winfo_height() - 40, 1)

            # OpenCV BGR -> RGB 변환
            rgb_img = cv2.cvtColor(display_img, cv2.COLOR_BGR2RGB)
            pil_img = Image.fromarray(rgb_img)
            
            # [비율 유지 계산] 컨테이너에 꽉 차는 최적의 크기 계산
            img_aspect = w / h
            win_aspect = win_w / win_h

            if win_aspect > img_aspect:
                # 컨테이너가 더 넓은 경우 -> 높이에 맞춤
                new_h = win_h
                new_w = int(new_h * img_aspect)
            else:
                # 컨테이너가 더 좁은 경우 -> 너비에 맞춤
                new_w = win_w
                new_h = int(new_w / img_aspect)

            # 계산된 크기로 리사이즈
            pil_img = pil_img.resize((new_w, new_h), Image.Resampling.LANCZOS)
            
            tk_img = ImageTk.PhotoImage(image=pil_img)
            self.video_label.configure(image=tk_img)
            self.video_label.image = tk_img # 가비지 컬렉션 방지
            
            # 태그 텍스트 정보 업데이트
            if res.get('tag_pose_info'):
                tag = res['tag_pose_info']
                from vision_module import rmat_to_euler_zyx
                yaw, _, _ = rmat_to_euler_zyx(tag['R_mat'])
                self.tag_status.set(f"Tag 탐지됨 (ID: {tag['id']})")
                self.tag_pos_x.set(f"X: {tag['t_vec'][0,0]*1000:.1f}mm")
                self.tag_pos_y.set(f"Y: {tag['t_vec'][1,0]*1000:.1f}mm")
                self.tag_rot_yaw.set(f"Yaw: {yaw:.1f}°")
            else:
                self.tag_status.set("태그 없음")

        if not self._going_down:
            self.root.after(40, self._ui_tick) # 25 FPS 수준으로 업데이트

    def _on_close(self):
        """종료 처리"""
        self._going_down = True
        try:
            self.detector.cam.stop()
        except:
            pass
        cv2.destroyAllWindows()
        self._exec.shutdown()
        self.root.destroy()
        if rclpy.ok():
            rclpy.shutdown()

def main():
    rclpy.init()
    
    # 카메라 초기화
    cam = AlviumCamera()
    cam.start()
    
    # 모듈 초기화
    robot = URInterface()
    detector = TagDetectorWrapper(cam)
    controller = AutomationController(robot, detector)
    
    # GUI 앱 실행
    app = CalibrationApp(robot, detector, controller)
    try:
        app.root.mainloop()
    except KeyboardInterrupt:
        pass
    finally:
        app._on_close()

if __name__ == "__main__":
    main()
