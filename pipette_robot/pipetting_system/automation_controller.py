"""
Automation / Sequencing Layer

`AutomationController`는 GUI(`main.py`)에서 호출되는 "동작 시퀀스"를 담당합니다.

핵심 책임:
- 태그 센터링(픽셀 오차 → 카메라 좌표 오차 근사 → 상대 이동 반복)
- 카메라/툴 장착각 보정이 포함된 상대 이동(`execute_relative_move`)
- Well Plate의 (row, col) 목표를 태그 포즈 기준으로 변환하여 이동(`move_to_well`)
- 피펫 시퀀스: 오프셋 이동 → Z 분할 하강/상승 → 복귀(`run_pipette_sequence`)

주의:
- 실제 로봇 이동은 `URInterface`(robot_interface.py)로 위임합니다.
- 대부분의 동작은 별도 스레드에서 실행되어 UI를 블로킹하지 않도록 되어 있습니다.
"""

import time
import math
import threading
import numpy as np
import rclpy
from scipy.spatial.transform import Rotation as R
from config import RobotConfig, WellPlateConfig, ControlParams, PipetteConfig

class AutomationController:
    def __init__(self, robot, detector, status_callback=None):
        self.robot = robot
        self.detector = detector
        self.status_callback = status_callback
        self._is_sequence_running = False

    def execute_relative_move(self, dx_cam, dy_cam, duration_sec=None):
        """60도 보정 알고리즘을 적용한 상대 이동

        입력:
        - dx_cam, dy_cam: 카메라 좌표계에서의 오차(m)로 취급되는 상대 이동량
        - duration_sec: 이동 시간(초). None이면 기본값 사용

        처리:
        - 툴 장착각(`RobotConfig.MOUNT_ANGLE_DEG`) 및 축 스왑 규칙을 적용해 툴 좌표 이동량으로 변환
        - 현재 TF의 툴 회전을 이용해 base_link 좌표계 상대 이동량으로 변환
        - 최종 목표 pos를 만들고 IK+이동을 요청
        """
        try:
            # [수정] TF가 사용 가능할 때까지 최대 2초간 대기
            now = rclpy.time.Time()
            if not self.robot.tf_buffer.can_transform(
                RobotConfig.BASE_LINK, 
                RobotConfig.EE_LINK, 
                now, 
                rclpy.duration.Duration(seconds=2.0)
            ):
                if self.status_callback:
                    self.status_callback("Waiting for TF (base_link)...")
                return

            # 이제 안전하게 좌표를 가져옵니다.
            tf = self.robot.tf_buffer.lookup_transform(
                RobotConfig.BASE_LINK, 
                RobotConfig.EE_LINK, 
                now
            )    
            R_base_tool = R.from_quat([tf.transform.rotation.x, tf.transform.rotation.y, tf.transform.rotation.z, tf.transform.rotation.w]).as_matrix()
            angle_rad = math.radians(RobotConfig.MOUNT_ANGLE_DEG)
            cos_a, sin_a = math.cos(angle_rad), math.sin(angle_rad)
            dx_p, dy_p = dy_cam, dx_cam  # 축 스왑
            dx_tool = dx_p * cos_a - dy_p * sin_a
            dy_tool = dx_p * sin_a + dy_p * cos_a

            P_base_relative = R_base_tool @ np.array([dx_tool, dy_tool, 0]).reshape(3, 1)
            target_pos = [tf.transform.translation.x + P_base_relative[0,0], tf.transform.translation.y + P_base_relative[1,0], tf.transform.translation.z]
            self.robot.solve_ik_and_move(target_pos, tf.transform.rotation, duration_sec)
        except Exception as e:
            print(f"Move Error: {e}")

    def wait_for_tag_pose(self, timeout_sec=2.0, min_timestamp=None):
        """태그 포즈가 감지될 때까지 대기"""
        t0 = time.time()
        while time.time() - t0 < timeout_sec:
            res = self.detector.latest_result
            if res and res.get('tag_pose_info'):
                ts = res.get('timestamp')
                if min_timestamp is None or ts is None or ts > min_timestamp:
                    return res
            time.sleep(0.05)
        return None

    def center_tag_and_get_latest_pose(self, timeout_sec=3.0, move_time=None):
        """태그를 화면 중앙에 정렬하고 최신 포즈 반환

        동작:
        - 최신 프레임에서 태그 중심 픽셀 오차를 읽고,
        - 카메라 내참(K)을 이용해 오차를 거리(m)로 근사한 뒤,
        - 상대 이동을 반복하여 임계 픽셀 오차 이하로 수렴시키는 방식입니다.
        """
        if move_time is None:
            move_time = RobotConfig.DEFAULT_TIME_SEC
        last_ts = None
        for it in range(1, ControlParams.CENTER_MAX_ITERS + 1):
            res = self.wait_for_tag_pose(timeout_sec=ControlParams.CENTER_PER_ITER_TIMEOUT, min_timestamp=last_ts)
            if not res:
                if self.status_callback:
                    self.status_callback(f"Center Fail: No Tag (Iter {it})")
                return None
            last_ts = res.get('timestamp')
            tag, K = res['tag_pose_info'], res['K']
            u0, v0 = res['frame'].shape[1] * 0.5, res['frame'].shape[0] * 0.5
            u_err, v_err = tag['center_px'][0] - u0, tag['center_px'][1] - v0

            if abs(u_err) <= ControlParams.CENTER_THRESH_PX and abs(v_err) <= ControlParams.CENTER_THRESH_PX:
                if self.status_callback:
                    self.status_callback("Center OK")
                return tag

            x_cam, y_cam = (u_err / K[0,0]) * tag['t_vec'][2,0], (v_err / K[1,1]) * tag['t_vec'][2,0]
            if self.status_callback:
                self.status_callback(f"Centering {it}/{ControlParams.CENTER_MAX_ITERS}")
            self.execute_relative_move(-x_cam, y_cam, duration_sec=move_time)
            time.sleep(move_time + 0.4)
        return None

    def move_to_well(self, target_row, target_col):
        """특정 Well로 이동

        - 태그 포즈를 기준 프레임처럼 사용하여 로컬 좌표의 웰 위치를 카메라 좌표로 변환합니다.
        - 이후 `execute_relative_move()`로 이동을 수행합니다.
        """
        if self._is_sequence_running:
            return
        def seq_thread():
            self._is_sequence_running = True
            try:
                row_labels = ['A', 'B', 'C', 'D']
                tag_pose = self.center_tag_and_get_latest_pose()
                if not tag_pose:
                    return
                time.sleep(0.2)

                local_x = (target_col * WellPlateConfig.WELL_SPACING_MM) / 1000.0
                local_y = (WellPlateConfig.OFFSET_TAG_TO_REF_Y_MM + (target_row * WellPlateConfig.WELL_SPACING_MM)) / 1000.0
                P_local = np.array([local_x, local_y, 0.0]).reshape(3, 1)

                P_well_in_cam = tag_pose['R_mat'] @ P_local + tag_pose['t_vec']
                P_well_in_cam = P_well_in_cam.flatten()

                move_x, move_y = -P_well_in_cam[0], P_well_in_cam[1]
                dist = math.hypot(move_x, move_y)
                move_time = max(ControlParams.WELL_MOVE_MIN_T, min(ControlParams.WELL_MOVE_MAX_T, dist / ControlParams.WELL_MOVE_V_MPS))

                if self.status_callback:
                    self.status_callback(f"Moving to Well {row_labels[target_row]}{target_col+1}...")
                self.execute_relative_move(move_x, move_y, duration_sec=move_time)
                time.sleep(move_time + 0.3)
                if self.status_callback:
                    self.status_callback(f"Arrived Well {row_labels[target_row]}{target_col+1}")
            except Exception as e:
                print(f"Well Move Error: {e}")
            finally:
                self._is_sequence_running = False
        threading.Thread(target=seq_thread, daemon=True).start()

    def move_z_linear(self, start_pos_xyz, rot, dz_total, step_m, status_callback=None):
        """Z축 전체 경로를 미리 계산하여 부드럽게 이동

        구현 포인트:
        - 분할 스텝마다 IK를 미리 풀어서 `q_sequence`를 만든 후,
        - 하나의 multi-point trajectory로 전송합니다.

        안전 포인트:
        - 특정 스텝에서 IK가 실패하면 즉시 중단(False 반환)합니다.
        """
        dz_total = float(dz_total)
        step_m = abs(float(step_m))
        if abs(dz_total) < 1e-9:
            return True

        num_steps = int(abs(dz_total) // step_m)
        direction = 1.0 if dz_total > 0 else -1.0

        q_sequence = []
        curr_p = list(start_pos_xyz)
        last_seed = self.robot.last_q

        # 모든 점의 IK를 미리 계산하여 리스트에 저장
        for i in range(1, num_steps + 1):
            curr_p[2] = start_pos_xyz[2] + (direction * step_m * i)
            q_sol = self.robot.get_ik_sync(curr_p, rot, last_seed)
            if q_sol:
                q_sequence.append(q_sol)
                last_seed = q_sol
            else:
                if status_callback:
                    status_callback(f"IK Failed at step {i}")
                return False

        # 자투리 거리 처리
        if abs(dz_total) % step_m > 1e-6:
            curr_p[2] = start_pos_xyz[2] + dz_total
            q_sol = self.robot.get_ik_sync(curr_p, rot, last_seed)
            if q_sol:
                q_sequence.append(q_sol)

        # 하나의 Trajectory로 전송
        if q_sequence:
            self.robot.send_multi_point_traj(q_sequence, ControlParams.STEP_DURATION_SEC)
            if status_callback:
                status_callback(f"Sending Trajectory ({len(q_sequence)} points)")
            # 전체 이동 시간만큼 대기
            time.sleep(len(q_sequence) * ControlParams.STEP_DURATION_SEC + 0.5)
            return True
        return False

    def run_pipette_sequence(self, pip_offset_x, pip_offset_y, pip_down_z):
        """피펫 시퀀스 실행: 오프셋 이동 -> 하강 -> 상승 -> 복귀"""
        if self._is_sequence_running:
            return
        def seq():
            self._is_sequence_running = True
            try:
                tf = self.robot.tf_buffer.lookup_transform(RobotConfig.BASE_LINK, RobotConfig.EE_LINK, rclpy.time.Time())
                start_pos = [tf.transform.translation.x, tf.transform.translation.y, tf.transform.translation.z]
                rot = tf.transform.rotation

                # 1. 오프셋 이동 (X, Y 보정)
                if self.status_callback:
                    self.status_callback("Moving to Pipette Offset...")
                target = [start_pos[0] + float(pip_offset_x), start_pos[1] + float(pip_offset_y), start_pos[2]]
                self.robot.solve_ik_and_move(target, rot)
                time.sleep(RobotConfig.DEFAULT_TIME_SEC + 0.2)

                # 2. 연속 하강 (Continuous Down)
                if self.status_callback:
                    self.status_callback("Continuous Downstream Move...")
                self.move_z_linear(target, rot, -float(pip_down_z), ControlParams.LINEAR_STEP_M, self.status_callback)
                time.sleep(1.0)

                # 3. 연속 상승 (Continuous Upstream Move)
                if self.status_callback:
                    self.status_callback("Continuous Upstream Move...")
                bottom_pos = [target[0], target[1], target[2] - float(pip_down_z)]
                self.move_z_linear(bottom_pos, rot, float(pip_down_z), ControlParams.LINEAR_STEP_M, self.status_callback)

                # 4. 복귀
                if self.status_callback:
                    self.status_callback("Returning...")
                self.robot.solve_ik_and_move(start_pos, rot)
            except Exception as e:
                if self.status_callback:
                    self.status_callback(f"Pipette Error: {e}")
            finally:
                self._is_sequence_running = False
        threading.Thread(target=seq, daemon=True).start()