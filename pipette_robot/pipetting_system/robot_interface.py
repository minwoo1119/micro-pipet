"""
ROS2 / MoveIt2 Robot Interface (UR)

이 모듈은 로봇 제어의 "통신/연동" 계층입니다.

제공 기능:
- `/joint_states` 구독으로 최신 관절각을 유지
- TF 조회(`base_link` ↔ `tool0`)
- MoveIt IK 서비스(`/compute_ik`) 호출
  - `get_ik_sync()`: 다점 궤적 계산용(블로킹) - 별도 스레드에서 사용 권장
  - `solve_ik_and_move()`: 일반 이동용(비동기)
- FollowJointTrajectory Action으로 궤적 전송(단점/다점)

주의:
- `URInterface`는 rclpy `Node`이므로 executor spin이 반드시 필요합니다(`main.py`에서 별도 스레드 spin).
"""

from dataclasses import dataclass
from typing import List
import rclpy
from rclpy.node import Node
from rclpy.action import ActionClient
from rclpy.duration import Duration
from sensor_msgs.msg import JointState
from control_msgs.action import FollowJointTrajectory
from trajectory_msgs.msg import JointTrajectory, JointTrajectoryPoint
from moveit_msgs.srv import GetPositionIK
from geometry_msgs.msg import PoseStamped
import tf2_ros
from config import RobotConfig

@dataclass
class JointOrder:
    names: List[str]
    index: dict

class URInterface(Node):
    def __init__(self):
        super().__init__("ur_robot_interface")
        self.tf_buffer = tf2_ros.Buffer(cache_time=Duration(seconds=2.0))
        self.tf_listener = tf2_ros.TransformListener(self.tf_buffer, self)
        self.ik_cli = self.create_client(GetPositionIK, "/compute_ik")
        self.ac = ActionClient(self, FollowJointTrajectory, RobotConfig.ACTION_NAME)
        
        self._last_q = []
        self._joint_order = None
        self._ik_future_to_time = {}
        self.create_subscription(JointState, "/joint_states", self._on_joint_states, 50)

    def _on_joint_states(self, msg):
        """관절 상태 구독 콜백"""
        if self._joint_order is None:
            names = [n for n in msg.name if "joint" in n]
            self._joint_order = JointOrder(names=names, index={n: msg.name.index(n) for n in names})
        self._last_q = [msg.position[self._joint_order.index[n]] for n in self._joint_order.names]

    @property
    def joint_order(self):
        return self._joint_order

    @property
    def last_q(self):
        return self._last_q

    def get_ik_sync(self, pos, rot, seed_q):
        """IK 서비스를 동기적으로 호출하여 해를 즉시 반환 (궤적 생성용)

        인수인계/운영 전제(중요):
        - 이 함수는 `/joint_states`를 최소 1회 이상 수신하여 `self._joint_order`가 준비된 이후에만
          정상 동작할 수 있습니다. (그 전에는 seed/조인트 순서를 만들 수 없음)
        - 블로킹 호출이므로 executor spin을 담당하는 스레드(또는 Tkinter 메인 스레드)에서
          과도하게 호출하면 앱이 멈춘 것처럼 보일 수 있습니다.
        """
        if not self.ik_cli.service_is_ready():
            return None
        req = GetPositionIK.Request()
        req.ik_request.group_name = RobotConfig.GROUP_NAME
        req.ik_request.robot_state.joint_state.name = self._joint_order.names
        req.ik_request.robot_state.joint_state.position = seed_q
        ps = PoseStamped()
        ps.header.frame_id = RobotConfig.BASE_LINK
        ps.pose.position.x, ps.pose.position.y, ps.pose.position.z = pos
        ps.pose.orientation = rot
        req.ik_request.pose_stamped = ps

        # 동기 대기 (데드락 주의: executor와 같은 스레드에서 호출하지 않도록 주의)
        result = self.ik_cli.call(req)
        if result.error_code.val == result.error_code.SUCCESS:
            js = result.solution.joint_state
            return [js.position[js.name.index(n)] for n in self._joint_order.names]
        return None

    def solve_ik_and_move(self, target_pos, target_rot, duration_sec=None, callback=None):
        """IK를 계산하고 궤적을 전송 (비동기)

        실패가 "무반응"처럼 보이는 대표 케이스:
        - `/joint_states` 미수신으로 `self._joint_order` / `self._last_q`가 준비되지 않음(초기 구동 직후)
        - `/compute_ik` 서비스가 준비되지 않음(MoveIt 미기동)
        - FollowJointTrajectory Action server가 준비되지 않음(UR driver/컨트롤러 미기동)
        """
        if self._joint_order is None or not self._last_q:
            return
        req = GetPositionIK.Request()
        req.ik_request.group_name = RobotConfig.GROUP_NAME
        req.ik_request.robot_state.joint_state.name = self._joint_order.names
        req.ik_request.robot_state.joint_state.position = self._last_q
        ps = PoseStamped()
        ps.header.frame_id = RobotConfig.BASE_LINK
        ps.pose.position.x, ps.pose.position.y, ps.pose.position.z = target_pos[0], target_pos[1], target_pos[2]
        ps.pose.orientation = target_rot
        req.ik_request.pose_stamped = ps
        req.ik_request.ik_link_name = RobotConfig.EE_LINK

        future = self.ik_cli.call_async(req)
        self._ik_future_to_time[future] = float(duration_sec or RobotConfig.DEFAULT_TIME_SEC)
        if callback:
            future.add_done_callback(callback)
        else:
            future.add_done_callback(self._on_ik_done)

    def _on_ik_done(self, future):
        """IK 완료 콜백"""
        dur = self._ik_future_to_time.pop(future, RobotConfig.DEFAULT_TIME_SEC)
        try:
            resp = future.result()
            if resp.error_code.val == resp.error_code.SUCCESS:
                js = resp.solution.joint_state
                q = [js.position[js.name.index(n)] for n in self._joint_order.names]
                self.send_traj(q, dur)
        except Exception as e:
            self.get_logger().error(f"IK Error: {e}")

    def send_traj(self, q, duration_sec):
        """단일 궤적 포인트 전송"""
        if not self.ac.server_is_ready() or self._joint_order is None:
            return
        jt = JointTrajectory(joint_names=self._joint_order.names)
        pt = JointTrajectoryPoint(positions=q, time_from_start=Duration(seconds=float(duration_sec)).to_msg())
        jt.points = [pt]
        self.ac.send_goal_async(FollowJointTrajectory.Goal(trajectory=jt))

    def send_multi_point_traj(self, q_list, step_duration_sec):
        """여러 개의 관절값을 하나의 연결된 궤적으로 전송"""
        if not self.ac.server_is_ready() or self._joint_order is None:
            return
        jt = JointTrajectory(joint_names=self._joint_order.names)
        for i, q in enumerate(q_list):
            pt = JointTrajectoryPoint()
            pt.positions = q
            pt.time_from_start = Duration(seconds=(i+1)*step_duration_sec).to_msg()
            jt.points.append(pt)
        self.ac.send_goal_async(FollowJointTrajectory.Goal(trajectory=jt))