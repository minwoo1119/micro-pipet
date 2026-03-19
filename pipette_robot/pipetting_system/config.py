import math

class RobotConfig:
    """로봇 제어 관련 물리 상수 및 설정"""
    GROUP_NAME = "ur_manipulator"
    BASE_LINK = "base_link"
    EE_LINK = "tool0"
    
    # ROS2 Action 및 Service 인터페이스 이름
    ACTION_NAME = "/scaled_joint_trajectory_controller/follow_joint_trajectory"
    
    # [중요] 카메라가 Tool 끝단에 -60도 회전되어 부착된 상태를 보정하기 위한 값
    MOUNT_ANGLE_DEG = -60.0
    
    # 안전을 위한 초기 포즈 (Joint space)
    HOME_JOINTS = [math.pi/2, -math.pi/2, 0.0, -math.pi/2, -math.pi/2, math.pi + math.radians(60)]
    DEFAULT_TIME_SEC = 2.0
    DEFAULT_JOINT_STEP_RAD = 0.5
    DEFAULT_CART_STEP_M = 0.10
    
    # 저장 디렉토리
    SAVE_DIR = "calibration_data_apriltag"

class WellPlateConfig:
    """Well Plate 기하학적 정보 (단위: m, mm)"""
    TAG_SIZE_M = 0.02
    TAG_FAMILY = "tag36h11"
    
    # 광학 파라미터 (Alvium 카메라 기준)
    LENS_FOCAL_LENGTH_MM = 8.0
    SENSOR_PIXEL_SIZE_MM = 0.00274
    
    # [중요] AprilTag 중심에서 첫 번째 Well(A1) 중심까지의 Y축 오프셋
    OFFSET_TAG_TO_REF_Y_MM = 26.34
    # Well 사이의 간격 (Row/Col 동일)
    WELL_SPACING_MM = 19.3
    WELL_ROWS = 4
    WELL_COLS = 6
    
    # AprilTag 회전 보정 (부착 오차 보정용)
    TAG_YAW_OFFSET_DEG = 0.7
    TAG_PITCH_OFFSET_DEG = 0.0
    TAG_ROLL_OFFSET_DEG = 0.0

class ControlParams:
    """정밀 제어 및 시퀀스 파라미터"""
    LINEAR_STEP_M = 0.01      # Z축 하강 시 1cm씩 분할
    STEP_DURATION_SEC = 0.4   # 분할 이동 시 스텝당 소요 시간
    CENTER_THRESH_PX = 3      # 정렬 허용 오차 (Pixel 단위)
    CENTER_MAX_ITERS = 8      # 최대 보정 횟수
    CENTER_PER_ITER_TIMEOUT = 1.2  # 각 반복당 타임아웃 (초)
    WELL_MOVE_V_MPS = 0.02    # Well 간 이동 목표 속도 (m/s)
    WELL_MOVE_MIN_T = 1.0     # Well 이동 최소 시간 (초)
    WELL_MOVE_MAX_T = 6.0     # Well 이동 최대 시간 (초)

class PipetteConfig:
    """피펫 제어 관련 오프셋 상수"""
    PIPETTE_OFFSET_X = -0.08   # (m)
    PIPETTE_OFFSET_Y = -0.016  # (m)
    PIPETTE_DOWN_Z = 0.2      # (m)