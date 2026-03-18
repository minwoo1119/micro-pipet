#!/bin/bash

# 1. 경로 설정
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
cd "$SCRIPT_DIR"
VENV_PATH="$SCRIPT_DIR/../../.venv/bin/activate"
ROS_SETUP="source /opt/ros/jazzy/setup.bash"

echo "--- [REVOLVER ROBOT] 시스템 통합 런처 (xterm 기반) ---"

# 2. 터미널 실행 함수: xterm을 사용하여 환경 변수 충돌 원천 차단
open_robot_terminal() {
    local TITLE=$1
    local CMD=$2
    
    echo ">> $TITLE 기동 중..."
    # env -i: 모든 환경 변수를 삭제한 뒤 필요한 것(DISPLAY, PATH 등)만 넘겨서 충돌 방지
    env -i DISPLAY=$DISPLAY TERM=$TERM PATH=$PATH HOME=$HOME \
    xterm -T "$TITLE" -geometry 100x24 -hold -e "bash -c '$ROS_SETUP; $CMD'" &
}

# 3. 로봇 드라이버 및 MoveIt 실행
open_robot_terminal "1. UR Driver" "ros2 launch ur_robot_driver ur_control.launch.py ur_type:=ur5e robot_ip:=192.168.64.106 launch_rviz:=false"
sleep 5

open_robot_terminal "2. MoveIt" "ros2 launch ur_moveit_config ur_moveit.launch.py ur_type:=ur5e launch_rviz:=true publish_robot_description_semantic:=true"
sleep 7

# 4. 메인 앱 실행 (현재 터미널)
echo "--- 3. 메인 앱 실행 ---"
$ROS_SETUP
if [ -f "$VENV_PATH" ]; then
    source "$VENV_PATH"
    echo "[OK] 가상환경 활성화 완료"
fi

python3 main.py