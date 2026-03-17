"""회전형 용량 조절에 쓰는 기어드 DC 모터를 감싼 얇은 래퍼."""

from worker.make_packet import MakePacket


class VolumeDCActuator:
    """
    Geared DC Motor for Pipette Volume
    - C# MouseDown / MouseUp 구조 1:1 대응
    """

    def __init__(self, serial, actuator_id: int):
        self.serial = serial
        self.actuator_id = actuator_id

    # ======================================================
    # Start rotating (MouseDown)
    # ======================================================
    def run(self, direction: int, duty: int):
        """정규화된 방향값과 PWM duty 값으로 회전을 시작한다."""
        direction = 1 if int(direction) > 0 else 0
        duty = max(0, min(100, int(duty)))

        self.serial.send_pipette_change_volume(
            actuator_id=self.actuator_id,
            direction=direction,
            duty=duty,
        )

    # ======================================================
    # Stop rotating (MouseUp)
    # ======================================================
    def stop(self):
        """출력을 0으로 보내 DC 모터를 정지시킨다."""
        self.serial.send_pipette_stop(self.actuator_id)
