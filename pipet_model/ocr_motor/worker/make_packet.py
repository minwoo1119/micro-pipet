"""피펫 펌웨어에서 사용하는 13바이트 시리얼 패킷 조합 헬퍼."""

from typing import ByteString


class MakePacket:
    # ===== Constants =====
    HEADER1 = 0xEA
    HEADER2 = 0xEB
    ENDOFBYTE = 0xED
    LEN = 0x07

    # ==============================
    # Command Codes
    # ==============================
    # MightyZap
    MIGHTYZAP_SetPosition     = 0x01
    MIGHTYZAP_SetSpeed        = 0x02
    MIGHTYZAP_SetCurrent      = 0x03
    MIGHTYZAP_SetForceOnOff   = 0x04
    MIGHTYZAP_GetMovingState  = 0x05
    MIGHTYZAP_GetFeedbackData = 0x07

    # MyActuator
    MyActuator_setAbsoluteAngle = 0xA4
    MyActuator_getAbsoluteAngle = 0x92

    # Geared DC
    GearedDC_changePipetteVolume = 0xA1

    # ==============================
    # Checksum (CMD~DATA6 only)
    # ==============================
    @staticmethod
    def _checksum(packet: ByteString) -> int:
        """CMD부터 DATA6까지를 기준으로 펌웨어 체크섬을 계산한다."""
        checksum_raw = sum(packet[4:11])
        return (0xFF - (checksum_raw % 256)) & 0xFF

    # ==============================
    # Base Packet (13 bytes)
    # ==============================
    @staticmethod
    def _base_packet(id_: int, cmd: int, data: list[int]) -> bytes:
        """헤더, payload padding, 체크섬, ETX를 포함한 전체 프로토콜 프레임을 만든다."""
        packet = bytearray(13)
        packet[0] = MakePacket.HEADER1
        packet[1] = MakePacket.HEADER2
        packet[2] = id_ & 0xFF
        packet[3] = MakePacket.LEN
        packet[4] = cmd & 0xFF

        for i in range(6):
            packet[5 + i] = data[i] if i < len(data) else 0x00

        packet[11] = MakePacket._checksum(packet)
        packet[12] = MakePacket.ENDOFBYTE
        return bytes(packet)

    # ==============================
    # MightyZap
    # ==============================
    @staticmethod
    def set_position(id_: int, position: int) -> bytes:
        """MightyZap 절대 위치 명령 패킷을 만든다."""
        return MakePacket._base_packet(
            id_,
            MakePacket.MIGHTYZAP_SetPosition,
            [position & 0xFF, (position >> 8) & 0xFF],
        )

    @staticmethod
    def set_speed(id_: int, speed: int) -> bytes:
        """MightyZap 속도 명령 패킷을 만든다."""
        return MakePacket._base_packet(
            id_,
            MakePacket.MIGHTYZAP_SetSpeed,
            [speed & 0xFF, (speed >> 8) & 0xFF],
        )

    @staticmethod
    def set_current(id_: int, current: int) -> bytes:
        """MightyZap 전류 제한 명령 패킷을 만든다."""
        return MakePacket._base_packet(
            id_,
            MakePacket.MIGHTYZAP_SetCurrent,
            [current & 0xFF, (current >> 8) & 0xFF],
        )

    @staticmethod
    def set_force_onoff(id_: int, onoff: int) -> bytes:
        """액추에이터 force 모드를 켜거나 끄는 패킷을 만든다."""
        return MakePacket._base_packet(
            id_,
            MakePacket.MIGHTYZAP_SetForceOnOff,
            [1 if onoff else 0],
        )

    @staticmethod
    def get_moving(id_: int) -> bytes:
        """액추에이터 이동 상태 플래그를 조회하는 패킷을 만든다."""
        return MakePacket._base_packet(
            id_,
            MakePacket.MIGHTYZAP_GetMovingState,
            [],
        )

    @staticmethod
    def get_feedback(id_: int) -> bytes:
        """액추에이터 피드백 프레임을 요청하는 패킷을 만든다."""
        return MakePacket._base_packet(
            id_,
            MakePacket.MIGHTYZAP_GetFeedbackData,
            [],
        )

    # ==============================
    # ✔ C# Poll Timer와 동일
    # ==============================
    @staticmethod
    def request_check_operate_status() -> bytes:
        """기존 C# 타이머 루프와 동일한 이동 상태 poll 브로드캐스트 패킷을 만든다."""
        return MakePacket.get_moving(0xFF)

    # ==============================
    # MyActuator
    # ==============================
    @staticmethod
    def myactuator_set_absolute_angle(id_: int, speed: int, angle: int) -> bytes:
        """커스텀 회전 액추에이터용 절대 각도 명령 패킷을 만든다."""
        data = [
            speed & 0xFF,
            (speed >> 8) & 0xFF,
            angle & 0xFF,
            (angle >> 8) & 0xFF,
            (angle >> 16) & 0xFF,
            (angle >> 24) & 0xFF,
        ]
        return MakePacket._base_packet(
            id_,
            MakePacket.MyActuator_setAbsoluteAngle,
            data,
        )

    @staticmethod
    def myactuator_get_absolute_angle(id_: int) -> bytes:
        """커스텀 액추에이터의 현재 절대 각도를 요청하는 패킷을 만든다."""
        return MakePacket._base_packet(
            id_,
            MakePacket.MyActuator_getAbsoluteAngle,
            [],
        )

    # ==============================
    # Geared DC Motor
    # ==============================
    @staticmethod
    def pipette_change_volume(id_: int, direction: int, duty: int) -> bytes:
        """피펫 용량 변경용 기어드 DC 모터 명령 패킷을 만든다."""
        direction = 1 if direction > 0 else 0
        duty = max(0, min(100, duty))

        return MakePacket._base_packet(
            id_,
            MakePacket.GearedDC_changePipetteVolume,
            [direction, duty],
        )
