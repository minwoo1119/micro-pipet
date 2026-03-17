"""기존 C# 통신 구조를 Python으로 옮긴 시리얼 전송 계층."""

import time
import threading
import queue
from typing import Optional, Callable

import serial
from worker.make_packet import MakePacket


class SerialController:
    """
    C# MightyZap 통신 구조 1:1 대응
    - Poll Timer 기반
    - RX Status Frame 기반 상태 관리
    """

    TX_TICK_SEC = 0.05
    POLL_INTERVAL_SEC = 0.1
    MAX_QUEUE = 3

    STX1 = 0xEA
    STX2 = 0xEB
    ETX  = 0xED

    def __init__(
        self,
        port: str = "/dev/ttyUSB0",
        baudrate: int = 115200,
        timeout: float = 0.1,
    ):
        self.port = port
        self.baudrate = baudrate
        self.timeout = timeout

        self.ser: Optional[serial.Serial] = None
        self.running = False

        self.tx_queue: "queue.Queue[bytes]" = queue.Queue()

        # 🔥 Poll은 항상 켜져 있어야 한다
        self.polling_enabled = True
        self._last_poll_time = 0.0
        self._rx_received = True

        # Status storage
        self.states = {}
        self._state_lock = threading.Lock()

        self.rx_debug = True
        self.tx_debug = True

        self.make_poll_status: Optional[Callable[[], bytes]] = getattr(
            MakePacket, "request_check_operate_status", None
        )

        # Thread refs (optional but clean)
        self._tx_thread = None
        self._rx_thread = None
        self._poll_thread = None

    # =========================
    # Connection
    # =========================
    def connect(self) -> bool:
        """포트를 연 뒤 TX, RX, poll 스레드를 함께 띄워 기존 C# 동작 순서를 유지한다."""
        self.ser = serial.Serial(
            port=self.port,
            baudrate=self.baudrate,
            timeout=self.timeout,
            bytesize=serial.EIGHTBITS,
            parity=serial.PARITY_NONE,
            stopbits=serial.STOPBITS_ONE,
        )

        time.sleep(0.5)
        self.running = True

        self._tx_thread = threading.Thread(
            target=self._tx_worker, daemon=True
        )
        self._rx_thread = threading.Thread(
            target=self._rx_worker, daemon=True
        )
        self._poll_thread = threading.Thread(
            target=self._poll_worker, daemon=True
        )

        self._tx_thread.start()
        self._rx_thread.start()
        self._poll_thread.start()

        return self.ser.is_open

    # =========================
    # Graceful Close (🔥 필수)
    # =========================
    def close(self):
        """종료 시 호출하는 정리 메서드로, 스레드를 멈춘 뒤 포트를 안전하게 닫는다."""
        self.running = False

        # thread들이 loop 탈출할 시간
        time.sleep(0.1)

        try:
            if self.ser and self.ser.is_open:
                self.ser.close()
                if self.tx_debug:
                    print("[SERIAL] closed")
        except Exception as e:
            print("[SERIAL] close error:", e)

    # =========================
    # TX
    # =========================
    def enqueue(self, packet: bytes):
        """전송은 모두 큐를 거치므로, 호출부는 이 메서드만 써도 된다."""
        if not self.ser or not self.ser.is_open:
            return

        if self.tx_queue.qsize() >= self.MAX_QUEUE:
            return

        self.tx_queue.put(packet)
        if self.tx_debug:
            print(f"[ENQUEUE] {packet.hex(' ')}")

    def _tx_worker(self):
        """TX 큐에 들어온 패킷을 순서대로 실제 시리얼 포트로 내보낸다."""
        while self.running:
            try:
                if not self.tx_queue.empty():
                    pkt = self.tx_queue.get_nowait()
                    self.ser.write(pkt)
                    self.ser.flush()
                    if self.tx_debug:
                        print(f"[TX] {pkt.hex(' ')}")
            except Exception as e:
                if self.running:
                    print("[TX ERROR]", e)

            time.sleep(self.TX_TICK_SEC)

    # =========================
    # Poll (C# Timer 복제)
    # =========================
    def _poll_worker(self):
        """통신이 한가한 시점에만 상태 poll을 넣어 C# 타이머 방식과 동일하게 유지한다."""
        while self.running:
            try:
                now = time.time()

                if not self._rx_received:
                    time.sleep(0.01)
                    continue

                if (now - self._last_poll_time) < self.POLL_INTERVAL_SEC:
                    time.sleep(0.01)
                    continue

                if not self.tx_queue.empty():
                    time.sleep(0.01)
                    continue

                if self.make_poll_status and self.polling_enabled:
                    self.enqueue(self.make_poll_status())
                    self._rx_received = False
                    self._last_poll_time = now

            except Exception as e:
                if self.running:
                    print("[POLL ERROR]", e)

            time.sleep(0.01)

    # =========================
    # RX
    # =========================
    def _rx_worker(self):
        """수신 바이트를 프레임 단위로 잘라 `_handle_frame`에 넘기는 역할만 담당한다."""
        buffer = bytearray()

        while self.running:
            try:
                if self.ser and self.ser.in_waiting:
                    buffer += self.ser.read(self.ser.in_waiting)

                    while len(buffer) >= 13:
                        if buffer[0] != self.STX1 or buffer[1] != self.STX2:
                            buffer.pop(0)
                            continue

                        if self.ETX not in buffer:
                            break

                        end = buffer.index(self.ETX)
                        frame = bytes(buffer[:end + 1])
                        buffer = buffer[end + 1:]

                        if self.rx_debug:
                            print(f"[RX] {frame.hex(' ')}")

                        self._handle_frame(frame)

            except Exception as e:
                if self.running:
                    print("[RX ERROR]", e)

            time.sleep(0.002)

    def _handle_frame(self, frame: bytes):
        """정상 상태 프레임만 받아 최신 moving 상태를 내부 캐시에 반영한다."""
        if len(frame) != 13:
            return

        cmd = frame[4]
        actuator_id = frame[2]

        # Status Frame only
        if cmd != 0x11:
            return

        moving = frame[8]

        with self._state_lock:
            self.states[actuator_id] = {
                "moving": moving,
                "timestamp": time.time(),
            }

        self._rx_received = True

        if self.rx_debug:
            print(f"[STATUS] id={hex(actuator_id)} moving={moving}")

    # =========================
    # Blocking helper
    # =========================
    def move_and_wait(self, actuator_id: int, position: int, timeout: float = 5.0):
        """예전 호출부 호환용 메서드로, 현재는 간단한 대기만 두고 있다."""
        self.enqueue(MakePacket.set_position(actuator_id, position))

        # C# 동일 동작
        time.sleep(0.6)
        return True

    # =========================
    # High-level APIs (🔥 호환 필수)
    # =========================
    def send_mightyzap_set_position(self, actuator_id: int, position: int):
        """호출부가 패킷 포맷을 몰라도 위치 명령을 보낼 수 있게 감싼 메서드다."""
        self.enqueue(MakePacket.set_position(actuator_id, position))

    def send_mightyzap_set_speed(self, actuator_id: int, speed: int):
        """MightyZap 속도 명령을 전달한다."""
        self.enqueue(MakePacket.set_speed(actuator_id, speed))

    def send_mightyzap_set_current(self, actuator_id: int, current: int):
        """MightyZap 전류 제한 명령을 전달한다."""
        self.enqueue(MakePacket.set_current(actuator_id, current))

    def send_mightyzap_force_onoff(self, actuator_id: int, onoff: int):
        """기존 on/off 의미를 유지한 채 MightyZap force 모드를 전환한다."""
        self.enqueue(
            MakePacket.set_force_onoff(actuator_id, 1 if onoff else 0)
        )

    def send_pipette_change_volume(self, actuator_id: int, direction: int, duty: int):
        """정규화한 방향과 duty 값으로 피펫 용량용 DC 모터를 구동한다."""
        direction = 0 if int(direction) <= 0 else 1
        duty = max(0, min(100, int(duty)))
        self.enqueue(
            MakePacket.pipette_change_volume(actuator_id, direction, duty)
        )

    def send_pipette_stop(self, actuator_id: int):
        """duty 0 명령을 보내 피펫 용량용 DC 모터를 정지시킨다."""
        self.enqueue(
            MakePacket.pipette_change_volume(actuator_id, 0, 0)
        )
