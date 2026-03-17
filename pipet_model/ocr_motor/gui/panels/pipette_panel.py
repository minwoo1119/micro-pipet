"""자동 제어와 별개로 현장에서 수동 조작이 필요할 때 쓰는 액추에이터 제어 패널."""

from PyQt5.QtWidgets import (
    QWidget, QVBoxLayout, QHBoxLayout, QGridLayout,
    QLabel, QPushButton, QLineEdit, QGroupBox, QMessageBox
)
from PyQt5.QtCore import Qt

from gui.controller import Controller
from worker.actuator_volume_dc import VolumeDCActuator


class PipettePanel(QWidget):
    """
    C# Form1 Pipette / End-Effector FULL panel (1:1 대응)
    - 리니어 버튼: 토글 방식 (하강 ↔ 상승)
    - 버튼 텍스트 상태에 따라 변경
    """

    def __init__(self, controller: Controller, parent=None):
        """수동 조작도 동일 시리얼 세션을 쓰도록 컨트롤러의 연결을 그대로 재사용한다."""
        super().__init__(parent)

        self.controller = controller

        # ===== Volume Rotary DC motor =====
        self.volume_dc = VolumeDCActuator(
            serial=self.controller.serial,
            actuator_id=0x0C,
        )

        # ===== 상태 플래그 (C# bool 변수 대응) =====
        self._pipetting_down = False
        self._tip_down = False
        self._volume_down = False

        self._build_ui()

    # ==========================================================
    # UI
    # ==========================================================
    def _build_ui(self):
        """현장 조작에 필요한 리니어/회전 모터 제어 UI를 한 곳에 모아 구성한다."""
        main = QVBoxLayout(self)
        main.addWidget(QLabel("<b>Pipette / End-Effector Control</b>"))

        # ======================================================
        # Linear Toggle Control
        # ======================================================
        linear_box = QGroupBox("Linear Motor Control")
        linear_layout = QHBoxLayout(linear_box)

        # --- 흡인분주 ---
        self.btn_pip = QPushButton("흡인분주 하강")
        self.btn_pip.clicked.connect(self._toggle_pipetting)
        linear_layout.addWidget(self.btn_pip)

        # --- 팁 교체 ---
        self.btn_tip = QPushButton("팁 교체 하강")
        self.btn_tip.clicked.connect(self._toggle_tip_change)
        linear_layout.addWidget(self.btn_tip)

        # --- 용량 조절 ---
        self.btn_vol = QPushButton("용량 조절 하강")
        self.btn_vol.clicked.connect(self._toggle_volume_linear)
        linear_layout.addWidget(self.btn_vol)

        main.addWidget(linear_box)

        # ======================================================
        # Linear Move (목표 위치 이동)
        # ======================================================
        move_box = QGroupBox("Linear Motor - Move To Position")
        grid = QGridLayout(move_box)

        self.tb_pip_pos = QLineEdit()
        self.tb_tip_pos = QLineEdit()
        self.tb_vol_pos = QLineEdit()

        grid.addWidget(QLabel("흡인분주 목표"), 0, 0)
        grid.addWidget(self.tb_pip_pos, 0, 1)
        grid.addWidget(
            self._btn("이동", lambda: self._linear_move(0x0B, self.tb_pip_pos)),
            0, 2
        )

        grid.addWidget(QLabel("팁 교체 목표"), 1, 0)
        grid.addWidget(self.tb_tip_pos, 1, 1)
        grid.addWidget(
            self._btn("이동", lambda: self._linear_move(0x0B, self.tb_tip_pos)),
            1, 2
        )

        grid.addWidget(QLabel("용량 조절 목표"), 2, 0)
        grid.addWidget(self.tb_vol_pos, 2, 1)
        grid.addWidget(
            self._btn("이동", lambda: self._linear_move(0x0A, self.tb_vol_pos)),
            2, 2
        )

        main.addWidget(move_box)

        # ======================================================
        # Rotary Volume Control (중공축 DC 모터)
        # ======================================================
        rotary_box = QGroupBox("Volume Rotary Motor")
        rotary_layout = QVBoxLayout(rotary_box)

        duty_layout = QHBoxLayout()
        duty_layout.addWidget(QLabel("Duty"))
        self.tb_duty = QLineEdit("40")
        duty_layout.addWidget(self.tb_duty)
        rotary_layout.addLayout(duty_layout)

        btn_row = QHBoxLayout()

        btn_cw = QPushButton("CW")
        btn_ccw = QPushButton("CCW")
        btn_stop = QPushButton("정지")

        # C# MouseDown / MouseUp 대응
        btn_cw.pressed.connect(lambda: self._rotary_start(direction=1))
        btn_cw.released.connect(self.volume_dc.stop)

        btn_ccw.pressed.connect(lambda: self._rotary_start(direction=0))
        btn_ccw.released.connect(self.volume_dc.stop)

        btn_stop.clicked.connect(self.volume_dc.stop)

        btn_row.addWidget(btn_cw)
        btn_row.addWidget(btn_stop)
        btn_row.addWidget(btn_ccw)

        rotary_layout.addLayout(btn_row)
        main.addWidget(rotary_box)

        main.addStretch(1)

    # ==========================================================
    # Toggle handlers (C# Button Click 로직 대응)
    # ==========================================================
    def _toggle_pipetting(self):
        """흡인분주 축의 현재 상태를 기준으로 상/하 동작을 번갈아 수행한다."""
        if not self._pipetting_down:
            self.controller.pipetting_down()
            self.btn_pip.setText("흡인분주 상승")
        else:
            self.controller.pipetting_up()
            self.btn_pip.setText("흡인분주 하강")

        self._pipetting_down = not self._pipetting_down

    def _toggle_tip_change(self):
        """팁 교체 축의 현재 상태를 기준으로 상/하 동작을 번갈아 수행한다."""
        if not self._tip_down:
            self.controller.tip_change_down()
            self.btn_tip.setText("팁 교체 상승")
        else:
            self.controller.tip_change_up()
            self.btn_tip.setText("팁 교체 하강")

        self._tip_down = not self._tip_down

    def _toggle_volume_linear(self):
        """용량 조절 축의 현재 상태를 기준으로 상/하 동작을 번갈아 수행한다."""
        if not self._volume_down:
            self.controller.volume_down()
            self.btn_vol.setText("용량 조절 상승")
        else:
            self.controller.volume_up()
            self.btn_vol.setText("용량 조절 하강")

        self._volume_down = not self._volume_down

    # ==========================================================
    # Helpers
    # ==========================================================
    def _btn(self, text, cb):
        """반복되는 버튼 생성 코드를 줄이기 위한 내부 헬퍼다."""
        b = QPushButton(text)
        b.clicked.connect(cb)
        return b

    def _linear_move(self, actuator_id: int, edit: QLineEdit):
        """운영자가 직접 입력한 목표 위치로 해당 리니어 액추에이터를 이동시킨다."""
        try:
            pos = int(edit.text())
            self.controller.linear_move(actuator_id, pos)
        except Exception as e:
            QMessageBox.critical(self, "Error", str(e))

    def _rotary_start(self, direction: int):
        """입력창의 duty 값을 기준으로 회전형 용량 모터를 수동 시작한다."""
        try:
            duty = int(self.tb_duty.text())
            self.volume_dc.run(direction=direction, duty=duty)
        except Exception as e:
            QMessageBox.critical(self, "Error", str(e))
