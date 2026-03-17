"""현재 용량 확인과 자동 목표 이동 시작/중지를 담당하는 패널."""

from PyQt5.QtWidgets import (
    QGroupBox, QLabel, QVBoxLayout, QHBoxLayout,
    QPushButton, QSpinBox
)
from worker.paths import FRAME_JPG_PATH
import os
from PyQt5.QtGui import QPixmap


class TargetPanel(QGroupBox):
    def __init__(self, controller):
        """목표값 입력과 자동 제어 시작/정지에 필요한 UI를 구성한다."""
        super().__init__("Target Control (OCR(TRT) + Motor)")

        self.controller = controller

        self.target_spin = QSpinBox()
        self.target_spin.setRange(0, 9999)
        self.target_spin.setValue(0)

        self.btn_read = QPushButton("Read Current Volume (OCR)")
        self.btn_start = QPushButton("Run To Target")
        self.btn_stop = QPushButton("Stop Run")

        self.status = QLabel("Status: Idle")
        self.status.setWordWrap(True)

        self.btn_read.clicked.connect(self.on_read)
        self.btn_start.clicked.connect(self.on_start)
        self.btn_stop.clicked.connect(self.on_stop)

        top = QHBoxLayout()
        top.addWidget(QLabel("Target Volume:"))
        top.addWidget(self.target_spin)
        top.addStretch(1)
        top.addWidget(self.btn_read)
        top.addWidget(self.btn_start)
        top.addWidget(self.btn_stop)

        layout = QVBoxLayout()
        layout.addLayout(top)
        layout.addWidget(self.status)
        self.setLayout(layout)

    def on_read(self):
        """현재 눈금을 바로 읽어보고 싶을 때 사용하는 OCR 단발 호출이다."""
        res = self.controller.ocr_read_volume(camera_index=0)
        if not res.ok:
            self.status.setText("Status: OCR failed.")
            return

        v = int(res.data.get("volume", -1))
        self.status.setText(f"Status: OCR OK, current={v:04d}")


    def on_start(self):
        """입력된 목표값 기준으로 자동 보정 루프를 시작한다."""
        t = int(self.target_spin.value())
        self.status.setText(f"Status: Running to target {t:04d} (see terminal logs)...")
        self.controller.start_run_to_target(target=t, camera_index=0)

    def on_stop(self):
        """실행 중인 자동 보정 루프를 중단하고 화면 상태를 즉시 바꾼다."""
        self.controller.stop_run_to_target()
        self.status.setText("Status: Stopped.")

    def update_camera_frame(self):
        """예전에 직접 preview를 갱신하던 흔적으로, 현재 메인 흐름에서는 사용하지 않는다."""
        if not os.path.exists(FRAME_JPG_PATH):
            return

        pixmap = QPixmap(FRAME_JPG_PATH)
        self.camera_label.setPixmap(pixmap)
