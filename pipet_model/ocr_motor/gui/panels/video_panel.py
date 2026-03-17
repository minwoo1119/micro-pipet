"""운영자가 현재 카메라 프레임과 최근 OCR 결과를 확인하는 preview 패널."""

import os
from PyQt5.QtWidgets import (
    QGroupBox, QLabel, QVBoxLayout, QHBoxLayout,
    QPushButton, QSpinBox
)
from PyQt5.QtCore import Qt
from PyQt5.QtGui import QPixmap

from worker.paths import FRAME_JPG_PATH


class VideoPanel(QGroupBox):
    def __init__(self, controller):
        """카메라 선택과 단일 프레임 확인에 필요한 UI를 구성한다."""
        super().__init__("Preview (single-frame capture)")

        self.controller = controller

        self.video_label = QLabel("No image yet.\nPress 'Capture Frame'")
        self.video_label.setAlignment(Qt.AlignCenter)
        self.video_label.setFixedSize(600, 960)
        self.video_label.setStyleSheet("border: 1px solid #999;")

        self.volume_label = QLabel("Latest Volume: ----")
        self.volume_label.setAlignment(Qt.AlignLeft)

        self.camera_spin = QSpinBox()
        self.camera_spin.setRange(0, 10)
        self.camera_spin.setValue(0)

        self.btn_capture = QPushButton("Capture Frame")
        self.btn_capture.clicked.connect(self.on_capture)

        top = QHBoxLayout()
        top.addWidget(QLabel("Camera Index:"))
        top.addWidget(self.camera_spin)
        top.addStretch(1)
        top.addWidget(self.volume_label)
        top.addStretch(1)
        top.addWidget(self.btn_capture)

        layout = QVBoxLayout()
        layout.addLayout(top)
        layout.addWidget(self.video_label)
        self.setLayout(layout)

        self._last_image_path = None

    def set_latest_volume(self, v: int):
        """다른 패널에서 읽은 최신 용량값을 상단 표시 영역에 반영한다."""
        self.volume_label.setText(f"Latest Volume: {v:04d}")

    def show_image(self, path: str):
        """worker가 저장한 이미지를 읽어 preview 영역에 맞게 보여준다."""
        if not path or not os.path.exists(path):
            self.video_label.setText("Image not found.")
            return

        self._last_image_path = path
        pix = QPixmap(path)
        self.video_label.setPixmap(
            pix.scaled(
                self.video_label.size(),
                Qt.KeepAspectRatio,
                Qt.SmoothTransformation,
            )
        )

    def on_capture(self):
        """운영자가 직접 프레임 1장을 다시 캡처하고 싶을 때 호출된다."""
        cam = int(self.camera_spin.value())

        res = self.controller.capture_frame(camera_index=cam)
        if not res.ok:
            self.video_label.setText(
                "Capture failed.\nCheck worker stderr in terminal."
            )
            return

        self.show_image(FRAME_JPG_PATH)
