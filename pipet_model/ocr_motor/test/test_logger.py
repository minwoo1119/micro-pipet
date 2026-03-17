"""반복 OCR / 모터 테스트 결과를 CSV로 남기기 위한 로깅 헬퍼."""

import csv
from datetime import datetime

LOG_PATH = "logs/batch_test_log.csv"

def init_log():
    """배치 테스트 스크립트에서 사용하는 컬럼으로 새 CSV 파일을 만든다."""
    with open(LOG_PATH, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow([
            "order",
            "target_ul",
            "target_ml",
            "final_ocr_ul",
            "success",
            "elapsed_sec",
            "timestamp"
        ])

def append_log(order, target_ul, target_ml, final_ul, success, elapsed):
    """완료된 테스트 결과 1건을 CSV 로그에 추가한다."""
    with open(LOG_PATH, "a", newline="") as f:
        writer = csv.writer(f)
        writer.writerow([
            order,
            target_ul,
            target_ml,
            final_ul,
            success,
            round(elapsed, 2),
            datetime.now().isoformat()
        ])
