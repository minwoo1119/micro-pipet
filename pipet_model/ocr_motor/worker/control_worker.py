"""현재 OCR 값을 기준으로 다음 모터 명령을 계산하는 비전 기반 제어 루프."""

import json
import time
import sys

from worker.camera import capture_one_frame
from worker.ocr_trt import TRTWrapper, read_volume_trt
from worker.paths import OCR_TRT_PATH

VOLUME_TOLERANCE = 1
SETTLE_TIME = 0.7
MAX_ITER = 60


def _elog(msg: str):
    """stdout은 기계 파싱용으로 비워두기 위해 사람이 읽는 로그는 stderr로 분리한다."""
    print(msg, file=sys.stderr, flush=True)


def run_to_target(
    target: int,
    camera_index: int = 0,
    max_iter: int = MAX_ITER,
):
    """현재 용량을 반복 측정하면서 다음 보정 명령을 단계별 JSON으로 내보낸다."""
    print(">>> ENTER run_to_target()", flush=True)
    _elog("[RUN] run_to_target started (VISION ONLY)")

    print("[DEBUG] before TRT load", flush=True)
    trt_model = TRTWrapper(OCR_TRT_PATH)
    print("[DEBUG] after TRT load", flush=True)

    final_volume = None
    success = False

    for step in range(max_iter):
        print("[DEBUG] before capture", flush=True)
        frame = capture_one_frame(camera_index)
        print("[DEBUG] after capture", flush=True)
        cur_volume = int(read_volume_trt(frame, trt_model))
        err = target - cur_volume

        final_volume = cur_volume

        # 허용 오차 안으로 들어오면 호출부가 추가 제어 없이 종료할 수 있다.
        if abs(err) <= VOLUME_TOLERANCE:
            print(json.dumps({
                "cmd": "done",
                "step": step,
                "current": cur_volume,
                "target": target,
                "error": err,
            }), flush=True)

            _elog("[DONE] target reached")

            success = True
            reason = "done"
            break

        direction = 1 if err < 0 else 0
        abs_err = abs(err)

        if abs_err >= 300:
            duty = 60
            duration_ms = 300
        elif abs_err >= 100:
            duty = 45
            duration_ms = 250
        elif abs_err >= 30:
            duty = 35
            duration_ms = 200
        else:
            duty = 25
            duration_ms = 150

        _elog(
            f"[STEP {step}] cur={cur_volume} err={err} "
            f"dir={'CCW' if direction==1 else 'CW'} duty={duty} dur={duration_ms}ms"
        )

        # 실제 모터 구동은 GUI 쪽에서 하므로, 여기서는 필요한 제어값만 전달한다.
        print(json.dumps({
            "cmd": "volume",
            "step": step,
            "current": cur_volume,
            "target": target,
            "error": err,
            "direction": direction,
            "duty": duty,
            "duration_ms": duration_ms,
        }), flush=True)

        time.sleep(SETTLE_TIME)

    else:
        print(json.dumps({
            "cmd": "warn",
            "status": "max_iter"
        }), flush=True)

        _elog("[WARN] max_iter reached")

        success = False
        reason = "max_iter"

    _elog("[CLEANUP] run_to_target finished")

    # 테스트 코드에서는 이 요약값만 확인해도 전체 결과를 판단할 수 있게 해둔다.
    return {
        "success": success,
        "final_ul": final_volume,
        "target_ul": target,
        "iterations": step + 1,
        "reason": reason
    }
