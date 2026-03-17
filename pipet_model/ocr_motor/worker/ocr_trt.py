"""저장된 ROI 4개를 TensorRT 모델로 읽어 최종 용량값을 만드는 OCR 모듈."""

import json
import os
import numpy as np
import cv2
import tensorrt as trt
import pycuda.driver as cuda
import pycuda.autoinit  # noqa

from PIL import Image
from torchvision import transforms

from worker.paths import OCR_TRT_PATH, ROIS_JSON_PATH

# =============================
# OCR preprocessing settings (TRAIN/VAL과 동일)
# =============================
INPUT_SIZE = (224, 224)
NORM_MEAN = [0.485, 0.456, 0.406]  # RGB
NORM_STD  = [0.229, 0.224, 0.225]  # RGB
VOLUME_WEIGHTS = [1000, 100, 10, 1]


# =========================================================
# TensorRT Wrapper
# =========================================================
class TRTWrapper:
    """TensorRT 엔진 로딩과 배치 추론 1회를 담당하는 얇은 래퍼."""

    def __init__(self, engine_path: str):
        if not os.path.exists(engine_path):
            raise FileNotFoundError(engine_path)

        logger = trt.Logger(trt.Logger.WARNING)
        with open(engine_path, "rb") as f:
            runtime = trt.Runtime(logger)
            self.engine = runtime.deserialize_cuda_engine(f.read())

        self.context = self.engine.create_execution_context()
        self.stream = cuda.Stream()

        self.input_name = None
        self.output_name = None

        n_tensors = self.engine.num_io_tensors
        for i in range(n_tensors):
            name = self.engine.get_tensor_name(i)
            mode = self.engine.get_tensor_mode(name)
            if mode == trt.TensorIOMode.INPUT:
                self.input_name = name
            else:
                self.output_name = name

        if self.input_name is None or self.output_name is None:
            self.input_name = self.engine.get_tensor_name(0)
            self.output_name = self.engine.get_tensor_name(1)

    def infer(self, x_nchw: np.ndarray):
        """전처리된 배치를 한 번 추론하고 자리별 클래스/신뢰도를 반환한다."""
        if x_nchw.dtype != np.float32:
            x_nchw = x_nchw.astype(np.float32)

        N, C, H, W = x_nchw.shape

        self.context.set_input_shape(self.input_name, (N, C, H, W))
        out_shape = tuple(self.context.get_tensor_shape(self.output_name))

        host_input = cuda.pagelocked_empty((N, C, H, W), dtype=np.float32)
        host_output = cuda.pagelocked_empty(out_shape, dtype=np.float32)

        d_input = cuda.mem_alloc(host_input.nbytes)
        d_output = cuda.mem_alloc(host_output.nbytes)

        self.context.set_tensor_address(self.input_name, int(d_input))
        self.context.set_tensor_address(self.output_name, int(d_output))

        np.copyto(host_input, x_nchw)

        cuda.memcpy_htod_async(d_input, host_input, self.stream)
        self.context.execute_async_v3(stream_handle=self.stream.handle)
        cuda.memcpy_dtoh_async(host_output, d_output, self.stream)
        self.stream.synchronize()

        d_input.free()
        d_output.free()

        logits = host_output.reshape((N, -1))
        e_x = np.exp(logits - np.max(logits, axis=1, keepdims=True))
        prob = e_x / np.sum(e_x, axis=1, keepdims=True)

        cls = prob.argmax(axis=1).astype(int)
        conf = prob[np.arange(N), cls]
        return cls.tolist(), conf.tolist(), prob


# =========================================================
# torchvision preprocessing (TRAIN 코드와 100% 동일)
# =========================================================
_preprocess = transforms.Compose([
    transforms.Resize(INPUT_SIZE, antialias=True),
    transforms.ToTensor(),  # [0,1], CHW
    transforms.Normalize(mean=NORM_MEAN, std=NORM_STD),
])

def preprocess_roi_bgr_trt(roi_bgr: np.ndarray) -> np.ndarray:
    """학습 시 사용한 전처리와 맞추기 위해 ROI를 TRT 입력 텐서 형태로 변환한다."""
    rgb = cv2.cvtColor(roi_bgr, cv2.COLOR_BGR2RGB)
    pil = Image.fromarray(rgb)

    x = _preprocess(pil)          # torch.Tensor (3,H,W)
    x = x.numpy().astype(np.float32)
    return x


# =========================================================
# ROI loading
# =========================================================
def load_rois():
    """YOLO가 state 디렉터리에 남긴 최신 ROI 파일을 읽어온다."""
    if not os.path.exists(ROIS_JSON_PATH):
        raise FileNotFoundError(f"ROIs not found: {ROIS_JSON_PATH}")

    with open(ROIS_JSON_PATH, "r", encoding="utf-8") as f:
        rois = json.load(f)

    if not isinstance(rois, list) or len(rois) == 0:
        raise RuntimeError(f"Invalid ROIs: {rois}")

    return rois


# =========================================================
# Main OCR logic (TRT)
# =========================================================
def read_volume_trt(frame: np.ndarray, trt_model: TRTWrapper) -> int:
    """프레임에서 ROI 4개를 잘라 자리별 숫자를 읽고 최종 용량값으로 조합한다."""
    rois = load_rois()

    # 현재 장비 기준으로 ROI는 위에서 아래 순서가 천/백/십/일 자리다.
    rois = sorted(rois, key=lambda r: r[1])

    h, w = frame.shape[:2]
    crops = []

    for i, (x, y, rw, rh) in enumerate(rois[:4]):
        x1 = max(0, min(w - 1, int(x)))
        y1 = max(0, min(h - 1, int(y)))
        x2 = max(0, min(w, x1 + int(rw)))
        y2 = max(0, min(h, y1 + int(rh)))

        crop = frame[y1:y2, x1:x2]
        if crop.size == 0:
            raise RuntimeError(f"Empty ROI{i}")

        # 자리별 crop을 남겨두면 현장 디버깅 시 어느 자리에서 오인식이 났는지 바로 확인할 수 있다.
        cv2.imwrite(f"/tmp/ocr_roi_{i}.jpg", crop)
        crops.append(crop)

    if len(crops) < 4:
        raise RuntimeError("Not enough ROIs")

    batch = np.stack(
        [preprocess_roi_bgr_trt(c) for c in crops],
        axis=0
    ).astype(np.float32)

    pred_cls, pred_conf, _ = trt_model.infer(batch)
    digits = [int(d) for d in pred_cls[:4]]

    volume = sum(d * w for d, w in zip(digits, VOLUME_WEIGHTS))
    return volume
