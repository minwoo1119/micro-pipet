"""저장된 프레임과 ROI 파일로 원본 PyTorch OCR 모델을 점검하는 디버그 스크립트."""

import os
import cv2
import json
import torch
import numpy as np
import timm

from PIL import Image
from torchvision import transforms

from worker.paths import FRAME_JPG_PATH, ROIS_JSON_PATH

# ==============================
# CONFIG
# ==============================
OCR_PT_PATH = "/home/sixr/Desktop/pipet_model/ocr_motor/best_efficientnet_origin.pt"
NUM_CLASSES = 10

# ==============================
# Load checkpoint metadata
# ==============================
ckpt = torch.load(OCR_PT_PATH, map_location="cpu")
print("[CKPT keys]", ckpt.keys())
print("[CKPT classes]", ckpt.get("classes"))

INPUT_SIZE = (224, 224)
NORM_MEAN = ckpt.get("norm_mean", [0.485, 0.456, 0.406])
NORM_STD  = ckpt.get("norm_std",  [0.229, 0.224, 0.225])

# ==============================
# Preprocess (TRAIN / VAL과 100% 동일)
# ==============================
preprocess = transforms.Compose([
    transforms.Resize(INPUT_SIZE, antialias=True),
    transforms.ToTensor(),                     # [0,1], CHW, RGB
    transforms.Normalize(NORM_MEAN, NORM_STD),
])


def preprocess_roi(roi_bgr: np.ndarray) -> torch.Tensor:
    """OpenCV ROI 1개를 PyTorch 모델이 기대하는 정규화 텐서로 변환한다."""
    rgb = cv2.cvtColor(roi_bgr, cv2.COLOR_BGR2RGB)
    pil = Image.fromarray(rgb)
    return preprocess(pil)   # (3,224,224) torch.Tensor


# ==============================
# Load OCR model (PT)
# ==============================
def load_ocr_model():
    """EfficientNet 구조를 다시 만들고 저장된 체크포인트 가중치를 올린다."""
    model = timm.create_model(
        "efficientnet_b0",
        pretrained=False,     # ✅ 반드시 False
        num_classes=NUM_CLASSES,
    )

    state_dict = ckpt["model"] if "model" in ckpt else ckpt
    model.load_state_dict(state_dict, strict=True)
    model.eval()
    return model


# ==============================
# MAIN (OCR ONLY)
# ==============================
def main():
    """저장된 프레임/ROI를 읽어 자리별 OCR을 수행하고 최종 용량을 출력한다."""
    print("=== OCR ONLY (FROM SAVED FILES, PT) ===")

    # ---------------------------------
    # 1. Load image
    # ---------------------------------
    assert os.path.exists(FRAME_JPG_PATH), f"Image not found: {FRAME_JPG_PATH}"
    frame = cv2.imread(FRAME_JPG_PATH)
    h, w = frame.shape[:2]

    # ---------------------------------
    # 2. Load ROIs
    # ---------------------------------
    assert os.path.exists(ROIS_JSON_PATH), f"ROIs not found: {ROIS_JSON_PATH}"
    with open(ROIS_JSON_PATH, "r") as f:
        rois = json.load(f)

    rois = sorted(rois, key=lambda r: r[1])  # top → bottom
    print(f"[INFO] Loaded {len(rois)} ROIs")

    # ---------------------------------
    # 3. OCR
    # ---------------------------------
    model = load_ocr_model()
    digits = []

    for i, (x, y, rw, rh) in enumerate(rois[:4]):
        x1 = max(0, min(w - 1, int(x)))
        y1 = max(0, min(h - 1, int(y)))
        x2 = max(0, min(w, x1 + int(rw)))
        y2 = max(0, min(h, y1 + int(rh)))

        crop = frame[y1:y2, x1:x2]
        if crop.size == 0:
            print(f"[WARN] ROI{i} empty")
            continue

        # debug save
        cv2.imwrite(f"/tmp/ocr_roi_{i}.jpg", crop)
        cv2.imshow(f"OCR ROI {i}", crop)

        x_in = preprocess_roi(crop).unsqueeze(0)  # (1,3,224,224)

        with torch.no_grad():
            logits = model(x_in)
            prob = torch.softmax(logits, dim=1)
            pred = int(prob.argmax(dim=1))
            conf = float(prob.max(dim=1).values)

        digits.append(pred)
        print(f"ROI{i}: digit={pred}, conf={conf:.3f}")

    # ---------------------------------
    # 4. Volume
    # ---------------------------------
    if len(digits) == 4:
        volume = digits[0]*1000 + digits[1]*100 + digits[2]*10 + digits[3]
        print(f"\n[VOLUME] {volume}")
    else:
        print("\n[VOLUME] skipped (ROI count != 4)")

    cv2.waitKey(0)
    cv2.destroyAllWindows()


if __name__ == "__main__":
    main()
