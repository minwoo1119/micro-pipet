"""TensorRT 엔진의 텐서 메타데이터를 빠르게 확인하는 스크립트."""

import tensorrt as trt
import os

ENGINE_PATH = "models/ocr/efficientnet_b0_fp16.trt"

assert os.path.exists(ENGINE_PATH), ENGINE_PATH

logger = trt.Logger(trt.Logger.INFO)

with open(ENGINE_PATH, "rb") as f:
    runtime = trt.Runtime(logger)
    engine = runtime.deserialize_cuda_engine(f.read())

print("\n========== TRT ENGINE INFO ==========")
print("num_io_tensors:", engine.num_io_tensors)
print("has_implicit_batch:", engine.has_implicit_batch_dimension)

for i in range(engine.num_io_tensors):
    name = engine.get_tensor_name(i)
    mode = engine.get_tensor_mode(name)
    dtype = engine.get_tensor_dtype(name)
    shape = engine.get_tensor_shape(name)

    print(f"\n[TENSOR {i}]")
    print(" name :", name)
    print(" mode :", mode)     # INPUT / OUTPUT
    print(" dtype:", dtype)    # FLOAT / HALF / INT8
    print(" shape:", shape)
