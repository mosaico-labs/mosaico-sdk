import struct
import sys
from pathlib import Path

from rosbags.highlevel import AnyReader

ASSETS = Path(sys.argv[1])
OUT = Path(sys.argv[2])
N = int(sys.argv[3]) if len(sys.argv) > 3 else 60
JOBS = [
    ("r2b_galileo2_0.mcap", "/camera/color/image_raw", "g2_color"),
    ("r2b_robotarm_0.mcap", "/camera_1/color/image_raw", "arm_color"),
]

OUT.mkdir(parents=True, exist_ok=True)
for bag, topic, name in JOBS:
    with AnyReader([ASSETS / bag]) as reader:
        connections = [c for c in reader.connections if c.topic == topic]
        step = max(1, connections[0].msgcount // N)
        kept = 0
        with open(OUT / f"{name}.frames", "wb") as out:
            for i, (connection, _, raw) in enumerate(reader.messages(connections=connections)):
                if i % step or kept >= N:
                    continue
                msg = reader.deserialize(raw, connection.msgtype)
                data = bytes(msg.data)
                out.write(struct.pack("<III", msg.height, msg.step, len(data)) + data)
                kept += 1
        print(name, msg.width, msg.height, msg.encoding, kept, "frames", flush=True)
