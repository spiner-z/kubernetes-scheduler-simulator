import re

def parse_log_file(path):
    # duration -> {"cpu": float, "gpu": float}
    result = {}

    # 正则表达式：提取第二个 CPU、第二个 GPU、duration
    pattern = re.compile(
        r'CPU=.*?,\s*([\d\.]+)%;\s*GPU=.*?,\s*([\d\.]+)%;\s*duration=([\d\.]+)s'
    )

    with open(path, "r") as f:
        for line in f:
            m = pattern.search(line)
            if not m:
                continue

            cpu2 = float(m.group(1))   # 第二个 CPU
            gpu2 = float(m.group(2))   # 第二个 GPU
            duration = float(m.group(3))

            # 去重：相同 duration 取后出现的
            result[duration] = {
                "cpu": cpu2,
                "gpu": gpu2,
            }

    return result

policy = ['Random', 'Tetris', 'Synergy', 'MLaaS', 'Eva', 'DRIFT']
# ====== 使用示例 ======
if __name__ == "__main__":
    metrics = []
    for i in range(len(policy)):
        path = f"filter/0{i+1}.log"
        print(f"Parsing log file: {path}")
        metric = parse_log_file(path)
        metrics.append(metric)
    
