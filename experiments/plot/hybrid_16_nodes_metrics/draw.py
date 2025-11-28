import re
import matplotlib
import matplotlib.pyplot as plt
import seaborn as sns
import pandas as pd
from matplotlib import font_manager

# ================== 1. 解析日志 ==================

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

# ================== 2. 读取所有 policy 的数据并构造 DataFrame ==================

metrics = []
for i, pol in enumerate(policy):
    path = f"filter/0{i+1}.txt"
    print(f"Parsing log file: {path}")
    metric = parse_log_file(path)
    metrics.append(metric)

# 把 metrics 转成长表：每行 = 一个 (policy, duration, cpu, gpu, mixed)
records = []
for pol, metric in zip(policy, metrics):
    for duration, vals in metric.items():
        cpu = vals["cpu"]
        gpu = vals["gpu"]
        mixed = (cpu + gpu) / 2.0   # 混合利用率：简单取平均
        hours = duration / 3600.0   # 横坐标：按小时

        records.append({
            "policy": pol,
            "duration": duration,
            "hours": hours,
            "cpu": cpu,
            "gpu": gpu,
            "mixed": mixed,
        })

df = pd.DataFrame(records)
# 按时间排序，画出来更顺眼
df = df.sort_values(by=["hours", "policy"]).reset_index(drop=True)

# ================== 3. 画图风格设置（参考你给的脚本） ==================

PAPER_PLOT = True   # 论文画图
SAVEFIG = True      # 是否保存为 pdf

matplotlib.rcdefaults()
matplotlib.rcParams['pdf.fonttype'] = 42

# ---- 自动选择一个可用的中文字体 ----
candidate_fonts = [
    "Noto Sans CJK SC",    # Ubuntu: fonts-noto-cjk
    "WenQuanYi Micro Hei",
    "WenQuanYi Zen Hei",
    "SimHei",              # Windows
    "Microsoft YaHei",
]
available = {f.name for f in font_manager.fontManager.ttflist}
for fname in candidate_fonts:
    if fname in available:
        matplotlib.rcParams["font.family"] = fname
        print("Use Chinese font:", fname)
        break
else:
    print("Warning: No known Chinese font found, Chinese may show as squares.")

# 避免负号变成方块
matplotlib.rcParams["axes.unicode_minus"] = False

if PAPER_PLOT:
    matplotlib.rcParams.update({"font.size": 24})
    matplotlib.rcParams['lines.linewidth'] = 4
else:
    matplotlib.rcParams.update({"font.size": 14})
    matplotlib.rcParams['lines.linewidth'] = 2

# 颜色和顺序：和你原来的 policy 顺序保持一致
policy_keep = ['Random', 'Tetris', 'Synergy', 'MLaaS', 'Eva', 'DRIFT']
policy_keepr = ['DRIFT', 'Eva', 'MLaaS', 'Synergy', 'Tetris', 'Random']

colors = sns.color_palette()
colors.reverse()
colors = colors[-6:]
palette = {p: c for p, c in zip(policy_keep, colors)}

# ================== 4. 画 3 张图：CPU / GPU / 混合利用率 ==================

def plot_metric(metric_col, ylabel, fname):
    plt.figure(figsize=(10, 3.5), dpi=120)
    sns.lineplot(
        data=df,
        x="hours",
        y=metric_col,
        hue="policy",
        style="policy",
        hue_order=policy_keep,
        style_order=policy_keepr,
        palette=[palette[p] for p in policy_keep],
        estimator=None,     # 不做聚合，直接连点
        errorbar=None,
    )

    plt.grid(linestyle='-.', alpha=0.8)
    plt.xlabel("时间 (小时)")
    plt.ylabel(ylabel)
    plt.xlim(0, None)
    plt.ylim(0, 100)   # 如果数据不在 0~100 可以自己调

    if PAPER_PLOT:
        plt.legend(loc='upper left', bbox_to_anchor=(1, 1.05),
                   prop={'size': 20}, frameon=False, borderpad=0, ncol=3)
    else:
        plt.legend(ncol=3)

    if SAVEFIG:
        plt.savefig(fname, bbox_inches='tight')
    else:
        plt.show()


if __name__ == "__main__":
    plot_metric("cpu",   "CPU 利用率 (%)",            "util_cpu.pdf")
    plot_metric("gpu",   "GPU 利用率 (%)",            "util_gpu.pdf")
    plot_metric("mixed", "混合利用率 (CPU/GPU 平均, %)", "util_mixed.pdf")
