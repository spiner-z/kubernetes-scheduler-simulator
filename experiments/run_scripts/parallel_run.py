import os
import subprocess
from concurrent.futures import ThreadPoolExecutor

# 全局变量
# SCRIPT_PATH = "experiments/run_scripts/run_scripts_0511.sh"  # 脚本文件路径
SCRIPT_PATH = "experiments/run_scripts/run_scripts_1028.sh"
MAX_PROCS = 8  # 最大并行进程数

def run_command(command):
    """执行每个命令"""
    try:
        # 执行命令，等待完成
        subprocess.run(command, shell=True, check=True)
        print(f"执行成功: {command}")
    except subprocess.CalledProcessError as e:
        print(f"执行失败: {command}, 错误: {e}")

def main():
    # 读取脚本文件，获取所有命令
    with open(SCRIPT_PATH, "r") as file:
        commands = [line.strip() for line in file.readlines() if line.strip()]

    # 使用 ThreadPoolExecutor 来并行执行命令
    with ThreadPoolExecutor(max_workers=MAX_PROCS) as executor:
        executor.map(run_command, commands)

if __name__ == "__main__":
    main()
