#!/usr/bin/env python3
import json
import sys
import subprocess
import shlex
import os

def parse_args(arg_string):
    # 使用 shlex.split 处理参数字符串，支持引号等
    return shlex.split(arg_string)

def update_config(config_path, new_args):
    with open(config_path, 'r') as f:
        config = json.load(f)

    config['process']['args'] = ['/stress-ng'] + new_args
    with open(config_path, 'w') as f:
        json.dump(config, f, indent=4)
    print(f"Updated args in {config_path} to: {config['process']['args']}")

def run_container():
    # 假设使用 runc 运行容器
    command = ['runc', 'run', '-b', "/runc/containers/stress-ng", 'stress-ng']
    try:
        subprocess.run(command, cwd=os.getcwd(), check=True)
        # print("Container started successfully.")
    except subprocess.CalledProcessError as e:
        print(f"Error running container: {e}")

def main():
    if len(sys.argv) != 2:
        print("Usage: run_stress_ng.py \"<args string>\"")
        print("Example: run_stress_ng.py \"-c 1 --cpu-load 100 --cpu-method all -t 60s --metrics-brief\"")
        sys.exit(1)

    config_path = "config.json"
    arg_string = sys.argv[1]

    if not os.path.isfile(config_path):
        print(f"Error: {config_path} does not exist.")
        sys.exit(1)

    new_args = parse_args(arg_string)
    update_config(config_path, new_args)
    run_container()

if __name__ == "__main__":
    main()
