import pandas as pd
import numpy as np
import yaml
from pathlib import Path

NODE_NUM = 16
NODE_CSV = "csv/openb_node_list_gpu_node.csv"
OUTPUT_DIR = "long_term_16_nodes_hybrid"

def analyze_and_generate_nodes_to_yaml(csv_file, x, output_yaml_file):
    df = pd.read_csv(csv_file)
    
    df_filtered = df[['cpu_milli', 'memory_mib', 'gpu']].astype(int)
    
    specs_counts = df_filtered.value_counts()
    
    total_count = len(df_filtered)
    
    specs_proportion = specs_counts / total_count
    
    selected_nodes = []
    for spec, proportion in specs_proportion.items():
        count_to_select = int(np.round(proportion * x))
        
        nodes_with_spec = df_filtered[df_filtered.apply(lambda row: tuple(row) == spec, axis=1)]
        
        selected_nodes.append(nodes_with_spec.sample(n=count_to_select, random_state=42))
    
    final_selected_nodes = pd.concat(selected_nodes)

    node_list = []
    total_gpu_count = 0
    for idx, row in final_selected_nodes.iterrows():
        node_name = f"openb-node-{idx:04d}"
        
        gpu_count = row['gpu']
        total_gpu_count = total_gpu_count + gpu_count
        gpu_milli = gpu_count * 1000
        
        node_yaml = {
            'apiVersion': 'v1',
            'kind': 'Node',
            'metadata': {
                'labels': {
                    'beta.kubernetes.io/os': 'linux',
                    'kubernetes.io/hostname': node_name,
                    'kubernetes.io/os': 'linux',
                },
                'name': node_name
            },
            'status': {
                'allocatable': {
                    'alibabacloud.com/gpu-count': str(gpu_count),
                    'alibabacloud.com/gpu-milli': str(gpu_milli),
                    'cpu': f"{row['cpu_milli']}m",
                    'memory': f"{row['memory_mib']}Mi",
                    'pods': '1001',
                },
                'capacity': {
                    'alibabacloud.com/gpu-count': str(gpu_count),
                    'alibabacloud.com/gpu-milli': str(gpu_milli),
                    'cpu': f"{row['cpu_milli']}m",
                    'memory': f"{row['memory_mib']}Mi",
                    'pods': '1001',
                }
            }
        }
        node_list.append(node_yaml)
    
    with open(output_yaml_file, 'w') as f:
        for node_yaml in node_list:
            yaml.dump(node_yaml, f, default_flow_style=False, allow_unicode=True)
            f.write("\n---\n")

    print(f"nodes yaml save to {output_yaml_file}")
    return total_gpu_count

def generate_long_term_nodes():
    node_csv = NODE_CSV
    node_num = NODE_NUM
    output_path = OUTPUT_DIR + "/openb_node_list_gpu_node.yaml"
    output_dir_path = Path(OUTPUT_DIR)
    output_dir_path.mkdir(exist_ok=True)
    total_gpu_count = analyze_and_generate_nodes_to_yaml(node_csv, node_num, output_path)
    return total_gpu_count

import yaml
import pandas as pd
from pathlib import Path

POD_CSV_FILE = "csv/openb_pod_list_default.csv"

USAGE_PROMPT="""Usage:
python3 pod_csv_to_yaml.py data/csv/openb_pod_list_gpuspec10.csv
"""
OUTPUT_DIR_DEFAULT="data/new_output"

MILLI = 1000
DATA_CREATION_TIME = "creation_time"
DATA_DELETION_TIME = "deletion_time"

ResourceName = "alibabacloud.com/gpu-milli"      # GPU milli, i.e., 1000 == 1 GPU, for pod only, node is 1000 by default
CountName    = "alibabacloud.com/gpu-count"      # GPU number request (or allocatable), for pod and node
DeviceIndex  = "alibabacloud.com/gpu-index"      # Exists when the pod are assigned/predefined to a GPU device
ModelName    = "alibabacloud.com/gpu-card-model" # GPU card model, for pod and node
AssumeTime   = "alibabacloud.com/assume-time"    # To retrieve the scheduling latency
CreationTime = "alibabacloud.com/creation-time"  # creation timestamp
DeletionTime = "alibabacloud.com/deletion-time"  # deletion timestamp
RelateveCreationTime = "drift.scheduler/relative-creation-time"
RelativeDuration = "drift.scheduler/relative-duration"
PodNsNameSep = "/"
DevIdSep     = "-"

def generate_pod_yaml(workload_name='paib-pod-10',
                      workload_namespace='paib-gpu',
                      container_name='main',
                      container_image='tensorflow:latest',
                      container_requests={'cpu': '6000m'},
                      container_limits={'cpu': '6000m'},
                      node_selector_node_ip="",
                      annotations={},
                      labels={}):
    pod_template = """
    apiVersion: v1
    kind: Pod
    metadata:
      name: single-pod
    spec:
      containers:
      - name: php-redis
        image: gcr.io/google-samples/gb-frontend:v4
        imagePullPolicy: Always
        resources:
          requests:
            cpu: 100m
          limits:
            cpu: 100m
      restartPolicy: "OnFailure"
      dnsPolicy: "Default"
    """
    workload_yaml = yaml.safe_load(pod_template)
    workload_yaml['metadata']['name'] = workload_name
    workload_yaml['metadata']['namespace'] = workload_namespace
    workload_yaml['spec']['containers'][0]['name'] = container_name
    workload_yaml['spec']['containers'][0]['image'] = container_image
    workload_yaml['spec']['containers'][0]['resources']['requests'] = container_requests
    workload_yaml['spec']['containers'][0]['resources']['limits'] = container_limits

    if len(node_selector_node_ip) > 0:
        if 'nodeSelector' not in workload_yaml['spec']:
            workload_yaml['spec']['nodeSelector'] = {}
        workload_yaml['spec']['nodeSelector']['node-ip'] = node_selector_node_ip
    elif 'nodeSelector' in workload_yaml['spec']:
        if 'node-ip' in workload_yaml["spec"]["nodeSelector"]:
            del workload_yaml['spec']['nodeSelector']['node-ip']

    for k, v in annotations.items():
        if 'annotations' not in workload_yaml['metadata']:
            workload_yaml['metadata']['annotations'] = {}
        if v is not None:
            workload_yaml['metadata']['annotations'][k] = v  # e.g., {"alibabacloud.com/gpu-index":"2-3-4"}
    for k, v in labels.items():
        workload_yaml['metadata'][k] = v

    return workload_yaml

def get_random_hour(max_hours) -> int:
    return np.random.randint(1, max_hours + 1)

def get_random_duration(index, total_count) -> str:
    max_hours = 8
    duration = 1
    if index + max_hours < total_count:
        duration = get_random_hour(max_hours)
    return f"{duration * 3600}"
        
def output_pod(dfp, outfile='pod.yaml', node_select=False, nodes_total_gpu_count=1):
    num_pod = len(dfp)
    nodes_total_gpu_milli = nodes_total_gpu_count * 1000
    pods_total_gpu_milli = 0

    # 总数
    pod_count = len(dfp)
    
    for index, row in dfp.iterrows():
        if 'name' in row: 
            workload_name = row['name']
        elif 'job_id' in row:
            workload_name = f"job-{row['job_id']:04}" # float is not allowed
        else:
            exit("neither name nor job_id in row")
           
        container_requests = {}
        if 'cpu_milli' in row:
            container_requests['cpu'] = "%dm" % (row['cpu_milli'])
        elif 'cpu' in row:
            container_requests['cpu'] = "%dm" % (row['cpu'] * MILLI)
        elif 'num_cpu' in row:
            container_requests['num_cpu'] = "%dm" % (row['num_cpu'] * MILLI)
        else:
            exit("neither cpu_milli nor cpu in row")
        if 'memory_mib' in row:
            container_requests['memory'] = "%dMi" % row['memory_mib']
        container_limits = container_requests.copy()

        host_node_ip = row['ip'] if node_select else ""

        annotations = {}
        if int(row['num_gpu']) != 0:
            if node_select:
                annotations[DeviceIndex] = row['gpu_index'] if type(row['gpu_index']) == str else ""
            if 'gpu_milli' not in row:
                annotations[ResourceName] = 1000
            else:
                annotations[ResourceName] = "%d" % (int(row['gpu_milli'])) if 0 < row['gpu_milli'] <= 1000 else "1000" if row['gpu_milli'] > 1000 else "0"
            annotations[CountName] = "%d" % (int(row['num_gpu']))
          
            if 'gpu_spec' in row:
                gpu_req_val = [x for x in row['gpu_spec'].split('|') if len(x) > 0]
                gpu_req_out = "|".join(x for x in gpu_req_val)
                if len(gpu_req_out) > 0:
                    annotations[ModelName] = gpu_req_out
        # annotations[CreationTime] = "%s" % row[DATA_CREATION_TIME] if DATA_CREATION_TIME in row else None
        # annotations[DeletionTime] = "%s" % row[DATA_DELETION_TIME] if DATA_DELETION_TIME in row else None
        pods_total_gpu_milli = pods_total_gpu_milli + (int(annotations[ResourceName]) if ResourceName in annotations else 0)
        annotations[RelateveCreationTime] = f"{3600 * (pods_total_gpu_milli // nodes_total_gpu_milli)}"
        annotations[RelativeDuration] = get_random_duration(index, pod_count)

        pod_yaml = generate_pod_yaml(workload_name=workload_name, container_requests=container_requests,
                                     container_limits=container_limits, node_selector_node_ip=host_node_ip,
                                     annotations=annotations)

        if index == 0:
            with open(outfile, 'w') as file:
                yaml.dump(pod_yaml, file)
        else:
            with open(outfile, 'a') as file:
                file.writelines(['\n---\n\n'])
                yaml.dump(pod_yaml, file)

    ratio = pods_total_gpu_milli / nodes_total_gpu_milli
    print("ratio:", ratio)
    
    

if __name__ == '__main__':
    total_gpu_count = generate_long_term_nodes()
    print(f"Total GPU count in generated nodes: {total_gpu_count}")
    pod_csv_file = POD_CSV_FILE
    
    dfp = pd.read_csv(pod_csv_file, dtype={'gpu_index': str})
    if 'gpu_spec' in dfp:
        dfp.gpu_spec = dfp.gpu_spec.fillna('')
    
    output_dir = OUTPUT_DIR
    if len(output_dir) <= 0:
        output_dir_path = Path(OUTPUT_DIR_DEFAULT)
    else:
        output_dir_path = Path(output_dir)
    output_dir_path.mkdir(exist_ok=True)

    pod_yaml_file = output_dir_path / ("openb_pod_list_default" + '.yaml') # .csv to .yaml
    output_pod(dfp, pod_yaml_file, node_select=False, nodes_total_gpu_count=total_gpu_count)
    print("OUTPUT: %s (len: %d)" % (pod_yaml_file, len(dfp)))
    
