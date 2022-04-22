import pathlib
import subprocess
from time import sleep

import yaml

cluster_sizes = [3, 6]
scenario_sizes = [4, 8, 16, 32]

print(f"Cluster sizes: {cluster_sizes}")
print(f"Scenario sizes: {scenario_sizes}")

HERE = pathlib.Path(__file__).parent


def read_yaml(p):
    dicts = []
    with open(p, 'r') as f:
        y = yaml.load_all(f, Loader=yaml.FullLoader)
        for v in y:
            dicts.append(v)
    return dicts


def write_yaml(p, dicts):
    with open(p, 'w') as f:
        yaml.dump_all(dicts, f)


namespaces_yaml = HERE / "mbei-namespace.yaml"
testing_job_base_yaml = HERE / "testing-job-base.yaml"
component_base_yaml = HERE / "mbei-component-base.yaml"
component_base = read_yaml(component_base_yaml)
testing_job = read_yaml(testing_job_base_yaml)
component_service = component_base[0]
component_statefulset = component_base[1]
output_folder_path = HERE / "output-folder"

config_folder = "config-folder"

component_yaml = HERE / "mbei-component.yaml"
testing_job_yaml = HERE / "testing-job.yaml"


#Create namespace
print("Creating namespaces")
create_namespace_cmd = ["kubectl", "apply", "-f", str(namespaces_yaml.absolute())]
print(f"Running command: {' '.join(create_namespace_cmd)}")
code = subprocess.run(create_namespace_cmd)
if code.returncode != 0:
    exit(code)

#Set context new namespace
print("Setting namespace context")
set_namespace_context_cmd = ["kubectl", "config", "set-context", "--current", "--namespace=mbei"]
print(f"Running command: {' '.join(set_namespace_context_cmd)}")
code = subprocess.run(set_namespace_context_cmd)
if code.returncode != 0:
    exit(code)


for c in cluster_sizes:
    for s in scenario_sizes:
        #First we create the config
        print(f"Create config folder for scenario size {s} and cluster size {c}")
        create_config_folder_cmd = ["cargo", "run", "--bin", "mbei-testdata-config", "--", f"-n={c}",
                                    f"-p={config_folder}", "-o", "complex-factory", f"--size={s}"]
        print(f"Running command: {' '.join(create_config_folder_cmd)}")
        code = subprocess.run(create_config_folder_cmd)
        if code.returncode != 0:
            exit(code)

        #We delete any config map if it exists
        print("Deleting config map")
        delete_configmap_cmd = ["kubectl", "delete", "configmap", "component-config"]
        print(f"Running command (ok if this fails): {' '.join(delete_configmap_cmd)}")
        subprocess.run(delete_configmap_cmd)

        #We create the configmap from config folder
        print("Creating new configmap from config folder")
        create_configmap_cmd = ["kubectl", "create", "configmap", "component-config", f"--from-file={config_folder}"]
        print(f"Running command: {' '.join(create_configmap_cmd)}")
        code = subprocess.run(create_configmap_cmd)
        if code.returncode != 0:
            exit(code)

        component_statefulset['spec']['replicas'] = c
        write_yaml(component_yaml, component_base)

        # We delete the service if it exists
        print("Deleting service if it exists")
        delete_service_cmd = ["kubectl", "delete", "-f", str(component_yaml.absolute())]
        print(f"Running command (ok if this fails): {' '.join(delete_service_cmd)}")
        subprocess.run(delete_service_cmd)
        # Sleep a little to ensure cleanup is done
        sleep(10)

        # Delete testing pod if it exists
        print("Deleting testing job if it exists")
        delete_testing_job_cmd = ["kubectl", "delete", "-f", str(testing_job_yaml.absolute())]
        print(f"Running command (ok if this fails): {' '.join(delete_testing_job_cmd)}")
        subprocess.run(delete_testing_job_cmd)
        # Sleep a little to ensure cleanup is done
        sleep(2)

        # Create the service
        print("Starting service")
        create_service_cmd = ["kubectl", "apply", "-f", str(component_yaml.absolute())]
        print(f"Running command: {' '.join(create_service_cmd)}")
        code = subprocess.run(create_service_cmd)
        if code.returncode != 0:
            exit(code)

        # Wait for service to be created
        print("Waiting for service to be ready")
        wait_for_service_cmd = ["kubectl", "wait", "pods", "--selector", "app=mbei-component", "--for=condition=Ready", "--timeout=360s"]
        print(f"Running command: {' '.join(wait_for_service_cmd)}")
        code = subprocess.run(wait_for_service_cmd)
        if code.returncode != 0:
            exit(code)
        # Connections between pods need to be established, not reflected in running status of pods
        sleep(120)

        #Set the correct command for the testing job, write to disk
        testing_job[0]['spec']['template']['spec']['containers'][0]['command'] = ["/bin/bash", "-c",
                                                              f"/usr/local/bin/mbei-testdata-writer -n=100000 -o=msgs.yaml complex-factory -s={s} &> writer.txt && " +\
                                                              "/usr/local/bin/mbei-testdata-file-producer -i=msgs.yaml -u=/etc/config/names-url-map.yaml"]
        write_yaml(testing_job_yaml, testing_job)

        #Start benchmark
        print("Starting benchmark")
        start_testing_cmd = ["kubectl", "apply", "-f", str(testing_job_yaml.absolute())]
        print(f"Running command: {' '.join(start_testing_cmd)}")
        code = subprocess.run(start_testing_cmd)
        if code.returncode != 0:
            exit(code)
        sleep(5)

        #Wait for benchmark to complete
        print("Waiting for benchmark to complete")
        wait_for_benchmark_cmd = ["kubectl", "wait", "job.batch/perftest-job", "--for=condition=complete", "--timeout=600s"]
        print(f"Running command: {' '.join(wait_for_benchmark_cmd)}")
        code = subprocess.run(wait_for_benchmark_cmd)
        if code.returncode != 0:
            exit(code)
        sleep(5)

        #Check pod scheduling, from: https://medium.com/@johnjjung/building-a-kubernetes-daemonstatefulset-30ad0592d8cb
        with open(str((output_folder_path / f'{c}-{s}-pod-scheduling.txt').absolute()), 'w') as f:
            get_scheduling_cmd = ["kubectl", "get", "pod", "-o=custom-columns=NODE:.spec.nodeName,NAME:.metadata.name"]
            # get_producer_logs_cmd = ["kubectl", "cp", "mbei/perftest - pod:/scripts/producer.txt", str((output_folder_path / f"{c}-{s}-producer.txt").absolute())]
            print(f"Running command: {' '.join(get_scheduling_cmd)}")
            code = subprocess.run(get_scheduling_cmd, stdout=f)
            if code.returncode != 0:
                exit(code)

        #Get logs from containers
        print("Getting producer logs from container")
        with open(str((output_folder_path / f'{c}-{s}-producer.txt').absolute()), 'w') as f:
            get_producer_logs_cmd = ["kubectl", "logs", "job.batch/perftest-job"]
            #get_producer_logs_cmd = ["kubectl", "cp", "mbei/perftest - pod:/scripts/producer.txt", str((output_folder_path / f"{c}-{s}-producer.txt").absolute())]
            print(f"Running command: {' '.join(get_producer_logs_cmd)}")
            code = subprocess.run(get_producer_logs_cmd, stdout=f)
            if code.returncode != 0:
                exit(code)

        for i in range(c):
            with open(str((output_folder_path / f'{c}-{s}-mbei-component-{i}.txt').absolute()), 'w') as f:
                get_container_logs_cmd = ["kubectl", "logs", f"pod/mbei-component-{i}", "mbei-component"]
                print(f"Running command: {' '.join(get_container_logs_cmd)}")
                code = subprocess.run(get_container_logs_cmd, stdout=f)
                if code.returncode != 0:
                    exit(code)