import argparse, time, math
from kubernetes import client, config

def load_client(kubeconfig=None):
    if kubeconfig:
        config.load_kube_config(kubeconfig)
    else:
        config.load_incluster_config()
    return client.CoreV1Api()

def bind_pod(api: client.CoreV1Api, pod, node_name: str):
    target = client.V1ObjectReference(
        api_version="v1",
        kind="Node",
        name=node_name
    )

    body = client.V1Binding(
        metadata=client.V1ObjectMeta(
            name=pod.metadata.name,
            namespace=pod.metadata.namespace
        ),
        target=target
    )

    #api.create_namespaced_binding(
    #    namespace=pod.metadata.namespace,
    #    body=body
    #)
    
    api.create_namespaced_pod_binding(
        name=pod.metadata.name,
        namespace=pod.metadata.namespace,
        body=body
    )


def choose_node(api: client.CoreV1Api, pod) -> str:
    
    
    nodes = api.list_node().items
    if not nodes:
        raise RuntimeError("No nodes available")

    pods = api.list_pod_for_all_namespaces().items
    min_cnt = math.inf
    pick = nodes[0].metadata.name

    for n in nodes:
        cnt = sum(1 for p in pods if p.spec.node_name == n.metadata.name)
        if cnt < min_cnt:
            min_cnt = cnt
            pick = n.metadata.name
            
    print("DEBUG nodes:", [n.metadata.name for n in nodes])
    print("DEBUG pick:", pick)


    return pick

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--scheduler-name", default="my-scheduler")
    parser.add_argument("--kubeconfig", default=None)
    parser.add_argument("--interval", type=float, default=2.0)
    args = parser.parse_args()

    api = load_client(args.kubeconfig)
    print(f"[polling] scheduler starting… name={args.scheduler_name}")

    while True:
        #pods = api.list_pod_for_all_namespaces(
        #    field_selector="spec.nodeName="
        #).items
        
        all_pods = api.list_pod_for_all_namespaces().items
        #pods = [p for p in all_pods if p.spec.node_name is None]
        pods = [
            p for p in all_pods
            if not p.spec.node_name  # esto captura None, "", null, etc.
            and p.status.phase == "Pending"
        ]




        for pod in pods:
            if pod.spec.scheduler_name != args.scheduler_name:
                continue
            try:
                node = choose_node(api, pod)
                print("DEBUG pod:", pod.metadata.name, "node:", node)
                bind_pod(api, pod, node)
                print(f"Bound {pod.metadata.namespace}/{pod.metadata.name} -> {node}")
            except Exception as e:
                print("error:", e)

        time.sleep(args.interval)

if __name__ == "__main__":
    main()
