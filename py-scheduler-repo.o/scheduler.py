import argparse, time, math
from kubernetes import client, config
from kubernetes.client.rest import ApiException

def load_client(kubeconfig=None):
    if kubeconfig:
        config.load_kube_config(kubeconfig)
    else:
        config.load_incluster_config()
    return client.CoreV1Api()

def bind_pod(api: client.CoreV1Api, pod, node_name: str):
    """Bind a pod to a node using direct HTTP POST to avoid client library bug"""
    binding = {
        "apiVersion": "v1",
        "kind": "Binding",
        "metadata": {
            "name": pod.metadata.name,
            "namespace": pod.metadata.namespace
        },
        "target": {
            "apiVersion": "v1",
            "kind": "Node",
            "name": node_name
        }
    }
    
    # Use the low-level API client to make the POST request directly
    # This avoids the V1Binding deserialization bug
    api_client = api.api_client
    path = f"/api/v1/namespaces/{pod.metadata.namespace}/pods/{pod.metadata.name}/binding"
    
    response = api_client.call_api(
        path,
        'POST',
        body=binding,
        response_type='V1Status',
        _return_http_data_only=True,
        _preload_content=True
    )
    
    return response


def choose_node(api: client.CoreV1Api, pod) -> str:
    """Choose a node using least-loaded strategy"""
    nodes = api.list_node().items
    if not nodes:
        raise RuntimeError("No nodes available")

    # Filter ready nodes
    ready_nodes = []
    for n in nodes:
        if n.status and n.status.conditions:
            for condition in n.status.conditions:
                if condition.type == "Ready" and condition.status == "True":
                    ready_nodes.append(n)
                    break
    
    if not ready_nodes:
        print("WARNING: No ready nodes found, using all nodes")
        ready_nodes = nodes

    # Count pods per node
    pods = api.list_pod_for_all_namespaces().items
    min_cnt = math.inf
    pick = ready_nodes[0].metadata.name

    for n in ready_nodes:
        cnt = sum(1 for p in pods if p.spec.node_name == n.metadata.name)
        if cnt < min_cnt:
            min_cnt = cnt
            pick = n.metadata.name
    
    print(f"DEBUG: Selected node {pick} with {min_cnt} pods")
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
        try:
            # Get all pods
            all_pods = api.list_pod_for_all_namespaces().items
            
            # Filter pending pods without node assignment
            pending_pods = [
                p for p in all_pods
                if not p.spec.node_name  # No node assigned
                and p.status.phase == "Pending"  # In Pending state
                and p.spec.scheduler_name == args.scheduler_name  # For our scheduler
            ]

            if pending_pods:
                print(f"Found {len(pending_pods)} pending pod(s) for {args.scheduler_name}")

            for pod in pending_pods:
                try:
                    node = choose_node(api, pod)
                    print(f"Attempting to bind {pod.metadata.namespace}/{pod.metadata.name} -> {node}")
                    bind_pod(api, pod, node)
                    print(f"✓ Successfully bound {pod.metadata.namespace}/{pod.metadata.name} -> {node}")
                except ApiException as e:
                    if e.status == 409:
                        print(f"Pod {pod.metadata.name} already bound (conflict)")
                    else:
                        print(f"API error binding {pod.metadata.name}: {e.status} - {e.reason}")
                except Exception as e:
                    print(f"Error binding {pod.metadata.name}: {type(e).__name__}: {e}")

        except Exception as e:
            print(f"Error in main loop: {type(e).__name__}: {e}")

        time.sleep(args.interval)


if __name__ == "__main__":
    main()