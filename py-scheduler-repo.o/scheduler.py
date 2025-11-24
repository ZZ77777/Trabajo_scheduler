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
    """Bind a pod to a node by patching spec.nodeName"""
    # Patch the pod to assign it to the node
    #body = {
    #    "spec": {
    #        "nodeName": node_name
    #    }
    #}
    
    #api.patch_namespaced_pod(
    #    name=pod.metadata.name,
    #    namespace=pod.metadata.namespace,
    #    body=body
    #)
    
    binding = {
        "apiVersion": "v1",
        "kind": "Binding",
        "metadata": {
            "name": pod.metadata.name
        },
        "target": {
            "apiVersion": "v1",
            "kind": "Node",
            "name": node_name
        }
    }
    
    api_response = api.api_client.call_api(
        f'/api/v1/namespaces/{pod.metadata.namespace}/pods/{pod.metadata.name}/binding',
        'POST',
        body=binding,
        header_params={'Content-Type': 'application/json'},
        response_type=object,
        _preload_content=False,
        _return_http_data_only=False
    )
    
    if api_response[1] not in [200, 201]:
        raise Exception(f"Binding failed with status {api_response[1]}")


def choose_node(api: client.CoreV1Api, pod) -> str:
    """Choose a node using least-loaded strategy"""
    nodes = api.list_node().items
    if not nodes:
        raise RuntimeError("No nodes available")

    # Filtrar nodos de tipo Ready
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

    # Contar pods por nodo
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
            # Obtener todos los pods
            all_pods = api.list_pod_for_all_namespaces().items
            
            # Filter pending pods without node assignment
            pending_pods = [
                p for p in all_pods
                if not p.spec.node_name  # No nodo asignado
                and p.status.phase == "Pending"  # Estado Pending
                and p.spec.scheduler_name == args.scheduler_name  
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