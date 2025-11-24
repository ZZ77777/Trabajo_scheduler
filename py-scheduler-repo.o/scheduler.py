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
    """Bind a pod to a node using the Kubernetes Binding API"""
    # Movido el target antes
    target = client.V1ObjectReference(
        kind="Node",
        api_version="v1",
        name=node_name
    )
    
    # metadatos
    meta = client.V1ObjectMeta(name=pod.metadata.name)
    
    # Pasando target como constructor, no como asignamiento
    body = client.V1Binding(target=target, metadata=meta)
    
    # La API de binding tiene un bug de deserializacion pero binding aún esto funciona??????
    try:
        api.create_namespaced_binding(
            namespace=pod.metadata.namespace,
            body=body,
            _preload_content=False  # Saltando deserializacion para evitar el bug
        )
    except ValueError as e:
        print(e)
        pass


def choose_node(api: client.CoreV1Api, pod) -> str:
    """Choose a node using least-loaded strategy"""
    nodes = api.list_node().items
    if not nodes:
        raise RuntimeError("No nodes available")

    # Filtrar nodos listos
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
            
            # Filtrar pods pending sin nodo asignado
            pending_pods = [
                p for p in all_pods
                if not p.spec.node_name  # No nodo asignado
                and p.status.phase == "Pending"  # En estado pending 
                and p.spec.scheduler_name == args.scheduler_name  # Para este scheduler
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