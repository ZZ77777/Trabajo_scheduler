import argparse, math
from kubernetes import client, config, watch
from kubernetes.client.rest import ApiException


def load_client(kubeconfig=None):
    """Load Kubernetes API client"""
    if kubeconfig:
        config.load_kube_config(kubeconfig)
    else:
        config.load_incluster_config()
    return client.CoreV1Api()


def bind_pod(api: client.CoreV1Api, pod, node_name: str):
    """Bind a pod to a node using the Kubernetes Binding API"""
    target = client.V1ObjectReference(
        kind="Node",
        api_version="v1",
        name=node_name
    )
    
    meta = client.V1ObjectMeta(name=pod.metadata.name)
    
    # Pass target in constructor, not as assignment
    body = client.V1Binding(target=target, metadata=meta)
    
    # The binding API has a deserialization bug but binding still works
    try:
        api.create_namespaced_binding(
            namespace=pod.metadata.namespace,
            body=body,
            _preload_content=False  # Skip deserialization to avoid bug
        )
    except ValueError:
        # Expected error due to client library bug - binding was successful
        pass


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
    args = parser.parse_args()

    api = load_client(args.kubeconfig)
    print(f"[watch] scheduler starting… name={args.scheduler_name}")

    w = watch.Watch()
    
    # Stream Pod events across all namespaces
    while True:
        try:
            for evt in w.stream(
                api.list_pod_for_all_namespaces,
                _request_timeout=60
            ):
                obj = evt['object']
                evt_type = evt['type']
                
                # Validate event object
                if obj is None or not hasattr(obj, 'spec'):
                    continue
                
                # Only act on ADDED or MODIFIED events for Pending pods
                if evt_type not in ['ADDED', 'MODIFIED']:
                    continue
                
                # Filter: Pending pods without node assignment for our scheduler
                if (not obj.spec.node_name and 
                    obj.status.phase == "Pending" and
                    obj.spec.scheduler_name == args.scheduler_name):
                    
                    try:
                        node = choose_node(api, obj)
                        print(f"[{evt_type}] Attempting to bind {obj.metadata.namespace}/{obj.metadata.name} -> {node}")
                        bind_pod(api, obj, node)
                        print(f"✓ Successfully bound {obj.metadata.namespace}/{obj.metadata.name} -> {node}")
                    
                    except ApiException as e:
                        if e.status == 409:
                            print(f"Pod {obj.metadata.name} already bound (conflict)")
                        else:
                            print(f"API error binding {obj.metadata.name}: {e.status} - {e.reason}")
                    
                    except Exception as e:
                        print(f"Error binding {obj.metadata.name}: {type(e).__name__}: {e}")
        
        except Exception as e:
            print(f"Watch stream error: {type(e).__name__}: {e}")
            print("Restarting watch stream in 5 seconds...")
            import time
            time.sleep(5)


if __name__ == "__main__":
    main()
