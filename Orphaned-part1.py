# part1.py
from kubernetes import client, config
from kubernetes.dynamic import DynamicClient
from concurrent.futures import ThreadPoolExecutor, as_completed
import logging
import pandas as pd
import os

# ------------------------------------------------------------------------------
# INITIALIZATION & CONFIGURATION
# ------------------------------------------------------------------------------
logging.basicConfig(
    filename="k8s_unused_resources.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)

# Load Kubernetes configuration (from kubeconfig or in-cluster)
config.load_kube_config()
v1 = client.CoreV1Api()
apps_v1 = client.AppsV1Api()
batch_v1 = client.BatchV1Api()
rbac_v1 = client.RbacAuthorizationV1Api()
networking_v1 = client.NetworkingV1Api()
apiextensions_v1 = client.ApiextensionsV1Api()
storage_v1 = client.StorageV1Api()
dynamic_client = DynamicClient(client.ApiClient())

# ------------------------------------------------------------------------------
# GET NAMESPACES
# ------------------------------------------------------------------------------
def get_namespaces():
    """
    Read namespaces from a text file (one per line) if available,
    otherwise use all namespaces in the cluster.
    """
    filename = "namespaces.txt"
    if os.path.exists(filename):
        with open(filename, "r") as f:
            namespaces = [line.strip() for line in f if line.strip()]
        logging.info(f"Using namespaces from file: {namespaces}")
        return namespaces
    else:
        ns_list = [ns.metadata.name for ns in v1.list_namespace().items]
        logging.info(f"No '{filename}' file found. Using all namespaces: {ns_list}")
        return ns_list

NAMESPACES = get_namespaces()

# ------------------------------------------------------------------------------
# GET CLUSTER CONTEXT (Cluster Name)
# ------------------------------------------------------------------------------
def get_cluster_context():
    contexts, active_context = config.list_kube_config_contexts()
    return active_context["context"].get("cluster", "UnknownCluster")

CLUSTER_CONTEXT = get_cluster_context()
logging.info(f"Active Cluster Context: {CLUSTER_CONTEXT}")

# ------------------------------------------------------------------------------
# HELPER FUNCTIONS & UTILITIES
# ------------------------------------------------------------------------------
def skip_due_to_label(resource):
    """
    Skip resources with a specific label. For instance, if the label
    "do-not-delete" is set to "true", the resource is skipped.
    """
    labels = resource.metadata.labels or {}
    return labels.get("do-not-delete", "false").lower() == "true"

def is_pvc_unused(pvc):
    """
    Determine if a PVC is unused. Even if bound, if no running or pending pod
    is mounting it, it is considered unused.
    """
    if pvc.status.phase != "Bound":
        return True  # Unbound PVCs are unused.
    pvc_name = pvc.metadata.name
    ns = pvc.metadata.namespace
    try:
        pods = v1.list_namespaced_pod(ns).items
    except Exception as e:
        logging.error(f"Error listing pods in namespace {ns}: {e}")
        return False
    for pod in pods:
        if pod.status.phase in ["Running", "Pending"]:
            if pod.spec.volumes:
                for vol in pod.spec.volumes:
                    if vol.persistent_volume_claim and vol.persistent_volume_claim.claim_name == pvc_name:
                        return False  # Found a pod using this PVC.
    return True

def is_orphaned(resource):
    """
    Check if a resource is orphaned by verifying if its ownerReferences can be resolved.
    """
    if not resource.metadata.owner_references:
        return False
    for owner in resource.metadata.owner_references:
        try:
            res = dynamic_client.resources.get(api_version=owner.api_version, kind=owner.kind)
            if resource.metadata.namespace:
                res.get(name=owner.name, namespace=resource.metadata.namespace)
            else:
                res.get(name=owner.name)
        except Exception as e:
            logging.info(f"Resource {resource.metadata.name} orphaned due to missing owner {owner.name}: {e}")
            return True
    return False

# ------------------------------------------------------------------------------
# CORE RESOURCE DETECTION FUNCTIONS
# ------------------------------------------------------------------------------

def find_unused_pvcs():
    """Detect unused PVCs across all namespaces."""
    unused = []
    def process_namespace(ns):
        ns_unused = []
        try:
            pvcs = v1.list_namespaced_persistent_volume_claim(ns).items
            for pvc in pvcs:
                if skip_due_to_label(pvc):
                    continue
                if is_pvc_unused(pvc):
                    ns_unused.append(f"{ns}/{pvc.metadata.name}")
        except Exception as e:
            logging.error(f"Error processing PVCs in namespace {ns}: {e}")
        return ns_unused

    with ThreadPoolExecutor() as executor:
        futures = {executor.submit(process_namespace, ns): ns for ns in NAMESPACES}
        for future in as_completed(futures):
            unused.extend(future.result())
    return unused

def find_unused_pvs():
    """Detect unused Persistent Volumes."""
    unused = []
    try:
        pvs = v1.list_persistent_volume().items
        for pv in pvs:
            if skip_due_to_label(pv):
                continue
            if pv.status.phase in ["Released", "Available"] or is_orphaned(pv):
                unused.append(pv.metadata.name)
    except Exception as e:
        logging.error(f"Error in find_unused_pvs: {e}")
    return unused

def find_unused_configmaps_and_secrets():
    """
    Detect ConfigMaps and Secrets that are not mounted by any pod.
    Returns two lists: unused configmaps and unused secrets.
    """
    used_configmaps, used_secrets = set(), set()
    def process_namespace(ns):
        local_cm, local_sec = set(), set()
        try:
            pods = v1.list_namespaced_pod(ns).items
            for pod in pods:
                if pod.spec.volumes:
                    for vol in pod.spec.volumes:
                        if vol.config_map:
                            local_cm.add(vol.config_map.name)
                        if vol.secret:
                            local_sec.add(vol.secret.secret_name)
        except Exception as e:
            logging.error(f"Error processing pods in namespace {ns}: {e}")
        return local_cm, local_sec

    with ThreadPoolExecutor() as executor:
        futures = {executor.submit(process_namespace, ns): ns for ns in NAMESPACES}
        for future in as_completed(futures):
            cm, sec = future.result()
            used_configmaps.update(cm)
            used_secrets.update(sec)
    try:
        all_configmaps = {cm.metadata.name for cm in v1.list_config_map_for_all_namespaces().items}
        all_secrets = {sec.metadata.name for sec in v1.list_secret_for_all_namespaces().items}
    except Exception as e:
        logging.error(f"Error listing all ConfigMaps/Secrets: {e}")
        return [], []
    return list(all_configmaps - used_configmaps), list(all_secrets - used_secrets)
      
