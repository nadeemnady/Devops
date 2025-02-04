import logging
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
from kubernetes import client, config, dynamic
from kubernetes.client import ApiClient
import os
import re

# ==============================================================================
# CONFIGURATION & INITIALIZATION
# ==============================================================================

logging.basicConfig(
    filename="k8s_unused_resources.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)

# Load Kubernetes configuration (from ~/.kube/config or in-cluster config)
config.load_kube_config()

# Retrieve the active cluster context from kubeconfig
def get_cluster_context():
    contexts, active_context = config.list_kube_config_contexts()
    return active_context["context"].get("cluster", "UnknownCluster")

CLUSTER_CONTEXT = get_cluster_context()
logging.info(f"Active Cluster Context: {CLUSTER_CONTEXT}")

# Initialize typed API clients and dynamic client
v1 = client.CoreV1Api()
apps_v1 = client.AppsV1Api()
batch_v1 = client.BatchV1Api()
rbac_v1 = client.RbacAuthorizationV1Api()
networking_v1 = client.NetworkingV1Api()
apiextensions_v1 = client.ApiextensionsV1Api()
storage_v1 = client.StorageV1Api()
dynamic_client = dynamic.DynamicClient(ApiClient())

# ==============================================================================
# HELPER FUNCTIONS
# ==============================================================================

def get_namespaces():
    """
    Return namespaces from file if available, else all namespaces.
    """
    if os.path.exists("namespaces.txt"):
        with open("namespaces.txt", "r") as f:
            namespaces = [line.strip() for line in f.readlines() if line.strip()]
        logging.info(f"Scanning namespaces (from file): {namespaces}")
        return namespaces
    else:
        ns_list = [ns.metadata.name for ns in v1.list_namespace().items]
        logging.info(f"No namespaces.txt found. Scanning all namespaces: {ns_list}")
        return ns_list

NAMESPACES = get_namespaces()

def parse_duration(duration_str):
    """
    Parse a duration string (e.g. '1h30m') and return a timedelta.
    Supports hours (h), minutes (m), and seconds (s).
    """
    pattern = r'((?P<hours>\d+)h)?((?P<minutes>\d+)m)?((?P<seconds>\d+)s)?'
    match = re.match(pattern, duration_str)
    if not match:
        return None
    parts = match.groupdict(default="0")
    return timedelta(
        hours=int(parts["hours"]),
        minutes=int(parts["minutes"]),
        seconds=int(parts["seconds"])
    )

def skip_due_to_label(obj):
    """
    Checks for an override label "kor/used":
      - "true": skip this resource (treat as used).
      - "false": force it to be considered unused.
    Returns True, False, or None.
    """
    labels = obj.metadata.labels or {}
    if "kor/used" in labels:
        val = labels["kor/used"].lower()
        if val == "true":
            return True
        elif val == "false":
            return False
    return None

def is_resource_expired(obj, ttl_annotation="orphanTTL", default_ttl=timedelta(days=15)):
    """
    (This function is retained for future use if needed.)
    Checks if the resource is older than the TTL.
    Uses TTL annotation if present; otherwise defaults to 15 days.
    """
    annotations = obj.metadata.annotations or {}
    if ttl_annotation in annotations:
        ttl = parse_duration(annotations[ttl_annotation])
        if ttl:
            age = datetime.utcnow() - obj.metadata.creation_timestamp.replace(tzinfo=None)
            return age > ttl
    age = datetime.utcnow() - obj.metadata.creation_timestamp.replace(tzinfo=None)
    return age > default_ttl

def is_orphaned(obj):
    """
    Checks owner references.
    Returns True if any owner reference is unresolvable.
    """
    if not obj.metadata.owner_references:
        return False
    for owner in obj.metadata.owner_references:
        try:
            res = dynamic_client.resources.get(api_version=owner.api_version, kind=owner.kind)
            if obj.metadata.namespace:
                res.get(name=owner.name, namespace=obj.metadata.namespace)
            else:
                res.get(name=owner.name)
        except Exception as e:
            logging.info(f"Resource {obj.metadata.name} orphaned due to missing owner {owner.name}: {e}")
            return True
    return False

# ==============================================================================
# ADVANCED UNUSED RESOURCE DETECTION FUNCTIONS (PART 1)
# ==============================================================================
# (Detection functions for PVs, PVCs, ConfigMaps/Secrets, Pods, Services,
# Deployments, and StatefulSets.)

def find_unused_pvs():
    try:
        pvs = v1.list_persistent_volume().items
        unused = []
        for pv in pvs:
            if skip_due_to_label(pv) is True:
                continue
            if pv.status.phase == "Available" or is_orphaned(pv):
                unused.append(pv.metadata.name)
        return unused
    except Exception as e:
        logging.error(f"Error in find_unused_pvs: {e}")
        return []

def find_unused_pvcs():
    """
    Enhanced PVC check:
    - Retrieve all PVCs for each namespace.
    - Build a set of PVCs referenced by pods in "Running" or "Pending" state.
    - Also check if a PVC is Bound but its associated PV is in Released/Failed state.
    - Mark PVC as unused if not Bound or if Bound but not referenced.
    """
    unused = []
    def process_namespace(ns):
        ns_unused = []
        try:
            pvc_list = v1.list_namespaced_persistent_volume_claim(ns).items
            referenced = set()
            pods = v1.list_namespaced_pod(ns).items
            for pod in pods:
                if pod.status.phase in ["Running", "Pending"]:
                    if pod.spec.volumes:
                        for vol in pod.spec.volumes:
                            if vol.persistent_volume_claim:
                                referenced.add(vol.persistent_volume_claim.claim_name)
            for pvc in pvc_list:
                if skip_due_to_label(pvc) is True:
                    continue
                if pvc.status.phase != "Bound" or pvc.metadata.name not in referenced:
                    ns_unused.append(f"{ns}/{pvc.metadata.name}")
                else:
                    if pvc.spec.volumeName:
                        try:
                            pv = v1.read_persistent_volume(pvc.spec.volumeName)
                            if pv.status.phase in ["Released", "Failed"]:
                                ns_unused.append(f"{ns}/{pvc.metadata.name}")
                        except Exception as e:
                            logging.error(f"Error reading PV {pvc.spec.volumeName} for PVC {pvc.metadata.name}: {e}")
            return ns_unused
        except Exception as e:
            logging.error(f"Error in find_unused_pvcs for namespace {ns}: {e}")
            return []
    with ThreadPoolExecutor() as executor:
        futures = {executor.submit(process_namespace, ns): ns for ns in NAMESPACES}
        for future in as_completed(futures):
            unused.extend(future.result())
    return unused

def find_unused_configmaps_and_secrets():
    used_configmaps, used_secrets = set(), set()
    def process_namespace(ns):
        local_used_cm, local_used_sec = set(), set()
        try:
            pods = v1.list_namespaced_pod(ns).items
            for pod in pods:
                if pod.spec.volumes:
                    for vol in pod.spec.volumes:
                        if vol.config_map:
                            local_used_cm.add(vol.config_map.name)
                        if vol.secret:
                            local_used_sec.add(vol.secret.secret_name)
        except Exception as e:
            logging.error(f"Error scanning pods in {ns} for ConfigMap/Secret usage: {e}")
        return local_used_cm, local_used_sec
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

def find_unused_pods():
    unused = []
    def process_namespace(ns):
        ns_unused = []
        try:
            pods = v1.list_namespaced_pod(ns).items
            for pod in pods:
                if skip_due_to_label(pod) is True:
                    continue
                phase = pod.status.phase
                if phase in ["Succeeded", "Failed"]:
                    ns_unused.append(f"{ns}/{pod.metadata.name}")
            return ns_unused
        except Exception as e:
            logging.error(f"Error in find_unused_pods for namespace {ns}: {e}")
            return []
    with ThreadPoolExecutor() as executor:
        futures = {executor.submit(process_namespace, ns): ns for ns in NAMESPACES}
        for future in as_completed(futures):
            unused.extend(future.result())
    return unused

def find_unused_services():
    unused = []
    def process_namespace(ns):
        ns_unused = []
        try:
            services = v1.list_namespaced_service(ns).items
            for svc in services:
                if skip_due_to_label(svc) is True:
                    continue
                if svc.spec.type == "ExternalName":
                    continue
                try:
                    ep = v1.read_namespaced_endpoints(svc.metadata.name, ns)
                    if not ep.subsets:
                        ns_unused.append(f"{ns}/{svc.metadata.name}")
                except Exception as e:
                    logging.error(f"Error reading endpoints for service {ns}/{svc.metadata.name}: {e}")
                    ns_unused.append(f"{ns}/{svc.metadata.name}")
        except Exception as e:
            logging.error(f"Error in find_unused_services for namespace {ns}: {e}")
        return ns_unused
    with ThreadPoolExecutor() as executor:
        futures = {executor.submit(process_namespace, ns): ns for ns in NAMESPACES}
        for future in as_completed(futures):
            unused.extend(future.result())
    return unused

def find_unused_deployments():
    unused = []
    def process_namespace(ns):
        ns_unused = []
        try:
            deps = apps_v1.list_namespaced_deployment(ns).items
            for dep in deps:
                if skip_due_to_label(dep) is True:
                    continue
                desired = dep.spec.replicas or 0
                available = dep.status.available_replicas or 0
                if desired == 0 or available == 0:
                    ns_unused.append(f"{ns}/{dep.metadata.name}")
                elif is_orphaned(dep):
                    ns_unused.append(f"{ns}/{dep.metadata.name}")
            return ns_unused
        except Exception as e:
            logging.error(f"Error in find_unused_deployments for namespace {ns}: {e}")
            return []
    with ThreadPoolExecutor() as executor:
        futures = {executor.submit(process_namespace, ns): ns for ns in NAMESPACES}
        for future in as_completed(futures):
            unused.extend(future.result())
    return unused

def find_unused_statefulsets():
    unused = []
    def process_namespace(ns):
        ns_unused = []
        try:
            sts_list = apps_v1.list_namespaced_stateful_set(ns).items
            for sts in sts_list:
                if skip_due_to_label(sts) is True:
                    continue
                if (sts.spec.replicas or 0) == 0:
                    ns_unused.append(f"{ns}/{sts.metadata.name}")
            return ns_unused
        except Exception as e:
            logging.error(f"Error in find_unused_statefulsets for namespace {ns}: {e}")
            return []
    with ThreadPoolExecutor() as executor:
        futures = {executor.submit(process_namespace, ns): ns for ns in NAMESPACES}
        for future in as_completed(futures):
            unused.extend(future.result())
    return unused
