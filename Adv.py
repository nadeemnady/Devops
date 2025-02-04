import logging
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
from kubernetes import client, config, dynamic
from kubernetes.client import ApiClient
import pandas as pd
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
    """Return namespaces from file if available, else all namespaces."""
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
      - "true": skip this resource.
      - "false": force it to be considered unused.
    Returns:
      True, False, or None.
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
    Checks if the resource is older than the TTL.
    If a TTL annotation exists, that value is used; otherwise, defaults to 15 days.
    """
    annotations = obj.metadata.annotations or {}
    if ttl_annotation in annotations:
        ttl = parse_duration(annotations[ttl_annotation])
        if ttl:
            age = datetime.utcnow() - obj.metadata.creation_timestamp.replace(tzinfo=None)
            return age > ttl
    # Default TTL of 15 days
    age = datetime.utcnow() - obj.metadata.creation_timestamp.replace(tzinfo=None)
    return age > default_ttl

def is_orphaned(obj):
    """
    Checks owner references. If any owner reference is unresolvable, the resource is orphaned.
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
# ADVANCED UNUSED RESOURCE DETECTION FUNCTIONS
# ==============================================================================

def find_unused_pvs():
    try:
        pvs = v1.list_persistent_volume().items
        unused = []
        for pv in pvs:
            if skip_due_to_label(pv) is True:
                continue
            # Mark as unused if PV is in Available state or orphaned.
            if pv.status.phase == "Available" or is_orphaned(pv):
                unused.append(pv.metadata.name)
        return unused
    except Exception as e:
        logging.error(f"Error in find_unused_pvs: {e}")
        return []

def find_unused_pvcs():
    """
    Enhanced PVC check:
    - If PVC is not in Bound state, or if it is Bound but not attached to any running pod,
      then mark it as unused.
    """
    unused = []
    def process_namespace(ns):
        ns_unused = []
        try:
            pvc_list = v1.list_namespaced_persistent_volume_claim(ns).items
            # Build a set of PVC names actively referenced by running pods.
            referenced = set()
            pods = v1.list_namespaced_pod(ns).items
            for pod in pods:
                # Consider only pods that are Running or Pending (i.e. active workload)
                if pod.status.phase in ["Running", "Pending"]:
                    if pod.spec.volumes:
                        for vol in pod.spec.volumes:
                            if vol.persistent_volume_claim:
                                referenced.add(vol.persistent_volume_claim.claim_name)
            for pvc in pvc_list:
                if skip_due_to_label(pvc) is True:
                    continue
                # Mark as unused if PVC is not in Bound state OR if it is Bound but not referenced.
                if pvc.status.phase != "Bound" or pvc.metadata.name not in referenced:
                    ns_unused.append(f"{ns}/{pvc.metadata.name}")
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
    threshold = datetime.utcnow() - timedelta(hours=1)
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
                else:
                    creation_ts = pod.metadata.creation_timestamp
                    if creation_ts and (datetime.utcnow() - creation_ts.replace(tzinfo=None)) > timedelta(days=15):
                        ns_unused.append(f"{ns}/{pod.metadata.name}")
                    elif is_resource_expired(pod, "orphanTTL"):
                        ns_unused.append(f"{ns}/{pod.metadata.name}")
        except Exception as e:
            logging.error(f"Error in find_unused_pods for namespace {ns}: {e}")
        return ns_unused
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
                elif is_resource_expired(dep, "orphanTTL"):
                    ns_unused.append(f"{ns}/{dep.metadata.name}")
        except Exception as e:
            logging.error(f"Error in find_unused_deployments for namespace {ns}: {e}")
        return ns_unused
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
                if (sts.spec.replicas or 0) == 0 or is_resource_expired(sts, "orphanTTL"):
                    ns_unused.append(f"{ns}/{sts.metadata.name}")
        except Exception as e:
            logging.error(f"Error in find_unused_statefulsets for namespace {ns}: {e}")
        return ns_unused
    with ThreadPoolExecutor() as executor:
        futures = {executor.submit(process_namespace, ns): ns for ns in NAMESPACES}
        for future in as_completed(futures):
            unused.extend(future.result())
    return unused

def find_unused_daemonsets():
    unused = []
    def process_namespace(ns):
        ns_unused = []
        try:
            ds_list = apps_v1.list_namespaced_daemon_set(ns).items
            for ds in ds_list:
                if skip_due_to_label(ds) is True:
                    continue
                if (ds.status.current_number_scheduled or 0) == 0 or is_resource_expired(ds, "orphanTTL"):
                    ns_unused.append(f"{ns}/{ds.metadata.name}")
        except Exception as e:
            logging.error(f"Error in find_unused_daemonsets for namespace {ns}: {e}")
        return ns_unused
    with ThreadPoolExecutor() as executor:
        futures = {executor.submit(process_namespace, ns): ns for ns in NAMESPACES}
        for future in as_completed(futures):
            unused.extend(future.result())
    return unused

def find_unused_replicasets():
    unused = []
    def process_namespace(ns):
        ns_unused = []
        try:
            rs_list = apps_v1.list_namespaced_replica_set(ns).items
            for rs in rs_list:
                if skip_due_to_label(rs) is True:
                    continue
                if (rs.spec.replicas or 0) == 0 or is_resource_expired(rs, "orphanTTL"):
                    ns_unused.append(f"{ns}/{rs.metadata.name}")
        except Exception as e:
            logging.error(f"Error in find_unused_replicasets for namespace {ns}: {e}")
        return ns_unused
    with ThreadPoolExecutor() as executor:
        futures = {executor.submit(process_namespace, ns): ns for ns in NAMESPACES}
        for future in as_completed(futures):
            unused.extend(future.result())
    return unused

def find_unused_jobs():
    unused_jobs = []
    unused_cronjobs = []
    def process_namespace(ns):
        ns_unused_jobs = []
        ns_unused_cronjobs = []
        try:
            jobs = batch_v1.list_namespaced_job(ns).items
            for job in jobs:
                if skip_due_to_label(job) is True:
                    continue
                if job.status.succeeded or job.status.failed or is_resource_expired(job, "orphanTTL"):
                    ns_unused_jobs.append(f"{ns}/{job.metadata.name}")
        except Exception as e:
            logging.error(f"Error in find_unused_jobs for namespace {ns}: {e}")
        try:
            cronjobs = batch_v1.list_namespaced_cron_job(ns).items
            for cj in cronjobs:
                if skip_due_to_label(cj) is True:
                    continue
                if not cj.spec.suspend and not cj.status.lastScheduleTime:
                    ns_unused_cronjobs.append(f"{ns}/{cj.metadata.name}")
        except Exception as e:
            logging.error(f"Error in find_unused_cronjobs for namespace {ns}: {e}")
        return ns_unused_jobs, ns_unused_cronjobs
    with ThreadPoolExecutor() as executor:
        futures = {executor.submit(process_namespace, ns): ns for ns in NAMESPACES}
        for future in as_completed(futures):
            nj, ncj = future.result()
            unused_jobs.extend(nj)
            unused_cronjobs.extend(ncj)
    return unused_jobs, unused_cronjobs

def find_unused_ingresses():
    unused = []
    def process_namespace(ns):
        ns_unused = []
        try:
            ing_list = networking_v1.list_namespaced_ingress(ns).items
            for ing in ing_list:
                if skip_due_to_label(ing) is True:
                    continue
                backend_missing = False
                if ing.spec.default_backend:
                    svc_name = ing.spec.default_backend.service.name
                    try:
                        v1.read_namespaced_service(svc_name, ns)
                    except Exception:
                        backend_missing = True
                if ing.spec.rules:
                    for rule in ing.spec.rules:
                        if rule.http and rule.http.paths:
                            for path in rule.http.paths:
                                svc_name = path.backend.service.name
                                try:
                                    v1.read_namespaced_service(svc_name, ns)
                                except Exception:
                                    backend_missing = True
                if backend_missing or is_resource_expired(ing, "orphanTTL"):
                    ns_unused.append(f"{ns}/{ing.metadata.name}")
        except Exception as e:
            logging.error(f"Error in find_unused_ingresses for namespace {ns}: {e}")
        return ns_unused
    with ThreadPoolExecutor() as executor:
        futures = {executor.submit(process_namespace, ns): ns for ns in NAMESPACES}
        for future in as_completed(futures):
            unused.extend(future.result())
    return unused

def find_unused_storageclasses():
    try:
        storage_classes = [sc.metadata.name for sc in storage_v1.list_storage_class().items]
        all_pvcs = []
        for ns in NAMESPACES:
            try:
                pvcs = v1.list_namespaced_persistent_volume_claim(ns).items
                all_pvcs.extend([pvc for pvc in pvcs if pvc.spec.storage_class_name])
            except Exception as e:
                logging.error(f"Error listing PVCs in namespace {ns}: {e}")
        used_sc = {pvc.spec.storage_class_name for pvc in all_pvcs}
        unused = [sc for sc in storage_classes if sc not in used_sc]
        return unused
    except Exception as e:
        logging.error(f"Error in find_unused_storageclasses: {e}")
        return []

def find_unused_serviceaccounts():
    used_sas = set()
    def process_namespace(ns):
        ns_used = set()
        try:
            pods = v1.list_namespaced_pod(ns).items
            for pod in pods:
                sa = pod.spec.service_account_name or "default"
                ns_used.add(f"{ns}/{sa}")
        except Exception as e:
            logging.error(f"Error scanning pods in {ns} for service account usage: {e}")
        return ns_used
    with ThreadPoolExecutor() as executor:
        futures = {executor.submit(process_namespace, ns): ns for ns in NAMESPACES}
        for future in as_completed(futures):
            used_sas.update(future.result())
    unused = []
    for ns in NAMESPACES:
        try:
            sas = v1.list_namespaced_service_account(ns).items
            for sa in sas:
                key = f"{ns}/{sa.metadata.name}"
                if key not in used_sas:
                    unused.append(key)
        except Exception as e:
            logging.error(f"Error listing service accounts in {ns}: {e}")
    return unused

def find_unused_namespaces():
    unused = []
    system_ns = {"kube-system", "kube-public", "default", "kube-node-lease"}
    for ns in NAMESPACES:
        if ns in system_ns:
            continue
        try:
            pods = v1.list_namespaced_pod(ns).items
            if not pods:
                unused.append(ns)
        except Exception as e:
            logging.error(f"Error checking namespace {ns} for pods: {e}")
    return unused

def find_unused_crds():
    unused = []
    try:
        crds = apiextensions_v1.list_custom_resource_definition().items
        for crd in crds:
            group = crd.spec.group
            versions = [v.name for v in crd.spec.versions if v.served]
            if not versions:
                continue
            version = versions[0]
            try:
                resource = dynamic_client.resources.get(api_version=f"{group}/{version}", kind=crd.spec.names.kind)
                items = resource.get().items
                if not items:
                    unused.append(crd.metadata.name)
            except Exception as e:
          
