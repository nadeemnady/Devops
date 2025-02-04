import logging
from datetime import datetime, timedelta
from kubernetes import client, config, dynamic
from kubernetes.client import ApiClient
import pandas as pd
import os

# ==============================================================================
# CONFIGURATION & INITIALIZATION
# ==============================================================================

# Configure logging
logging.basicConfig(
    filename="k8s_unused_resources.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)

# Load Kubernetes config (from ~/.kube/config or in-cluster config)
config.load_kube_config()

# Initialize API clients
v1 = client.CoreV1Api()
apps_v1 = client.AppsV1Api()
batch_v1 = client.BatchV1Api()
rbac_v1 = client.RbacAuthorizationV1Api()
networking_v1 = client.NetworkingV1Api()
apiextensions_v1 = client.ApiextensionsV1Api()
dynamic_client = dynamic.DynamicClient(ApiClient())

# ==============================================================================
# HELPER FUNCTIONS
# ==============================================================================

def get_namespaces():
    """
    Returns the list of namespaces to scan.
    If a file named "namespaces.txt" exists, uses its content.
    Otherwise, returns all namespaces.
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

# ==============================================================================
# UNUSED RESOURCE DETECTION FUNCTIONS (HEURISTICS)
# ==============================================================================

def find_unused_pvs():
    """Unused PVs are those in 'Available' phase (not bound)."""
    try:
        pvs = v1.list_persistent_volume().items
        unused = [pv.metadata.name for pv in pvs if pv.status.phase == "Available"]
        return unused
    except Exception as e:
        logging.error(f"Error in find_unused_pvs: {e}")
        return []

def find_unused_pvcs():
    """
    Unused PVCs: those not in the Bound phase.
    (Note: In some cases, Pending PVCs may be valid. Adjust as needed.)
    """
    unused = []
    for ns in NAMESPACES:
        try:
            pvcs = v1.list_namespaced_persistent_volume_claim(ns).items
            for pvc in pvcs:
                if pvc.status.phase != "Bound":
                    unused.append(f"{ns}/{pvc.metadata.name}")
        except Exception as e:
            logging.error(f"Error in find_unused_pvcs for namespace {ns}: {e}")
    return unused

def find_unused_configmaps_and_secrets():
    """
    Unused ConfigMaps and Secrets are those not referenced by any pod volumes.
    (Only scanning pods’ volumes – this may not catch every reference.)
    """
    used_configmaps, used_secrets = set(), set()
    for ns in NAMESPACES:
        try:
            pods = v1.list_namespaced_pod(ns).items
            for pod in pods:
                if pod.spec.volumes:
                    for volume in pod.spec.volumes:
                        if volume.config_map:
                            used_configmaps.add(volume.config_map.name)
                        if volume.secret:
                            used_secrets.add(volume.secret.secret_name)
        except Exception as e:
            logging.error(f"Error scanning pods for ConfigMap/Secret usage in {ns}: {e}")
    try:
        all_configmaps = {cm.metadata.name for cm in v1.list_config_map_for_all_namespaces().items}
        all_secrets = {sec.metadata.name for sec in v1.list_secret_for_all_namespaces().items}
    except Exception as e:
        logging.error(f"Error listing all ConfigMaps/Secrets: {e}")
        return [], []
    unused_configmaps = list(all_configmaps - used_configmaps)
    unused_secrets = list(all_secrets - used_secrets)
    return unused_configmaps, unused_secrets

def find_unused_pods():
    """
    Consider pods that have terminated (Succeeded or Failed) or have been
    inactive for more than an hour (adjust the threshold as needed) as unused.
    """
    unused = []
    threshold = datetime.utcnow() - timedelta(hours=1)
    for ns in NAMESPACES:
        try:
            pods = v1.list_namespaced_pod(ns).items
            for pod in pods:
                phase = pod.status.phase
                # Check if pod is completed or failed
                if phase in ["Succeeded", "Failed"]:
                    unused.append(f"{ns}/{pod.metadata.name}")
                else:
                    # Also check for stale pods (older than 1 hour)
                    creation_ts = pod.metadata.creation_timestamp
                    if creation_ts and creation_ts.replace(tzinfo=None) < threshold and phase != "Running":
                        unused.append(f"{ns}/{pod.metadata.name}")
        except Exception as e:
            logging.error(f"Error in find_unused_pods for namespace {ns}: {e}")
    return unused

def find_unused_services():
    """
    For Services (of type ClusterIP or NodePort), check if Endpoints are empty.
    (ExternalName services are skipped.)
    """
    unused = []
    for ns in NAMESPACES:
        try:
            services = v1.list_namespaced_service(ns).items
            for svc in services:
                svc_type = svc.spec.type
                if svc_type == "ExternalName":
                    continue  # skip external services
                try:
                    ep = v1.read_namespaced_endpoints(svc.metadata.name, ns)
                    subsets = ep.subsets
                    # If endpoints have no addresses, consider unused.
                    if not subsets:
                        unused.append(f"{ns}/{svc.metadata.name}")
                except Exception as e:
                    logging.error(f"Error reading endpoints for service {ns}/{svc.metadata.name}: {e}")
                    # If endpoints cannot be read, mark service as suspect.
                    unused.append(f"{ns}/{svc.metadata.name}")
        except Exception as e:
            logging.error(f"Error in find_unused_services for namespace {ns}: {e}")
    return unused

def find_unused_deployments():
    """Mark deployments with zero desired replicas or zero available replicas as unused."""
    unused = []
    for ns in NAMESPACES:
        try:
            deps = apps_v1.list_namespaced_deployment(ns).items
            for dep in deps:
                desired = dep.spec.replicas or 0
                available = dep.status.available_replicas or 0
                if desired == 0 or available == 0:
                    unused.append(f"{ns}/{dep.metadata.name}")
        except Exception as e:
            logging.error(f"Error in find_unused_deployments for namespace {ns}: {e}")
    return unused

def find_unused_statefulsets():
    """Mark statefulsets with zero replicas as unused."""
    unused = []
    for ns in NAMESPACES:
        try:
            sts_list = apps_v1.list_namespaced_stateful_set(ns).items
            for sts in sts_list:
                desired = sts.spec.replicas or 0
                if desired == 0:
                    unused.append(f"{ns}/{sts.metadata.name}")
        except Exception as e:
            logging.error(f"Error in find_unused_statefulsets for namespace {ns}: {e}")
    return unused

def find_unused_daemonsets():
    """Mark daemonsets with zero scheduled pods as unused."""
    unused = []
    for ns in NAMESPACES:
        try:
            ds_list = apps_v1.list_namespaced_daemon_set(ns).items
            for ds in ds_list:
                scheduled = ds.status.current_number_scheduled or 0
                if scheduled == 0:
                    unused.append(f"{ns}/{ds.metadata.name}")
        except Exception as e:
            logging.error(f"Error in find_unused_daemonsets for namespace {ns}: {e}")
    return unused

def find_unused_replicasets():
    """Mark replicasets with zero replicas as unused."""
    unused = []
    for ns in NAMESPACES:
        try:
            rs_list = apps_v1.list_namespaced_replica_set(ns).items
            for rs in rs_list:
                desired = rs.spec.replicas or 0
                if desired == 0:
                    unused.append(f"{ns}/{rs.metadata.name}")
        except Exception as e:
            logging.error(f"Error in find_unused_replicasets for namespace {ns}: {e}")
    return unused

def find_unused_jobs():
    """
    For Jobs, if they have succeeded or failed (completed), consider them unused.
    For CronJobs, check if they are not suspended and have no active jobs.
    """
    unused_jobs = []
    unused_cronjobs = []
    for ns in NAMESPACES:
        try:
            jobs = batch_v1.list_namespaced_job(ns).items
            for job in jobs:
                # if job is complete (either succeeded or failed)
                if job.status.succeeded or job.status.failed:
                    unused_jobs.append(f"{ns}/{job.metadata.name}")
        except Exception as e:
            logging.error(f"Error in find_unused_jobs for namespace {ns}: {e}")

        try:
            # Note: The BatchV1beta1Api is deprecated; adjust if needed.
            cronjobs = batch_v1.list_namespaced_cron_job(ns).items
            for cj in cronjobs:
                if not cj.spec.suspend and not cj.status.lastScheduleTime:
                    unused_cronjobs.append(f"{ns}/{cj.metadata.name}")
        except Exception as e:
            logging.error(f"Error in find_unused_cronjobs for namespace {ns}: {e}")
    return unused_jobs, unused_cronjobs

def find_unused_ingresses():
    """
    For Ingresses, check if their referenced services exist and have endpoints.
    This is best-effort since an Ingress may be configured even with no current traffic.
    """
    unused = []
    for ns in NAMESPACES:
        try:
            ing_list = networking_v1.list_namespaced_ingress(ns).items
            for ing in ing_list:
                # Check default backend if present
                backend_missing = False
                if ing.spec.default_backend:
                    svc_name = ing.spec.default_backend.service.name
                    try:
                        svc = v1.read_namespaced_service(svc_name, ns)
                    except Exception:
                        backend_missing = True
                # Check each rule's backend
                if ing.spec.rules:
                    for rule in ing.spec.rules:
                        if rule.http and rule.http.paths:
                            for path in rule.http.paths:
                                svc_name = path.backend.service.name
                                try:
                                    svc = v1.read_namespaced_service(svc_name, ns)
                                except Exception:
                                    backend_missing = True
                if backend_missing:
                    unused.append(f"{ns}/{ing.metadata.name}")
        except Exception as e:
            logging.error(f"Error in find_unused_ingresses for namespace {ns}: {e}")
    return unused

def find_unused_storageclasses():
    """
    A StorageClass is considered unused if no PVCs reference it.
    """
    try:
        storage_classes = [sc.metadata.name for sc in client.StorageV1Api().list_storage_class().items]
        all_pvcs = []
        for ns in NAMESPACES:
            try:
                pvcs = v1.list_namespaced_persistent_volume_claim(ns).items
                all_pvcs.extend(pvc for pvc in pvcs if pvc.spec.storage_class_name)
            except Exception as e:
                logging.error(f"Error listing PVCs in namespace {ns}: {e}")
        used_sc = {pvc.spec.storage_class_name for pvc in all_pvcs}
        unused = [sc for sc in storage_classes if sc not in used_sc]
        return unused
    except Exception as e:
        logging.error(f"Error in find_unused_storageclasses: {e}")
        return []

def find_unused_serviceaccounts():
    """
    A ServiceAccount is considered unused if it is not referenced by any pod.
    (This is a heuristic – some SA’s may be used indirectly.)
    """
    used_sas = set()
    for ns in NAMESPACES:
        try:
            pods = v1.list_namespaced_pod(ns).items
            for pod in pods:
                sa = pod.spec.service_account_name or "default"
                used_sas.add(f"{ns}/{sa}")
        except Exception as e:
            logging.error(f"Error scanning pods for service account usage in {ns}: {e}")
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
    """
    Heuristically flag non-system namespaces that have no pods.
    (System namespaces like kube-system, kube-public, and default are skipped.)
    """
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
    """
    For each CustomResourceDefinition, use the dynamic client to check if any
    custom objects exist. If none exist, consider the CRD unused.
    """
    unused = []
    try:
        crds = apiextensions_v1.list_custom_resource_definition().items
        for crd in crds:
            group = crd.spec.group
            # Use first version served; adjust if necessary.
            versions = [v.name for v in crd.spec.versions if v.served]
            if not versions:
                continue
            version = versions[0]
            plural = crd.spec.names.plural
            try:
                resource = dynamic_client.resources.get(api_version=f"{group}/{version}", kind=crd.spec.names.kind)
                items = resource.get().items
                if not items:
                    unused.append(crd.metadata.name)
            except Exception as e:
                logging.error(f"Error fetching custom objects for CRD {crd.metadata.name}: {e}")
                # If error, skip CRD
        return unused
    except Exception as e:
        logging.error(f"Error in find_unused_crds: {e}")
        return []

def find_unused_rbac():
    """
    For RBAC, this function simply lists Roles, RoleBindings, and ClusterRoles.
    Determining “unused” RBAC objects is non-trivial and may require policy analysis.
    Here we return all for reporting purposes.
    """
    unused_roles = []
    unused_rolebindings = []
    unused_clusterroles = []
    for ns in NAMESPACES:
        try:
            roles = rbac_v1.list_namespaced_role(ns).items
            unused_roles.extend([f"{ns}/{r.metadata.name}" for r in roles])
            rbs = rbac_v1.list_namespaced_role_binding(ns).items
            unused_rolebindings.extend([f"{ns}/{rb.metadata.name}" for rb in rbs])
        except Exception as e:
            logging.error(f"Error in find_unused_rbac for namespace {ns}: {e}")
    try:
        crs = rbac_v1.list_cluster_role().items
        unused_clusterroles = [cr.metadata.name for cr in crs]
    except Exception as e:
        logging.error(f"Error listing cluster roles: {e}")
    return unused_roles, unused_rolebindings, unused_clusterroles

# ==============================================================================
# REPORTING FUNCTION
# ==============================================================================

def save_results_to_excel(unused_resources):
    filename = "unused_k8s_resources.xlsx"
    try:
        with pd.ExcelWriter(filename, engine="xlsxwriter") as writer:
            for resource, items in unused_resources.items():
                if items:
                    df = pd.DataFrame(items, columns=[f"Unused {resource}"])
                    df.to_excel(writer, sheet_name=resource[:31], index=False)
                    # Simple formatting
                    workbook = writer.book
                    worksheet = writer.sheets[resource[:31]]
                    cell_format = workbook.add_format({"bold": True, "bg_color": "#FFC7CE"})
                    worksheet.set_column("A:A", 40, cell_format)
        logging.info(f"Results saved to '{filename}'")
    except Exception as e:
        logging.error(f"Error saving Excel file: {e}")

# ==============================================================================
# MAIN SCAN FUNCTION
# ==============================================================================

def scan_unused_resources():
    logging.info("Starting scan for unused Kubernetes resources...")

    # Build a dictionary with unused resources per kind.
    unused_resources = {
        "PersistentVolumes": find_unused_pvs(),
        "PersistentVolumeClaims": find_unused_pvcs(),
    }
    
    cm_unused, sec_unused = find_unused_configmaps_and_secrets()
    unused_resources["ConfigMaps"] = cm_unused
    unused_resources["Secrets"] = sec_unused

    unused_resources["Pods"] = find_unused_pods()
    unused_resources["Services"] = find_unused_services()
    unused_resources["Deployments"] = find_unused_deployments()
    unused_resources["StatefulSets"] = find_unused_statefulsets()
    unused_resources["DaemonSets"] = find_unused_daemonsets()
    unused_resources["ReplicaSets"] = find_unused_replicasets()

    jobs, cronjobs = find_unused_jobs()
    unused_resources["Jobs"] = jobs
    unused_resources["CronJobs"] = cronjobs

    unused_resources["Ingresses"] = find_unused_ingresses()
    unused_resources["StorageClasses"] = find_unused_storageclasses()
    unused_resources["ServiceAccounts"] = find_unused_serviceaccounts()
    unused_resources["Namespaces"] = find_unused_namespaces()
    unused_resources["CustomResourceDefinitions"] = find_unused_crds()

    rbac_roles, rbac_rolebindings, rbac_clusterroles = find_unused_rbac()
    unused_resources["Roles"] = rbac_roles
    unused_resources["RoleBindings"] = rbac_rolebindings
    unused_resources["ClusterRoles"] = rbac_clusterroles

    # (Optionally add ClusterRoleBindings here if needed)
    # unused_resources["ClusterRoleBindings"] = find_unused_clusterrolebindings()

    save_results_to_excel(unused_resources)
    logging.info("Scan completed.")

# ==============================================================================
# MAIN ENTRY POINT
# ==============================================================================

if __name__ == "__main__":
    scan_unused_resources()
