import logging
from kubernetes import client, config, dynamic
from kubernetes.client import ApiClient
import pandas as pd
import os

# Configure logging
logging.basicConfig(
    filename="k8s_unused_resources.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)

# Load Kubernetes config
config.load_kube_config()

# Initialize API clients
v1 = client.CoreV1Api()
batch_v1 = client.BatchV1Api()
rbac_v1 = client.RbacAuthorizationV1Api()
networking_v1 = client.NetworkingV1Api()
dynamic_client = dynamic.DynamicClient(client.ApiClient())

# Read namespaces from a file
def get_namespaces():
    if os.path.exists("namespaces.txt"):
        with open("namespaces.txt", "r") as f:
            namespaces = [line.strip() for line in f.readlines() if line.strip()]
        logging.info(f"Scanning namespaces: {namespaces}")
        return namespaces
    logging.warning("No namespaces.txt found. Scanning all namespaces.")
    return None  # Scan all namespaces if no file

NAMESPACES = get_namespaces()

# Get available resource kinds with API versions
def get_api_resources():
    api_resources = {}
    for resource in dynamic_client.resources.list():
        api_resources[resource.kind] = (resource.group_version, resource)
    return api_resources

API_RESOURCES = get_api_resources()

# Find unused Persistent Volumes
def find_unused_pvs():
    return [pv.metadata.name for pv in v1.list_persistent_volume().items if pv.status.phase == "Available"]

# Find unused ConfigMaps & Secrets
def find_unused_configmaps_and_secrets():
    used_configmaps, used_secrets = set(), set()
    for ns in NAMESPACES or [ns.metadata.name for ns in v1.list_namespace().items]:
        pods = v1.list_namespaced_pod(ns).items
        for pod in pods:
            if pod.spec.volumes:
                for volume in pod.spec.volumes:
                    if volume.config_map:
                        used_configmaps.add(volume.config_map.name)
                    if volume.secret:
                        used_secrets.add(volume.secret.secret_name)
    
    all_configmaps = {cm.metadata.name for cm in v1.list_config_map_for_all_namespaces().items}
    all_secrets = {sec.metadata.name for sec in v1.list_secret_for_all_namespaces().items}
    return list(all_configmaps - used_configmaps), list(all_secrets - used_secrets)

# Find unused Jobs & CronJobs
def find_unused_jobs():
    unused_jobs, unused_cronjobs = [], []
    for ns in NAMESPACES or [ns.metadata.name for ns in v1.list_namespace().items]:
        unused_jobs.extend([j.metadata.name for j in batch_v1.list_namespaced_job(ns).items if j.status.succeeded or j.status.failed])
        unused_cronjobs.extend([cj.metadata.name for cj in batch_v1.list_namespaced_cron_job(ns).items if not cj.spec.suspend])
    return unused_jobs, unused_cronjobs

# Find unused RBAC resources (Roles, RoleBindings, ClusterRoles)
def find_unused_rbac():
    unused_roles, unused_rolebindings = [], []
    for ns in NAMESPACES or [ns.metadata.name for ns in v1.list_namespace().items]:
        unused_roles.extend([r.metadata.name for r in rbac_v1.list_namespaced_role(ns).items])
        unused_rolebindings.extend([rb.metadata.name for rb in rbac_v1.list_namespaced_role_binding(ns).items])
    unused_clusterroles = [cr.metadata.name for cr in rbac_v1.list_cluster_role().items]
    return unused_roles, unused_rolebindings, unused_clusterroles

# Find unused resources dynamically
def find_unused_resources(kind):
    if kind not in API_RESOURCES:
        logging.warning(f"Skipping unknown kind: {kind}")
        return []

    group_version, resource = API_RESOURCES[kind]
    try:
        items = resource.get().items
        return [item.metadata.name for item in items]
    except Exception as e:
        logging.error(f"Error retrieving {kind}: {e}")
        return []

# Save results to an Excel file
def save_results_to_excel(unused_resources):
    filename = "unused_k8s_resources.xlsx"
    with pd.ExcelWriter(filename, engine="xlsxwriter") as writer:
        for resource, items in unused_resources.items():
            if items:
                df = pd.DataFrame(items, columns=["Unused " + resource])
                df.to_excel(writer, sheet_name=resource, index=False)
                
                # Apply formatting
                workbook = writer.book
                worksheet = writer.sheets[resource]
                format_highlight = workbook.add_format({"bold": True, "bg_color": "#FFC7CE"})
                worksheet.set_column("A:A", 30, format_highlight)
                
    logging.info(f"Results saved to '{filename}'")

# Scan for unused resources
def scan_unused_resources():
    logging.info("Scanning Kubernetes cluster for unused resources...")

    unused_resources = {
        "PersistentVolumes": find_unused_pvs(),
        "ConfigMaps": find_unused_configmaps_and_secrets()[0],
        "Secrets": find_unused_configmaps_and_secrets()[1],
        "Jobs": find_unused_jobs()[0],
        "CronJobs": find_unused_jobs()[1],
        "Roles": find_unused_rbac()[0],
        "RoleBindings": find_unused_rbac()[1],
        "ClusterRoles": find_unused_rbac()[2],
    }

    # Scan dynamically for other resource kinds
    additional_kinds = [
        "Pod", "Service", "PersistentVolumeClaim", "Namespace", "Node", "Event", "ServiceAccount",
        "LimitRange", "ResourceQuota", "Deployment", "StatefulSet", "DaemonSet", "ReplicaSet",
        "Ingress", "IngressClass", "NetworkPolicy", "StorageClass", "VolumeAttachment",
        "Role", "RoleBinding", "ClusterRole", "ClusterRoleBinding", "CustomResourceDefinition"
    ]

    for kind in additional_kinds:
        unused_resources[kind] = find_unused_resources(kind)

    save_results_to_excel(unused_resources)

# Run the script
if __name__ == "__main__":
    scan_unused_resources()
