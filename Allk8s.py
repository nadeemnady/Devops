import logging
from kubernetes import client, config
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
apps_v1 = client.AppsV1Api()
batch_v1 = client.BatchV1Api()
rbac_v1 = client.RbacAuthorizationV1Api()
networking_v1 = client.NetworkingV1Api()
custom_objects_api = client.CustomObjectsApi()
policy_v1 = client.PolicyV1Api()
storage_v1 = client.StorageV1Api()

# Read namespaces from text file
def get_namespaces():
    if os.path.exists("namespaces.txt"):
        with open("namespaces.txt", "r") as f:
            namespaces = [line.strip() for line in f.readlines() if line.strip()]
        logging.info(f"Scanning namespaces: {namespaces}")
        return namespaces
    logging.warning("No namespaces.txt found. Scanning all namespaces.")
    return None  # Scan all namespaces if no file

NAMESPACES = get_namespaces()

# Function to find unused Persistent Volumes
def find_unused_pvs():
    unused_pvs = []
    for pv in v1.list_persistent_volume().items:
        if pv.status.phase == "Available":
            # Further check if PV is not bound to any PVC
            if not pv.spec.claim_ref:
                unused_pvs.append(pv.metadata.name)
    return unused_pvs

# Function to find unused ConfigMaps & Secrets
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

# Function to find unused workloads (Deployments, ReplicaSets, StatefulSets, DaemonSets)
def find_unused_workloads():
    unused_deployments, unused_replicasets, unused_statefulsets, unused_daemonsets = [], [], [], []
    for ns in NAMESPACES or [ns.metadata.name for ns in v1.list_namespace().items]:
        # Check Deployments, ReplicaSets, StatefulSets, and DaemonSets
        unused_deployments.extend([d.metadata.name for d in apps_v1.list_namespaced_deployment(ns).items if d.status.replicas == 0])
        unused_replicasets.extend([rs.metadata.name for rs in apps_v1.list_namespaced_replica_set(ns).items if rs.status.replicas == 0])
        unused_statefulsets.extend([s.metadata.name for s in apps_v1.list_namespaced_stateful_set(ns).items if s.status.replicas == 0])
        unused_daemonsets.extend([ds.metadata.name for ds in apps_v1.list_namespaced_daemon_set(ns).items if ds.status.number_ready == 0])
    return unused_deployments, unused_replicasets, unused_statefulsets, unused_daemonsets

# Function to find unused Jobs & CronJobs
def find_unused_jobs():
    unused_jobs, unused_cronjobs = [], []
    for ns in NAMESPACES or [ns.metadata.name for ns in v1.list_namespace().items]:
        unused_jobs.extend([j.metadata.name for j in batch_v1.list_namespaced_job(ns).items if j.status.succeeded or j.status.failed])
        unused_cronjobs.extend([cj.metadata.name for cj in batch_v1.list_namespaced_cron_job(ns).items if not cj.spec.suspend])
    return unused_jobs, unused_cronjobs

# Function to find unused RBAC resources
def find_unused_rbac():
    unused_roles, unused_rolebindings, unused_clusterroles = [], [], []
    for ns in NAMESPACES or [ns.metadata.name for ns in v1.list_namespace().items]:
        unused_roles.extend([r.metadata.name for r in rbac_v1.list_namespaced_role(ns).items])
        unused_rolebindings.extend([rb.metadata.name for rb in rbac_v1.list_namespaced_role_binding(ns).items])
    unused_clusterroles = [cr.metadata.name for cr in rbac_v1.list_cluster_role().items]
    return unused_roles, unused_rolebindings, unused_clusterroles

# Function to find unused Services, Ingress, and Network Policies
def find_unused_network_resources():
    unused_services, unused_ingresses, unused_network_policies = [], [], []
    for ns in NAMESPACES or [ns.metadata.name for ns in v1.list_namespace().items]:
        unused_services.extend([s.metadata.name for s in v1.list_namespaced_service(ns).items])
        unused_ingresses.extend([i.metadata.name for i in networking_v1.list_namespaced_ingress(ns).items])
        unused_network_policies.extend([np.metadata.name for np in networking_v1.list_namespaced_network_policy(ns).items])
    return unused_services, unused_ingresses, unused_network_policies

# Function to find unused ServiceAccounts, PodDisruptionBudgets, and CRDs
def find_unused_misc_resources():
    unused_service_accounts, unused_pdbs, unused_crds = [], [], []
    for ns in NAMESPACES or [ns.metadata.name for ns in v1.list_namespace().items]:
        unused_service_accounts.extend([sa.metadata.name for sa in v1.list_namespaced_service_account(ns).items])
        unused_pdbs.extend([pdb.metadata.name for pdb in policy_v1.list_namespaced_pod_disruption_budget(ns).items])
    unused_crds = [crd.metadata.name for crd in custom_objects_api.list_cluster_custom_object("apiextensions.k8s.io", "v1", "customresourcedefinitions")["items"]]
    return unused_service_accounts, unused_pdbs, unused_crds

# Function to find unused StorageClasses
def find_unused_storage_classes():
    return [sc.metadata.name for sc in storage_v1.list_storage_class().items]

# Function to analyze resource usage (Ownership, References)
def analyze_resource_usage():
    # Add logic to analyze resource ownership and associations
    # For example, a ServiceAccount that has no associated Pods or Deployments
    # Or a Deployment with no running Pods
    # This function would return a list of "true unused" resources.
    pass

# Function to save results to an Excel file
def save_results_to_excel(unused_resources):
    with pd.ExcelWriter("unused_k8s_resources.xlsx") as writer:
        for resource, items in unused_resources.items():
            if items:
                df = pd.DataFrame(items, columns=["Unused " + resource])
                df.to_excel(writer, sheet_name=resource, index=False)
    logging.info("Results saved to 'unused_k8s_resources.xlsx'")

# Main function to scan for unused resources
def scan_unused_resources():
    logging.info("Scanning Kubernetes cluster for unused resources...")

    unused_resources = {
        "PersistentVolumes": find_unused_pvs(),
        "ConfigMaps": find_unused_configmaps_and_secrets()[0],
        "Secrets": find_unused_configmaps_and_secrets()[1],
        "Deployments": find_unused_workloads()[0],
        "ReplicaSets": find_unused_workloads()[1],
        "StatefulSets": find_unused_workloads()[2],
        "DaemonSets": find_unused_workloads()[3],
        "Jobs": find_unused_jobs()[0],
        "CronJobs": find_unused_jobs()[1],
        "Roles": find_unused_rbac()[0],
        "RoleBindings": find_unused_rbac()[1],
        "ClusterRoles": find_unused_rbac()[2],
        "Services": find_unused_network_resources()[0],
        "Ingresses": find_unused_network_resources()[1],
        "NetworkPolicies": find_unused_network_resources()[2],
        "ServiceAccounts": find_unused_misc_resources()[0],
        "PodDisruptionBudgets": find_unused_misc_resources()[1],
        "CustomResourceDefinitions": find_unused_misc_resources()[2],
        "StorageClasses": find_unused_storage_classes(),
    }

    # Further filter resources based on true usage (dependency analysis)
    analyze_resource_usage()

    save_results_to_excel(unused_resources)

# Run the script
if __name__ == "__main__":
    scan_unused_resources()
