# part2.py
from part1 import (
    NAMESPACES, v1, apps_v1, batch_v1, rbac_v1, networking_v1,
    apiextensions_v1, storage_v1, dynamic_client,
    CLUSTER_CONTEXT, skip_due_to_label, is_orphaned,
    find_unused_pvcs, find_unused_pvs, find_unused_configmaps_and_secrets
)
from kubernetes import config  # Already loaded in part1
from concurrent.futures import ThreadPoolExecutor, as_completed
import logging
import pandas as pd

# ------------------------------------------------------------------------------
# UNUSED RESOURCE DETECTION FUNCTIONS
# ------------------------------------------------------------------------------

def find_unused_pods():
    """Detect pods that are not running (Succeeded/Failed)."""
    unused = []
    def process_namespace(ns):
        ns_unused = []
        try:
            pods = v1.list_namespaced_pod(ns).items
            for pod in pods:
                if skip_due_to_label(pod):
                    continue
                if pod.status.phase in ["Succeeded", "Failed"]:
                    ns_unused.append(f"{ns}/{pod.metadata.name}")
            return ns_unused
        except Exception as e:
            logging.error(f"Error processing pods in namespace {ns}: {e}")
            return []
    with ThreadPoolExecutor() as executor:
        futures = {executor.submit(process_namespace, ns): ns for ns in NAMESPACES}
        for future in as_completed(futures):
            unused.extend(future.result())
    return unused

def find_unused_services():
    """Detect services that have no active endpoints."""
    unused = []
    def process_namespace(ns):
        ns_unused = []
        try:
            services = v1.list_namespaced_service(ns).items
            for svc in services:
                if skip_due_to_label(svc):
                    continue
                if svc.spec.type == "ExternalName":
                    continue  # Skip ExternalName services.
                try:
                    endpoints = v1.read_namespaced_endpoints(svc.metadata.name, ns)
                    if not endpoints.subsets:
                        ns_unused.append(f"{ns}/{svc.metadata.name}")
                except Exception as e:
                    logging.error(f"Error reading endpoints for {ns}/{svc.metadata.name}: {e}")
                    ns_unused.append(f"{ns}/{svc.metadata.name}")
            return ns_unused
        except Exception as e:
            logging.error(f"Error processing services in namespace {ns}: {e}")
            return []
    with ThreadPoolExecutor() as executor:
        futures = {executor.submit(process_namespace, ns): ns for ns in NAMESPACES}
        for future in as_completed(futures):
            unused.extend(future.result())
    return unused

def find_unused_deployments():
    """Detect deployments with zero desired or available replicas or that are orphaned."""
    unused = []
    def process_namespace(ns):
        ns_unused = []
        try:
            deployments = apps_v1.list_namespaced_deployment(ns).items
            for dep in deployments:
                if skip_due_to_label(dep):
                    continue
                desired = dep.spec.replicas or 0
                available = dep.status.available_replicas or 0
                if desired == 0 or available == 0 or is_orphaned(dep):
                    ns_unused.append(f"{ns}/{dep.metadata.name}")
            return ns_unused
        except Exception as e:
            logging.error(f"Error processing deployments in namespace {ns}: {e}")
            return []
    with ThreadPoolExecutor() as executor:
        futures = {executor.submit(process_namespace, ns): ns for ns in NAMESPACES}
        for future in as_completed(futures):
            unused.extend(future.result())
    return unused

def find_unused_statefulsets():
    """Detect statefulsets with zero replicas."""
    unused = []
    def process_namespace(ns):
        ns_unused = []
        try:
            sts_list = apps_v1.list_namespaced_stateful_set(ns).items
            for sts in sts_list:
                if skip_due_to_label(sts):
                    continue
                if (sts.spec.replicas or 0) == 0:
                    ns_unused.append(f"{ns}/{sts.metadata.name}")
            return ns_unused
        except Exception as e:
            logging.error(f"Error processing statefulsets in namespace {ns}: {e}")
            return []
    with ThreadPoolExecutor() as executor:
        futures = {executor.submit(process_namespace, ns): ns for ns in NAMESPACES}
        for future in as_completed(futures):
            unused.extend(future.result())
    return unused

def find_unused_daemonsets():
    """Detect daemonsets with no scheduled pods."""
    unused = []
    def process_namespace(ns):
        ns_unused = []
        try:
            ds_list = apps_v1.list_namespaced_daemon_set(ns).items
            for ds in ds_list:
                if skip_due_to_label(ds):
                    continue
                if (ds.status.current_number_scheduled or 0) == 0:
                    ns_unused.append(f"{ns}/{ds.metadata.name}")
            return ns_unused
        except Exception as e:
            logging.error(f"Error processing daemonsets in namespace {ns}: {e}")
            return []
    with ThreadPoolExecutor() as executor:
        futures = {executor.submit(process_namespace, ns): ns for ns in NAMESPACES}
        for future in as_completed(futures):
            unused.extend(future.result())
    return unused

def find_unused_replicasets():
    """Detect replicasets with zero replicas."""
    unused = []
    def process_namespace(ns):
        ns_unused = []
        try:
            rs_list = apps_v1.list_namespaced_replica_set(ns).items
            for rs in rs_list:
                if skip_due_to_label(rs):
                    continue
                if (rs.spec.replicas or 0) == 0:
                    ns_unused.append(f"{ns}/{rs.metadata.name}")
            return ns_unused
        except Exception as e:
            logging.error(f"Error processing replicasets in namespace {ns}: {e}")
            return []
    with ThreadPoolExecutor() as executor:
        futures = {executor.submit(process_namespace, ns): ns for ns in NAMESPACES}
        for future in as_completed(futures):
            unused.extend(future.result())
    return unused

def find_unused_jobs_and_cronjobs():
    """Detect jobs that have completed (succeeded/failed) and cronjobs that have never run."""
    unused_jobs, unused_cronjobs = [], []
    def process_namespace(ns):
        ns_unused_jobs = []
        ns_unused_cronjobs = []
        try:
            jobs = batch_v1.list_namespaced_job(ns).items
            for job in jobs:
                if skip_due_to_label(job):
                    continue
                if job.status.succeeded or job.status.failed:
                    ns_unused_jobs.append(f"{ns}/{job.metadata.name}")
        except Exception as e:
            logging.error(f"Error processing jobs in namespace {ns}: {e}")
        try:
            cronjobs = batch_v1.list_namespaced_cron_job(ns).items
            for cj in cronjobs:
                if skip_due_to_label(cj):
                    continue
                if not cj.spec.suspend and not cj.status.lastScheduleTime:
                    ns_unused_cronjobs.append(f"{ns}/{cj.metadata.name}")
        except Exception as e:
            logging.error(f"Error processing cronjobs in namespace {ns}: {e}")
        return ns_unused_jobs, ns_unused_cronjobs

    with ThreadPoolExecutor() as executor:
        futures = {executor.submit(process_namespace, ns): ns for ns in NAMESPACES}
        for future in as_completed(futures):
            nj, ncj = future.result()
            unused_jobs.extend(nj)
            unused_cronjobs.extend(ncj)
    return unused_jobs, unused_cronjobs

def find_unused_ingresses():
    """Detect ingresses with missing backend services."""
    unused = []
    def process_namespace(ns):
        ns_unused = []
        try:
            ing_list = networking_v1.list_namespaced_ingress(ns).items
            for ing in ing_list:
                if skip_due_to_label(ing):
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
                if backend_missing:
                    ns_unused.append(f"{ns}/{ing.metadata.name}")
        except Exception as e:
            logging.error(f"Error processing ingresses in namespace {ns}: {e}")
        return ns_unused
    with ThreadPoolExecutor() as executor:
        futures = {executor.submit(process_namespace, ns): ns for ns in NAMESPACES}
        for future in as_completed(futures):
            unused.extend(future.result())
    return unused

def find_unused_storageclasses():
    """Detect storage classes that are not used by any PVC."""
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
        logging.error(f"Error processing storage classes: {e}")
        return []

def find_unused_serviceaccounts():
    """Detect service accounts not used by any pod."""
    used_sas = set()
    def process_namespace(ns):
        ns_used = set()
        try:
            pods = v1.list_namespaced_pod(ns).items
            for pod in pods:
                sa = pod.spec.service_account_name or "default"
                ns_used.add(f"{ns}/{sa}")
        except Exception as e:
            logging.error(f"Error processing pods for service accounts in namespace {ns}: {e}")
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
            logging.error(f"Error processing service accounts in namespace {ns}: {e}")
    return unused

def find_unused_namespaces():
    """Detect namespaces that are empty (excluding system namespaces)."""
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
            logging.error(f"Error processing namespace {ns}: {e}")
    return unused

def find_unused_crds():
    """Detect CRDs with no active custom objects."""
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
                logging.error(f"Error processing CRD {crd.metadata.name}: {e}")
        return unused
    except Exception as e:
        logging.error(f"Error listing CRDs: {e}")
        return []

def find_unused_rbac():
    """Detect unused RBAC Roles, RoleBindings, and ClusterRoles."""
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
            logging.error(f"Error processing RBAC in namespace {ns}: {e}")
    try:
        crs = rbac_v1.list_cluster_role().items
        unused_clusterroles = [cr.metadata.name for cr in crs]
    except Exception as e:
        logging.error(f"Error processing cluster roles: {e}")
    return unused_roles, unused_rolebindings, unused_clusterroles

# ------------------------------------------------------------------------------
# REPORTING & MAIN FUNCTION
# ------------------------------------------------------------------------------

def save_results_to_excel(unused_resources):
    """
    Save the detailed results to an Excel file.
    The filename and each sheet name include the cluster context.
    """
    filename = f"unused_k8s_resources_{CLUSTER_CONTEXT}.xlsx"
    try:
        with pd.ExcelWriter(filename, engine="xlsxwriter") as writer:
            for resource, items in unused_resources.items():
                if items:
                    sheet_name = f"{resource[:20]}_{CLUSTER_CONTEXT}"[:31]  # Excel sheet name limit applied.
                    df = pd.DataFrame(items, columns=[f"Unused {resource}"])
                    df.to_excel(writer, sheet_name=sheet_name, index=False)
                    workbook = writer.book
                    worksheet = writer.sheets[sheet_name]
                    cell_format = workbook.add_format({"bold": True, "bg_color": "#FFC7CE"})
                    worksheet.set_column("A:A", 40, cell_format)
        logging.info(f"Results saved to '{filename}'")
        print(f"Results saved to '{filename}'")
    except Exception as e:
        logging.error(f"Error saving Excel file: {e}")
        print(f"Error saving Excel file: {e}")

def scan_unused_resources():
    """
    Main scan function that gathers unused resources across all kinds.
    Prints the results for each resource type and saves a detailed Excel report.
    """
    logging.info("Starting scan for unused Kubernetes resources...")
    print(f"Cluster Context: {CLUSTER_CONTEXT}")

    unused_resources = {
        "PersistentVolumes": find_unused_pvs(),
        "PersistentVolumeClaims": find_unused_pvcs(),
        "ConfigMaps": find_unused_configmaps_and_secrets()[0],
        "Secrets": find_unused_configmaps_and_secrets()[1],
        "Pods": find_unused_pods(),
        "Services": find_unused_services(),
        "Deployments": find_unused_deployments(),
        "StatefulSets": find_unused_statefulsets(),
        "DaemonSets": find_unused_daemonsets(),
        "ReplicaSets": find_unused_replicasets(),
    }
    jobs, cronjobs = find_unused_jobs_and_cronjobs()
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

    # Print summary for each resource kind.
    print(f"\nCluster Context: {CLUSTER_CONTEXT}")
    for kind, items in unused_resources.items():
        print(f"\nUnused {kind} ({len(items)}):")
        for item in items:
            print(f"  - {item}")

    # Save the detailed report to an Excel file.
    save_results_to_excel(unused_resources)
    logging.info("Scan completed.")

if __name__ == "__main__":
    scan_unused_resources()
