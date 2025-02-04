from k8s_unused_resources_part1 import (
    NAMESPACES, v1, apps_v1, batch_v1, rbac_v1, networking_v1,
    apiextensions_v1, storage_v1, dynamic_client,
    skip_due_to_label, is_resource_expired, is_orphaned,
    find_unused_pvs, find_unused_pvcs, find_unused_configmaps_and_secrets,
    find_unused_pods, find_unused_services, find_unused_deployments,
    find_unused_statefulsets
)
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
import logging
import pandas as pd

# ==============================================================================
# ADVANCED UNUSED RESOURCE DETECTION FUNCTIONS (PART 2)
# ==============================================================================

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
                logging.error(f"Error fetching custom objects for CRD {crd.metadata.name}: {e}")
        return unused
    except Exception as e:
        logging.error(f"Error in find_unused_crds: {e}")
        return []

def find_unused_rbac():
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
# REPORTING FUNCTION & MAIN SCAN
# ==============================================================================

def save_results_to_excel(unused_resources):
    filename = "unused_k8s_resources.xlsx"
    try:
        with pd.ExcelWriter(filename, engine="xlsxwriter") as writer:
            for resource, items in unused_resources.items():
                if items:
                    sheet_name = resource[:31]
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
    logging.info("Starting scan for unused Kubernetes resources...")
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

    # Print unused resources for each kind.
    for kind, items in unused_resources.items():
        print(f"\nUnused {kind} ({len(items)}):")
        for item in items:
            print(f"  - {item}")

    save_results_to_excel(unused_resources)
    logging.info("Scan completed.")

if __name__ == "__main__":
    scan_unused_resources()
          
