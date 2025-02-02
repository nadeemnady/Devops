import openpyxl
import json
import os
import logging
import yaml
import threading
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor
from kubernetes import client, config, dynamic
from kubernetes.client.rest import ApiException
from kubernetes.dynamic.exceptions import ResourceNotFoundError

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Load kubeconfig
config.load_kube_config()

# Initialize Kubernetes API clients
v1 = client.CoreV1Api()
apps_v1 = client.AppsV1Api()
batch_v1 = client.BatchV1Api()
networking_v1 = client.NetworkingV1Api()
rbac_v1 = client.RbacAuthorizationV1Api()
storage_v1 = client.StorageV1Api()
autoscaling_v1 = client.AutoscalingV1Api()
policy_v1 = client.PolicyV1Api()
apiextensions_v1 = client.ApiextensionsV1Api()
dynamic_client = dynamic.DynamicClient(client.ApiClient())

# Configuration
MAX_AGE_DAYS = 30  # Resources older than this are considered candidates for cleanup
EXCLUSION_FILE = "exclusions.yaml"
HISTORY_FILE = "unused_resources_history.json"

unused_resources = []
lock = threading.Lock()

class ResourceChecker:
    def __init__(self):
        self.exclusions = self.load_exclusions()
        self.api_resources = self.discover_api_resources()
        
    def load_exclusions(self):
        """Load exclusions from YAML file."""
        if os.path.exists(EXCLUSION_FILE):
            with open(EXCLUSION_FILE) as f:
                return yaml.safe_load(f) or {}
        return {"namespaces": [], "resources": []}

    def discover_api_resources(self):
        """Discover all API resources in the cluster."""
        resources = {}
        try:
            api_client = client.ApiClient()
            groups = client.ApisApi(api_client).get_api_versions().groups
            for group in groups:
                try:
                    versions = [group.preferred_version.version] if group.preferred_version else [v.version for v in group.versions]
                    for version in versions:
                        api_resources = client.ApisApi(api_client).get_api_resources(group.name, version).resources
                        for resource in api_resources:
                            key = f"{group.name}/{version}/{resource.kind}"
                            resources[key] = {
                                "group_version": f"{group.name}/{version}",
                                "kind": resource.kind,
                                "namespaced": resource.namespaced
                            }
                except ApiException as e:
                    logger.warning(f"Error discovering group {group.name}: {e}")
        except Exception as e:
            logger.error(f"Error discovering API resources: {e}")
        return resources

    def is_excluded(self, resource_type, name, namespace):
        """Check if a resource is excluded."""
        if namespace in self.exclusions.get("namespaces", []):
            return True
        return any(
            pattern.get("type") == resource_type and 
            pattern.get("name") == name and 
            (pattern.get("namespace") in [namespace, "*"])
            for pattern in self.exclusions.get("resources", [])
        )

    def check_owner_exists(self, resource):
        """Check if a resource has an owner reference."""
        if not resource.metadata.owner_references:
            return False
        for ref in resource.metadata.owner_references:
            try:
                api = dynamic_client.resources.get(api_version=ref.api_version, kind=ref.kind)
                api.get(name=ref.name, namespace=resource.metadata.namespace)
                return True
            except (ApiException, ResourceNotFoundError) as e:
                if e.status != 404:
                    logger.error(f"Error checking owner {ref.kind}/{ref.name}: {e}")
        return False

    def is_resource_old(self, resource):
        """Check if a resource is older than MAX_AGE_DAYS."""
        try:
            return (datetime.now(resource.metadata.creation_timestamp.tzinfo) - 
                    resource.metadata.creation_timestamp).days > MAX_AGE_DAYS
        except Exception as e:
            logger.error(f"Age check failed: {e}")
            return False

    def check_references(self, resource_type, resource_name, namespace):
        """Check if a resource is referenced by other resources."""
        try:
            handler = getattr(self, f"check_{resource_type.lower()}", None)
            if handler:
                return handler(resource_name, namespace)
            
            # Default check for unknown resources
            return self.check_generic_references(resource_type, resource_name, namespace)
            
        except ApiException as e:
            logger.error(f"API error checking {resource_type}: {e}")
            return True

    def check_generic_references(self, resource_type, name, namespace):
        """Fallback check for resource references using owner references."""
        try:
            resource = dynamic_client.resources.get(api_version=self._get_api_version(resource_type), kind=resource_type)
            obj = resource.get(name=name, namespace=namespace)
            return self.check_owner_exists(obj)
        except Exception as e:
            logger.warning(f"Generic check failed for {resource_type}/{name}: {e}")
            return True

    def _get_api_version(self, kind):
        """Map kind to preferred API version."""
        for key, res in self.api_resources.items():
            if res["kind"] == kind:
                return res["group_version"]
        return "v1"

    # --- Resource-specific checks ---
    def check_configmap(self, name, namespace):
        return self._check_volume_references("ConfigMap", name, namespace)

    def check_secret(self, name, namespace):
        return self._check_volume_references("Secret", name, namespace)

    def check_service(self, name, namespace):
        # Check endpoints
        endpoints = v1.read_namespaced_endpoints(name, namespace)
        if endpoints.subsets:
            return True
        
        # Check ingress references
        for ingress in self.paginated_list(networking_v1.list_namespaced_ingress, namespace):
            for rule in ingress.spec.rules or []:
                for path in rule.http.paths or []:
                    if path.backend.service and path.backend.service.name == name:
                        return True
        return False

    def check_persistentvolumeclaim(self, name, namespace):
        pvc = v1.read_namespaced_persistent_volume_claim(name, namespace)
        return pvc.status.phase == "Bound"

    def check_serviceaccount(self, name, namespace):
        # Check pods using the service account
        for pod in self.paginated_list(v1.list_namespaced_pod, namespace):
            if pod.spec.service_account_name == name:
                return True
        return False

    def check_networkpolicy(self, name, namespace):
        # NetworkPolicies are standalone; check if any pods are selected
        np = networking_v1.read_namespaced_network_policy(name, namespace)
        pod_selector = np.spec.pod_selector.match_labels or {}
        pods = v1.list_namespaced_pod(namespace, label_selector=",".join([f"{k}={v}" for k, v in pod_selector.items()]))
        return len(pods.items) > 0

    def check_horizontalpodautoscaler(self, name, namespace):
        hpa = autoscaling_v1.read_namespaced_horizontal_pod_autoscaler(name, namespace)
        return hpa.status.current_replicas > 0

    def _check_volume_references(self, resource_type, name, namespace):
        """Shared check for ConfigMap/Secret volume references."""
        for pod in self.paginated_list(v1.list_namespaced_pod, namespace):
            for vol in pod.spec.volumes or []:
                if vol.config_map and vol.config_map.name == name:
                    return True
                if vol.secret and vol.secret.secret_name == name:
                    return True
            for container in pod.spec.containers + pod.spec.init_containers:
                for env in container.env_from or []:
                    if env.config_map_ref and env.config_map_ref.name == name:
                        return True
                    if env.secret_ref and env.secret_ref.name == name:
                        return True
        return False

    def paginated_list(self, api_func, namespace=None, **kwargs):
        """Handle pagination for list operations."""
        _continue = ""
        while True:
            try:
                resp = api_func(namespace, **_clean_kwargs(kwargs, _continue=_continue)) if namespace else api_func(_continue=_continue, **kwargs)
                for item in resp.items:
                    yield item
                _continue = resp.metadata._continue
                if not _continue:
                    break
            except ApiException as e:
                logger.error(f"List error: {e}")
                break

def _clean_kwargs(kwargs, **overrides):
    return {k: v for k, v in {**kwargs, **overrides}.items() if v is not None}

def analyze_namespace(namespace, checker):
    local_unused = []
    logger.info(f"Scanning {namespace}")
    
    for resource_key in checker.api_resources:
        resource = checker.api_resources[resource_key]
        try:
            if resource["namespaced"] and namespace == "Cluster-wide":
                continue
            if not resource["namespaced"] and namespace != "Cluster-wide":
                continue
                
            api = dynamic_client.resources.get(api_version=resource["group_version"], kind=resource["kind"])
            items = api.get(namespace=namespace if resource["namespaced"] else None).items
            
            for item in items:
                if checker.is_excluded(resource["kind"], item.metadata.name, namespace):
                    continue
                if not checker.check_references(resource["kind"], item.metadata.name, namespace):
                    if checker.is_resource_old(item) and not checker.check_owner_exists(item):
                        entry = (
                            resource["kind"],
                            item.metadata.name,
                            namespace if resource["namespaced"] else "Cluster-wide",
                            item.metadata.creation_timestamp.strftime("%Y-%m-%d")
                        )
                        local_unused.append(entry)
                        
        except Exception as e:
            logger.error(f"Error processing {resource['kind']} in {namespace}: {e}")
    
    with lock:
        unused_resources.extend(local_unused)

def generate_report():
    """Generate an Excel report of unused resources."""
    wb = openpyxl.Workbook()
    ws_summary = wb.active
    ws_summary.title = "Summary"
    headers = ["Resource Type", "Resource Name", "Namespace", "Creation Date", "Age (days)"]
    ws_summary.append(headers)

    # Group resources by type
    resources = {}
    for res in unused_resources:
        resource_type = res[0]
        if resource_type not in resources:
            resources[resource_type] = []
        resources[resource_type].append(res)

    # Create sheets
    for resource_type, items in resources.items():
        ws = wb.create_sheet(title=resource_type)
        ws.append(headers)
        for item in items:
            creation_date = datetime.strptime(item[3], "%Y-%m-%d")
            age = (datetime.now() - creation_date).days
            ws.append([*item[:4], age])

    # Save report
    filename = f"k8s-unused-report-{datetime.now().strftime('%Y%m%d-%H%M')}.xlsx"
    wb.save(filename)
    logger.info(f"Report generated: {filename}")

def main():
    checker = ResourceChecker()
    namespaces = [ns.metadata.name for ns in checker.paginated_list(v1.list_namespace)]
    namespaces.append("Cluster-wide")  # For cluster-scoped resources

    with ThreadPoolExecutor(max_workers=3) as executor:
        futures = [executor.submit(analyze_namespace, ns, checker) for ns in namespaces]
        for future in futures:
            try:
                future.result()
            except Exception as e:
                logger.error(f"Error processing namespace: {e}")

    generate_report()
    logger.info("Scan completed successfully")

if __name__ == "__main__":
    main()
    