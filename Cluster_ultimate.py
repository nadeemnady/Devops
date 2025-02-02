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

# Initialize Kubernetes clients
core_v1 = client.CoreV1Api()
dynamic_client = dynamic.DynamicClient(client.ApiClient())

# Configuration
CONFIG = {
    'MAX_AGE_DAYS': 30,
    'EXCLUSION_FILE': 'exclusions.yaml',
    'THREAD_WORKERS': 5,
    'PAGE_LIMIT': 500
}

unused_resources = []
lock = threading.Lock()

class KubernetesCleaner:
    def __init__(self):
        self.exclusions = self.load_exclusions()
        self.api_resources = self.discover_resources()
        self.reference_cache = {}
        self.label_cache = {}
        
    def load_exclusions(self):
        """Load resource exclusions from YAML file"""
        if os.path.exists(CONFIG['EXCLUSION_FILE']):
            with open(CONFIG['EXCLUSION_FILE']) as f:
                return yaml.safe_load(f) or {}
        return {"namespaces": [], "resources": []}

    def discover_resources(self):
        """Discover all API resources with multi-version support"""
        resources = {}
        try:
            api_client = client.ApiClient()
            groups = client.ApisApi(api_client).get_api_versions().groups
            
            for group in groups:
                versions = [v.version for v in group.versions]
                for version in versions:
                    try:
                        for res in client.ApisApi(api_client).get_api_resources(
                            group.name, version
                        ).resources:
                            key = f"{group.name}/{version}/{res.kind}"
                            resources[key] = {
                                'group': group.name,
                                'version': version,
                                'kind': res.kind,
                                'namespaced': res.namespaced,
                                'verbs': res.verbs
                            }
                    except ApiException as e:
                        logger.warning(f"API discovery error: {e}")
        except Exception as e:
            logger.error(f"Resource discovery failed: {e}")
        return resources

    def is_excluded(self, kind, name, namespace):
        """Check if resource is in exclusion list"""
        if namespace in self.exclusions.get('namespaces', []):
            return True
        return any(
            p['type'] == kind and p['name'] == name and 
            p.get('namespace', namespace) in [namespace, '*']
            for p in self.exclusions.get('resources', [])
        )

    def check_ownership(self, resource):
        """Check for active owner references"""
        if not resource.metadata.owner_references:
            return False
        for ref in resource.metadata.owner_references:
            try:
                api = dynamic_client.resources.get(
                    api_version=ref.api_version, 
                    kind=ref.kind
                )
                api.get(name=ref.name, namespace=resource.metadata.namespace)
                return True
            except Exception as e:
                logger.debug(f"Owner check failed: {e}")
        return False

    def check_resource_age(self, resource):
        """Verify resource age against threshold"""
        try:
            delta = datetime.now(resource.metadata.creation_timestamp.tzinfo) - \
                   resource.metadata.creation_timestamp
            return delta.days > CONFIG['MAX_AGE_DAYS']
        except Exception as e:
            logger.error(f"Age check error: {e}")
            return False

    def check_references(self, kind, name, namespace):
        """Main reference checking logic with caching"""
        cache_key = f"{kind}/{namespace}/{name}"
        if cache_key in self.reference_cache:
            return self.reference_cache[cache_key]
        
        result = self._perform_reference_check(kind, name, namespace)
        self.reference_cache[cache_key] = result
        return result

    def _perform_reference_check(self, kind, name, namespace):
        """Execute comprehensive reference checks"""
        try:
            # Check resource-specific handlers first
            handler = getattr(self, f'check_{kind.lower()}', None)
            if handler:
                return handler(name, namespace)
            
            # Generic checks
            resource = self.get_resource(kind)
            obj = resource.get(name=name, namespace=namespace)
            
            if self.check_ownership(obj):
                return True
                
            if self.check_label_selectors(kind, name, namespace):
                return True
                
            return False
            
        except Exception as e:
            logger.error(f"Reference check failed: {e}")
            return True

    def check_label_selectors(self, kind, name, namespace):
        """Check for label selector references"""
        label_key = f"{namespace}/{name}"
        if label_key in self.label_cache:
            return self.label_cache[label_key]
            
        try:
            resource = self.get_resource(kind).get(name=name, namespace=namespace)
            labels = resource.metadata.labels or {}
            selector = ",".join([f"{k}={v}" for k, v in labels.items()])
            
            if not selector:
                return False
                
            for res_type in self.api_resources.values():
                if 'list' not in res_type['verbs']:
                    continue
                try:
                    api = dynamic_client.resources.get(
                        group=res_type['group'],
                        version=res_type['version'],
                        kind=res_type['kind']
                    )
                    items = api.get(
                        namespace=namespace if res_type['namespaced'] else None,
                        label_selector=selector
                    ).items
                    if items:
                        return True
                except Exception:
                    continue
            return False
        except Exception as e:
            logger.warning(f"Label check failed: {e}")
            return True

    # Resource-specific check implementations
    def check_configmap(self, name, namespace):
        """Check ConfigMap usage across all pods"""
        for pod in self.paginated_list(core_v1.list_namespaced_pod, namespace):
            for vol in pod.spec.volumes or []:
                if vol.config_map and vol.config_map.name == name:
                    return True
            for container in pod.spec.containers + pod.spec.init_containers:
                for env in container.env_from or []:
                    if env.config_map_ref and env.config_map_ref.name == name:
                        return True
        return False

    def check_secret(self, name, namespace):
        """Check Secret usage across all resources"""
        # Pod volume mounts
        for pod in self.paginated_list(core_v1.list_namespaced_pod, namespace):
            for vol in pod.spec.volumes or []:
                if vol.secret and vol.secret.secret_name == name:
                    return True
            # Container environment references
            for container in pod.spec.containers + pod.spec.init_containers:
                for env in container.env_from or []:
                    if env.secret_ref and env.secret_ref.name == name:
                        return True
        # ServiceAccount imagePullSecrets
        for sa in self.paginated_list(core_v1.list_namespaced_service_account, namespace):
            if any(secret.name == name for secret in sa.secrets or []):
                return True
        return False

    def check_service(self, name, namespace):
        """Check Service endpoints and ingress references"""
        # Endpoint subsets
        endpoints = core_v1.read_namespaced_endpoints(name, namespace)
        if endpoints.subsets:
            return True
        # Ingress references
        for ingress in self.paginated_list(client.NetworkingV1Api().list_namespaced_ingress, namespace):
            for rule in ingress.spec.rules or []:
                for path in rule.http.paths or []:
                    if path.backend.service and path.backend.service.name == name:
                        return True
        return False

    def check_persistentvolumeclaim(self, name, namespace):
        """Check PVC binding status"""
        pvc = core_v1.read_namespaced_persistent_volume_claim(name, namespace)
        return pvc.status.phase == 'Bound'

    def check_serviceaccount(self, name, namespace):
        """Check Pod ServiceAccount usage"""
        for pod in self.paginated_list(core_v1.list_namespaced_pod, namespace):
            if pod.spec.service_account_name == name:
                return True
        return False

    def check_networkpolicy(self, name, namespace):
        """Check NetworkPolicy pod selector matches"""
        policy = client.NetworkingV1Api().read_namespaced_network_policy(name, namespace)
        selector = policy.spec.pod_selector.match_labels or {}
        pods = core_v1.list_namespaced_pod(
            namespace, 
            label_selector=",".join(f"{k}={v}" for k, v in selector.items())
        )
        return len(pods.items) > 0

    # Add similar handlers for other resource types...

    def paginated_list(self, api_func, namespace=None):
        """Handle paginated list operations"""
        _continue = ""
        while True:
            try:
                resp = api_func(
                    namespace,
                    limit=CONFIG['PAGE_LIMIT'],
                    _continue=_continue
                ) if namespace else api_func(
                    limit=CONFIG['PAGE_LIMIT'],
                    _continue=_continue
                )
                for item in resp.items:
                    yield item
                _continue = resp.metadata._continue
                if not _continue:
                    break
            except ApiException as e:
                logger.error(f"List error: {e}")
                break

def get_cluster_name():
    """Extract cluster name from kubeconfig"""
    kubeconfig = config.list_kube_config_contexts()[1]
    return kubeconfig['context']['cluster']

def analyze_cluster():
    """Main analysis workflow"""
    cleaner = KubernetesCleaner()
    report_data = []
    
    # Process namespaced resources
    namespaces = [ns.metadata.name for ns in cleaner.paginated_list(core_v1.list_namespace)]
    
    with ThreadPoolExecutor(max_workers=CONFIG['THREAD_WORKERS']) as executor:
        # Process namespaces in parallel
        futures = []
        for ns in namespaces:
            futures.append(executor.submit(process_namespace, ns, cleaner))
        futures.append(executor.submit(process_cluster_resources, cleaner))
        
        for future in futures:
            try:
                report_data.extend(future.result())
            except Exception as e:
                logger.error(f"Processing failed: {e}")

    generate_report(report_data)

def process_namespace(namespace, cleaner):
    """Process a single namespace"""
    logger.info(f"Analyzing namespace: {namespace}")
    unused = []
    
    for res in cleaner.api_resources.values():
        if not res['namespaced']:
            continue
            
        try:
            api = dynamic_client.resources.get(
                group=res['group'],
                version=res['version'],
                kind=res['kind']
            )
            for item in api.get(namespace=namespace).items:
                if cleaner.is_excluded(res['kind'], item.metadata.name, namespace):
                    continue
                    
                if not cleaner.check_references(res['kind'], item.metadata.name, namespace):
                    if cleaner.check_resource_age(item) and not cleaner.check_ownership(item):
                        unused.append((
                            res['kind'],
                            item.metadata.name,
                            namespace,
                            item.metadata.creation_timestamp.strftime('%Y-%m-%d'),
                            (datetime.now() - item.metadata.creation_timestamp).days
                        ))
        except Exception as e:
            logger.error(f"Resource processing error: {e}")
    
    return unused

def process_cluster_resources(cleaner):
    """Process cluster-scoped resources"""
    logger.info("Analyzing cluster-wide resources")
    unused = []
    
    for res in cleaner.api_resources.values():
        if res['namespaced']:
            continue
            
        try:
            api = dynamic_client.resources.get(
                group=res['group'],
                version=res['version'],
                kind=res['kind']
            )
            for item in api.get().items:
                if cleaner.is_excluded(res['kind'], item.metadata.name, 'Cluster-wide'):
                    continue
                    
                if not cleaner.check_references(res['kind'], item.metadata.name, None):
                    if cleaner.check_resource_age(item) and not cleaner.check_ownership(item):
                        unused.append((
                            res['kind'],
                            item.metadata.name,
                            'Cluster-wide',
                            item.metadata.creation_timestamp.strftime('%Y-%m-%d'),
                            (datetime.now() - item.metadata.creation_timestamp).days
                        ))
        except Exception as e:
            logger.error(f"Cluster resource error: {e}")
    
    return unused

def generate_report(data):
    """Generate Excel report"""
    wb = openpyxl.Workbook()
    ws = wb.active
    ws.title = "Summary"
    headers = ["Kind", "Name", "Namespace", "Created", "Age (days)"]
    ws.append(headers)
    
    # Organize data by kind
    resource_map = {}
    for entry in data:
        kind = entry[0]
        if kind not in resource_map:
            resource_map[kind] = []
        resource_map[kind].append(entry)
    
    # Create worksheets
    for kind, items in resource_map.items():
        ws = wb.create_sheet(title=kind)
        ws.append(headers)
        for item in items:
            ws.append(item)
    
    # Save report
    cluster_name = get_cluster_name()
    filename = f"k8s-unused-report-{cluster_name}-{datetime.now().strftime('%Y%m%d-%H%M')}.xlsx"
    wb.save(filename)
    logger.info(f"Report generated: {filename}")

if __name__ == "__main__":
    analyze_cluster()
