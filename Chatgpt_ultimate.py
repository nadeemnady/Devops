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
            handler = getattr(self, f'check_{kind.lower()}', None)
            if handler:
                return handler(name, namespace)
            
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

def get_cluster_name():
    """Extract cluster name from kubeconfig"""
    kubeconfig = config.list_kube_config_contexts()[1]
    return kubeconfig['context']['cluster']

def analyze_cluster():
    """Main analysis workflow"""
    cleaner = KubernetesCleaner()
    report_data = []
    
    namespaces = [ns.metadata.name for ns in cleaner.paginated_list(core_v1.list_namespace)]
    
    with ThreadPoolExecutor(max_workers=CONFIG['THREAD_WORKERS']) as executor:
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

def generate_report(data):
    """Generate Excel report"""
    wb = openpyxl.Workbook()
    ws = wb.active
    ws.title = "Summary"
    headers = ["Kind", "Name", "Namespace", "Created", "Age (days)"]
    ws.append(headers)
    
    resource_map = {}
    for entry in data:
        kind = entry[0]
        if kind not in resource_map:
            resource_map[kind] = []
        resource_map[kind].append(entry)
    
    for kind, items in resource_map.items():
        ws = wb.create_sheet(title=kind)
        ws.append(headers)
        for item in items:
            ws.append(item)
    
    cluster_name = get_cluster_name()
    filename = f"k8s-unused-report-{cluster_name}-{datetime.now().strftime('%Y%m%d-%H%M')}.xlsx"
    wb.save(filename)
    logger.info(f"Report generated: {filename}")

if __name__ == "__main__":
    analyze_cluster()
