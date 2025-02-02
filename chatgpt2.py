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
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
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
    'THREAD_WORKERS': 10,
    'PAGE_LIMIT': 500,
    'WEBHOOK_URL': 'https://example.com/webhook'  # Change to your webhook endpoint
}

unused_resources = []
lock = threading.Lock()

class KubernetesScanner:
    def __init__(self):
        self.exclusions = self.load_exclusions()
        self.api_resources = self.discover_resources()
        self.reference_cache = {}
        self.label_cache = {}

    def load_exclusions(self):
        """Load exclusions from YAML file"""
        if os.path.exists(CONFIG['EXCLUSION_FILE']):
            with open(CONFIG['EXCLUSION_FILE']) as f:
                return yaml.safe_load(f) or {}
        return {"namespaces": [], "resources": []}

    def discover_resources(self):
        """Discover all Kubernetes resources dynamically"""
        resources = {}
        try:
            api_groups = dynamic_client.resources.api_groups
            for group in api_groups:
                for version in group['versions']:
                    try:
                        api = dynamic_client.resources.get(api_version=f"{group['name']}/{version['version']}")
                        for res in api.resources:
                            key = f"{group['name']}/{version['version']}/{res['kind']}"
                            resources[key] = {
                                'group': group['name'],
                                'version': version['version'],
                                'kind': res['kind'],
                                'namespaced': res.get('namespaced', True),
                                'verbs': res.get('verbs', [])
                            }
                    except ResourceNotFoundError:
                        continue
        except Exception as e:
            logger.error(f"Error discovering resources: {e}")
        return resources

    def is_excluded(self, kind, name, namespace):
        """Check if resource is excluded"""
        if namespace in self.exclusions.get('namespaces', []):
            return True
        return any(
            item['type'] == kind and item['name'] == name and item.get('namespace', namespace) in [namespace, '*']
            for item in self.exclusions.get('resources', [])
        )

    def is_unused(self, kind, name, namespace):
        """Check if a resource is unused"""
        cache_key = f"{kind}/{namespace}/{name}"
        if cache_key in self.reference_cache:
            return self.reference_cache[cache_key]

        used = self._perform_reference_check(kind, name, namespace)
        self.reference_cache[cache_key] = used
        return not used

    def _perform_reference_check(self, kind, name, namespace):
        """Perform advanced reference check"""
        try:
            # Check owner references
            resource = self.get_resource(kind)
            obj = resource.get(name=name, namespace=namespace)
            if obj.metadata.owner_references:
                return True

            # Check label-based dependencies
            if self.check_label_references(kind, name, namespace):
                return True

            return False
        except Exception:
            return True  # Assume it's in use to avoid false positives

    def check_label_references(self, kind, name, namespace):
        """Check label selector dependencies"""
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
                    items = api.get(namespace=namespace if res_type['namespaced'] else None, label_selector=selector).items
                    if items:
                        return True
                except Exception:
                    continue
            return False
        except Exception:
            return True

    def get_resource(self, kind):
        """Get dynamic resource handle"""
        for res in self.api_resources.values():
            if res['kind'].lower() == kind.lower():
                return dynamic_client.resources.get(group=res['group'], version=res['version'], kind=res['kind'])
        raise ResourceNotFoundError(f"Resource {kind} not found")

    def paginated_list(self, api_func, namespace=None):
        """Handle paginated API list calls"""
        _continue = None
        while True:
            try:
                resp = api_func(namespace=namespace, limit=CONFIG['PAGE_LIMIT'], _continue=_continue) if namespace else api_func(limit=CONFIG['PAGE_LIMIT'], _continue=_continue)
                for item in resp.items:
                    yield item
                _continue = resp.metadata._continue
                if not _continue:
                    break
            except ApiException as e:
                logger.error(f"Error listing resources: {e}")
                break

def analyze_cluster():
    """Main analysis workflow"""
    scanner = KubernetesScanner()
    report_data = []
    namespaces = [ns.metadata.name for ns in scanner.paginated_list(core_v1.list_namespace)]

    with ThreadPoolExecutor(max_workers=CONFIG['THREAD_WORKERS']) as executor:
        futures = [executor.submit(process_namespace, ns, scanner) for ns in namespaces]
        futures.append(executor.submit(process_cluster_resources, scanner))
        
        for future in futures:
            try:
                report_data.extend(future.result())
            except Exception as e:
                logger.error(f"Processing failed: {e}")

    generate_report(report_data)
    send_webhook_alert(report_data)

def process_namespace(namespace, scanner):
    """Process a single namespace"""
    logger.info(f"Scanning namespace: {namespace}")
    unused = []
    
    for res in scanner.api_resources.values():
        if not res['namespaced']:
            continue
        try:
            api = scanner.get_resource(res['kind'])
            for item in api.get(namespace=namespace).items:
                if scanner.is_excluded(res['kind'], item.metadata.name, namespace):
                    continue
                if scanner.is_unused(res['kind'], item.metadata.name, namespace):
                    unused.append((res['kind'], item.metadata.name, namespace))
        except Exception:
            continue
    return unused

def process_cluster_resources(scanner):
    """Process cluster-wide resources"""
    logger.info("Scanning cluster-wide resources")
    unused = []

    for res in scanner.api_resources.values():
        if res['namespaced']:
            continue
        try:
            api = scanner.get_resource(res['kind'])
            for item in api.get().items:
                if scanner.is_excluded(res['kind'], item.metadata.name, 'Cluster-wide'):
                    continue
                if scanner.is_unused(res['kind'], item.metadata.name, None):
                    unused.append((res['kind'], item.metadata.name, 'Cluster-wide'))
        except Exception:
            continue
    return unused

def generate_report(data):
    """Generate Excel report"""
    wb = openpyxl.Workbook()
    ws = wb.active
    ws.title = "Unused Resources"
    ws.append(["Kind", "Name", "Namespace"])

    for item in data:
        ws.append(item)

    filename = f"k8s-unused-{datetime.now().strftime('%Y%m%d-%H%M')}.xlsx"
    wb.save(filename)
    logger.info(f"Report generated: {filename}")

def send_webhook_alert(data):
    """Send webhook alerts"""
    if CONFIG['WEBHOOK_URL']:
        import requests
        requests.post(CONFIG['WEBHOOK_URL'], json={"unused_resources": data})
        logger.info("Webhook alert sent")

if __name__ == "__main__":
    analyze_cluster()
