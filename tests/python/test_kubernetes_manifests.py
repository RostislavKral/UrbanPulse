from pathlib import Path
from typing import Any

import yaml

MANIFEST_DIR = Path("deploy/kubernetes")


def load_yaml_documents(path: Path) -> list[dict[str, Any]]:
    with path.open(encoding="utf-8") as handle:
        return [doc for doc in yaml.safe_load_all(handle) if doc]


def find_resource(
    documents: list[dict[str, Any]],
    kind: str,
    name: str,
) -> dict[str, Any]:
    for document in documents:
        metadata = document.get("metadata") or {}
        if document.get("kind") == kind and metadata.get("name") == name:
            return document
    raise AssertionError(f"Could not find {kind}/{name}")


def test_kubernetes_yaml_files_parse() -> None:
    yaml_files = sorted(MANIFEST_DIR.glob("*.yaml"))

    assert yaml_files
    for path in yaml_files:
        assert load_yaml_documents(path), f"{path} did not contain any YAML documents"


def test_kustomization_references_existing_resources() -> None:
    kustomization = yaml.safe_load((MANIFEST_DIR / "kustomization.yaml").read_text())
    resources = kustomization["resources"]

    assert "timescaledb.yaml" in resources
    assert "data-service.yaml" in resources
    assert "frontend.yaml" in resources
    for resource in resources:
        assert (MANIFEST_DIR / resource).exists()


def test_timescaledb_statefulset_has_persistent_storage() -> None:
    statefulset = find_resource(
        load_yaml_documents(MANIFEST_DIR / "timescaledb.yaml"),
        "StatefulSet",
        "timescaledb",
    )
    claims = statefulset["spec"]["volumeClaimTemplates"]

    assert claims[0]["metadata"]["name"] == "data"
    assert claims[0]["spec"]["resources"]["requests"]["storage"] == "20Gi"


def test_app_deployments_use_config_and_secret_inputs() -> None:
    data_service_docs = load_yaml_documents(MANIFEST_DIR / "data-service.yaml")
    data_api = find_resource(data_service_docs, "Deployment", "data-api")
    data_worker = find_resource(data_service_docs, "Deployment", "data-worker")

    for deployment in [data_api, data_worker]:
        container = deployment["spec"]["template"]["spec"]["containers"][0]
        env_from = container["envFrom"]
        assert {"configMapRef": {"name": "urbanpulse-config"}} in env_from
        assert {"secretRef": {"name": "urbanpulse-secrets"}} in env_from
