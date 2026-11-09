"""Lightweight checks for the standalone UI factory and real API wiring."""

import ast
import re
from html.parser import HTMLParser
from pathlib import Path

from src.api.traffic_platform_ui import get_traffic_platform_html


def test_platform_factory_is_standalone_and_has_stable_hooks():
    html = get_traffic_platform_html()
    assert html.startswith("<!doctype html>")
    assert '<html lang="zh-CN">' in html
    for hook in (
        "service-ready", "template-select", "target-time", "batch-size",
        "create-btn", "run-btn", "stop-btn", "app-list", "workflow-select",
        "dag-content", "queue-content", "result-grid",
    ):
        assert f'id="{hook}"' in html
    assert html.count('role="tabpanel"') == 5
    assert "window,'trafficPlatform'" in html
    assert "writable:false,configurable:false" in html
    class AssetParser(HTMLParser):
        def handle_starttag(self, tag, attrs):
            if tag in {"script", "link"}:
                assert all(value.startswith("data:") for key, value in attrs
                           if key in {"src", "href"} and value is not None)

    AssetParser().feed(html)


def test_platform_uses_management_api_without_simulated_results():
    html = get_traffic_platform_html()
    for route in ("/overview", "/catalog", "/templates", "/apps", "/resources", "/qos", "/workflows/"):
        assert route in html
    assert "'/traffic/platform'" in html
    assert "method:'POST'" in html
    assert "target_frames,deadline_seconds:" in html
    assert "value=\"2024-01-27T08:00\"" in html
    assert "value=\"8\"" in html
    assert "Math.random" not in html
    assert not re.search(r"\b(?:mock|fake|synthetic)\w*\s*[:=]", html, re.I)
    assert "schedule_status" in html and "call_status" in html and "quality_status" in html
    assert "不能解释为操作系统利用率" in html


def test_platform_python_has_no_model_or_framework_imports():
    module = Path(__file__).resolve().parents[2] / "src/api/traffic_platform_ui.py"
    tree = ast.parse(module.read_text(encoding="utf-8"))
    assert not any(isinstance(node, (ast.Import, ast.ImportFrom)) for node in ast.walk(tree))
