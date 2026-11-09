"""Exercise a running platform over TCP and compare real B1 predictions.

The script creates its own applications and custom template. It does not train,
change model artifacts, inject fake responses or stop the running service.
"""
from pathlib import Path
import argparse
import hashlib
import json
import uuid

import httpx
import numpy as np


def custom_template():
    def node(name, capability, dependencies, inputs):
        return dict(node_id=name, capability_id=capability, depends_on=dependencies,
                    parameters=dict(inputs=inputs))

    def binding(node_id, output):
        return dict(node_id=node_id, output=output)

    return dict(requested_outputs=['flow', 'od'], nodes=[
        node('prepare', 'history.prepare', [], {}),
        node('predict', 'traffic.predict', ['prepare'], {'model_inputs': binding('prepare', 'inputs')}),
        node('flow', 'validate.flow', ['predict'], {'prediction': binding('predict', 'flow')}),
        node('od', 'validate.od', ['predict'], {'prediction': binding('predict', 'od')}),
        node('aggregate', 'result.aggregate', ['flow', 'od'],
             {'flow': binding('flow', 'flow'), 'od': binding('od', 'od')}),
    ])


def split_template(mixed=False):
    definition = custom_template()
    definition['nodes'] = definition['nodes'][:-1]
    for task in ('flow',) if mixed else ('flow', 'od'):
        definition['nodes'].append(dict(node_id='export_' + task, capability_id='result.aggregate',
            depends_on=[task], parameters=dict(inputs={task: dict(node_id=task, output=task)})))
    return definition


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--base-url', default='http://127.0.0.1:18763')
    parser.add_argument('--artifact-root', required=True, type=Path)
    parser.add_argument('--output-root', required=True, type=Path)
    args = parser.parse_args()
    run_id = uuid.uuid4().hex[:12]
    output = args.output_root / ('http_' + run_id)
    output.mkdir(parents=True, exist_ok=False)
    repo = Path(__file__).resolve().parents[1]
    source_paths = [repo / 'traffic_server.py', Path(__file__).resolve()]
    for directory in ('src/api', 'src/service', 'src/graph'):
        source_paths.extend((repo / directory).glob('traffic*.py'))
    source_sha256 = {str(path.relative_to(repo)): hashlib.sha256(path.read_bytes()).hexdigest()
                     for path in source_paths}
    reference_path = args.artifact_root / 'results/B1_seed42_L3/test_predictions.npz'
    reference_sha = hashlib.sha256(reference_path.read_bytes()).hexdigest()
    frames = list(range(1264, 1272))
    runs, calls = [], []
    prefix = '/traffic/platform'
    with httpx.Client(base_url=args.base_url, timeout=120.0) as client:
        def request(method, path, expected=200, **kwargs):
            response = client.request(method, path, **kwargs)
            calls.append(dict(method=method, path=path, status=response.status_code))
            assert response.status_code == expected, (path, response.status_code, response.text)
            return response.json()

        overview = request('GET', prefix + '/overview')
        instances = sorted(overview['instances'])
        assert len(instances) >= 2, 'This acceptance check requires at least two real instances'
        custom_id = 'http_flow_od_' + run_id
        request('POST', prefix + '/templates', 201,
                json=dict(template_id=custom_id, definition=custom_template()))
        no_aggregate_id = 'http_terminal_flow_' + run_id
        terminal_definition = dict(requested_outputs=['flow'], nodes=custom_template()['nodes'][:3])
        request('POST', prefix + '/templates', 201,
                json=dict(template_id=no_aggregate_id, definition=terminal_definition))
        split_id, mixed_id = 'http_split_' + run_id, 'http_mixed_' + run_id
        for template_id, mixed in ((split_id, False), (mixed_id, True)):
            request('POST', prefix + '/templates', 201,
                    json=dict(template_id=template_id, definition=split_template(mixed)))
        registered = {item['template'] for item in request('GET', prefix + '/templates')}
        assert {custom_id, no_aggregate_id, split_id, mixed_id} <= registered
        for template, outputs in [('flow', {'flow'}), ('analysis', {'state', 'flow'}),
                                  ('od_hotspots', {'od'}), (custom_id, {'flow', 'od'}),
                                  (no_aggregate_id, {'flow'}), (split_id, {'flow', 'od'}),
                                  (mixed_id, {'flow', 'od'})]:
            body = dict(template=template, target_frames=frames, deadline_seconds=120.0)
            app = request('POST', prefix + '/apps', 201,
                          json=dict(name='TCP验收_' + template + '_' + run_id, request=body))
            submitted = request('POST', prefix + '/apps/' + app['app_id'] + '/run', 202, json={})
            workflow = request('GET', prefix + '/workflows/' + submitted['workflow_id'] + '?wait=true')
            result = workflow['result']
            assert result['status'] == 'completed' and set(result['predictions']) == outputs
            current = request('GET', prefix + '/apps/' + app['app_id'])
            assert current['status'] == 'stopped', current
            runs.append(dict(template=template, app_id=app['app_id'], workflow_id=submitted['workflow_id'],
                             request=body, result=result, application=current))
        for instance_id in instances[:2]:
            message = dict(sender_id='http-acceptance', receiver_id=instance_id, message_type='request',
                           payload=dict(task_id='a2a-' + run_id + '-' + instance_id,
                                        task_type='traffic', task_description='Real joint traffic analysis',
                                        timeout=120, context=dict(template='analysis', target_frames=frames,
                                                                  enable_reuse=False)))
            envelope = request('POST', '/a2a/execute', json=message)
            assert envelope['payload']['status'] == 'success', envelope
            result = envelope['payload']['result']
            selected = [task['instance_id'] for task in result['tasks'].values()
                        if task['capability_id'] == 'traffic.predict']
            assert selected == [instance_id], selected
            runs.append(dict(template='a2a_analysis', instance_id=instance_id,
                             request=message, result=result))
        request('POST', prefix + '/instances/' + instances[0] + '/health',
                json=dict(healthy=False, reason='TCP acceptance health transition'))
        unhealthy = request('GET', prefix + '/catalog')
        assert unhealthy['instances'][instances[0]]['healthy'] is False
        request('POST', prefix + '/instances/' + instances[0] + '/health',
                json=dict(healthy=True, reason='TCP acceptance restore'))
        final = request('GET', prefix + '/overview')
        assert not final['reservations']
        assert all(final['resources'][key] == 0 for key in ('cpu_used', 'memory_used_mb', 'concurrency'))
        assert final['active_futures'] == 0 and final['cache']['inflight'] == 0
    differences = []
    with np.load(reference_path) as reference:
        positions = np.searchsorted(reference['target_frames'], frames)
        assert np.array_equal(reference['target_frames'][positions], frames)
        for run in runs:
            result = run['result']
            assert result['target_frames'] == frames
            difference = {}
            for task, prediction in result['predictions'].items():
                actual = np.asarray(prediction['prediction'], dtype=np.float32)[:, 0]
                expected = reference['pred_' + task][positions]
                maximum = float(np.abs(actual - expected).max())
                assert np.isfinite(actual).all() and np.allclose(actual, expected, rtol=1e-5, atol=1e-5)
                assert maximum <= 1e-5, (task, maximum)
                difference[task] = maximum
            differences.append(dict(template=run['template'], max_abs=difference))
    assert hashlib.sha256(reference_path.read_bytes()).hexdigest() == reference_sha
    assert source_sha256 == {str(path.relative_to(repo)): hashlib.sha256(path.read_bytes()).hexdigest()
                             for path in source_paths}, 'Source changed during TCP acceptance'
    (output / 'http_workflows.json').write_text(json.dumps(runs, ensure_ascii=False, indent=2))
    (output / 'http_final_snapshot.json').write_text(json.dumps(final, ensure_ascii=False, indent=2))
    manifest = dict(status='passed', url=args.base_url, workflows=len(runs),
                    templates=7, native_a2a_instances=instances[:2], calls=calls,
                    reference_sha256=reference_sha, prediction_differences=differences,
                    source_sha256=source_sha256, resources_released=True, service_still_running=True)
    (output / 'http_manifest.json').write_text(json.dumps(manifest, ensure_ascii=False, indent=2))
    print(json.dumps(dict(status='passed', directory=str(output), workflows=len(runs),
                         differences=differences), ensure_ascii=False, indent=2))


if __name__ == '__main__':
    main()
