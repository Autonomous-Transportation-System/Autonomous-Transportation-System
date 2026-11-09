"""Export an existing, trained B1 experiment into a portable local artifact root.

This copies real artifacts without training, generating data, or changing their
contents. The output belongs in ignored data storage, outside source commits.
"""
from pathlib import Path
import argparse
import hashlib
import json
import shutil


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--source', type=Path, required=True)
    parser.add_argument('--destination', type=Path, required=True)
    args = parser.parse_args()
    source = args.source.expanduser().resolve()
    destination = args.destination.expanduser().resolve()
    if source == destination or source in destination.parents:
        parser.error('Destination must be outside the source experiment')
    files = list((source / 'model').glob('*.py'))
    required = [
        'data/processed/dataset_metadata.json',
        'data/processed/normalization_train_only.npz',
        'data/processed/nyc_tlc_green_2024_01_6x6.npz',
        'results/B1_seed42_L3/best.pt',
    ]
    for name in required:
        path = source / name
        if not path.is_file():
            parser.error('Missing real artifact: ' + str(path))
        files.append(path)
    if not (source / 'model/traffic_net.py').is_file():
        parser.error('Missing original TrafficNet implementation')
    for name in ('results/B1_seed42_L3/result.json',
                 'results/B1_seed42_L3/test_predictions.npz'):
        if (source / name).is_file():
            files.append(source / name)
    if destination.exists() and any(destination.iterdir()):
        parser.error('Destination is nonempty; choose a new artifact directory')
    manifest = {'kind': 'real trained B1 export', 'files': {}}
    for path in sorted(files):
        relative = path.relative_to(source)
        target = destination / relative
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(path, target)
        digest = hashlib.sha256(target.read_bytes()).hexdigest()
        assert digest == hashlib.sha256(path.read_bytes()).hexdigest()
        manifest['files'][str(relative)] = {'sha256': digest, 'bytes': target.stat().st_size}
    (destination / 'artifact_manifest.json').write_text(
        json.dumps(manifest, ensure_ascii=False, indent=2))
    print('Exported', len(files), 'unchanged real files to', destination)
    print('Set TRAFFIC_EXPERIMENT_ROOT to this directory before starting the server.')


if __name__ == '__main__':
    main()
