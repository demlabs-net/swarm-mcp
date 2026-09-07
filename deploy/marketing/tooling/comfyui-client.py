#!/usr/bin/env python3
"""ComfyUI REST client: inspect safely by default, opt in to generation."""
import argparse
import json
import os
from pathlib import Path
import time
import urllib.parse
import urllib.request
import uuid


def request(base, path, data=None, binary=False):
    headers = {}
    if os.environ.get('COMFYUI_API_KEY'):
        headers['Authorization'] = 'Bearer ' + os.environ['COMFYUI_API_KEY']
    if data is not None:
        headers['Content-Type'] = 'application/json'
    req = urllib.request.Request(base + path, data=None if data is None else json.dumps(data).encode(), headers=headers)
    with urllib.request.urlopen(req, timeout=30) as response:
        return response.read() if binary else json.load(response)


def graph(config, prompt, seed):
    return {
        '1': {'class_type': 'CheckpointLoaderSimple', 'inputs': {'ckpt_name': config['checkpoint']}},
        '2': {'class_type': 'CLIPTextEncode', 'inputs': {'text': prompt, 'clip': ['1', 1]}},
        '3': {'class_type': 'CLIPTextEncode', 'inputs': {'text': 'text, watermark, logo, blurry', 'clip': ['1', 1]}},
        '4': {'class_type': 'EmptyLatentImage', 'inputs': {'width': config['width'], 'height': config['height'], 'batch_size': 1}},
        '5': {'class_type': 'KSampler', 'inputs': {'model': ['1', 0], 'positive': ['2', 0], 'negative': ['3', 0], 'latent_image': ['4', 0], 'seed': seed, 'steps': config['steps'], 'cfg': config['cfg'], 'sampler_name': config['sampler_name'], 'scheduler': config['scheduler'], 'denoise': 1.0}},
        '6': {'class_type': 'VAEDecode', 'inputs': {'samples': ['5', 0], 'vae': ['1', 2]}},
        '7': {'class_type': 'SaveImage', 'inputs': {'images': ['6', 0], 'filename_prefix': 'marketing/background'}},
    }


def validate(workflow, catalog):
    for node in workflow.values():
        name = node['class_type']
        if name not in catalog:
            raise ValueError('Missing ComfyUI node: ' + name)
        required = catalog[name]['input'].get('required', {})
        for key, spec in required.items():
            if key not in node['inputs']:
                raise ValueError('Missing input: ' + name + '.' + key)
            value = node['inputs'][key]
            if isinstance(spec[0], list) and value not in spec[0]:
                raise ValueError('Unsupported choice: ' + name + '.' + key)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--config', default='/opt/data/comfyui.json')
    parser.add_argument('--generate', action='store_true')
    parser.add_argument('--prompt', default='Abstract blue geometric background, no text or logo')
    parser.add_argument('--seed', type=int, default=42)
    args = parser.parse_args()
    config = json.loads(Path(args.config).read_text())
    base = os.environ.get('COMFYUI_URL', config['base_url']).rstrip('/')
    stats = request(base, '/system_stats')
    queue = request(base, '/queue')
    workflow = graph(config, args.prompt, args.seed)
    validate(workflow, request(base, '/object_info'))
    print(json.dumps({'endpoint': base, 'workflow_validated_against_catalog': True, 'devices': stats.get('devices'), 'queue': queue}, ensure_ascii=False))
    if not args.generate:
        return
    if queue.get('queue_running') or queue.get('queue_pending'):
        raise SystemExit('ComfyUI queue is busy; no job submitted')
    cuda = [d for d in stats.get('devices', []) if d.get('type') == 'cuda']
    if not cuda or any(d.get('vram_free', 0) < config['minimum_free_vram_bytes'] for d in cuda):
        raise SystemExit('Insufficient free GPU memory; no job submitted. Ask infrastructure owner, do not unload other models.')
    job = request(base, '/prompt', {'prompt': workflow, 'client_id': str(uuid.uuid4())})
    if job.get('node_errors') or not job.get('prompt_id'):
        raise SystemExit('ComfyUI rejected graph: ' + json.dumps(job))
    prompt_id = job['prompt_id']
    # Server-supplied identifiers must not escape output_dir or alter the URL path.
    if not isinstance(prompt_id, str) or str(uuid.UUID(prompt_id)) != prompt_id:
        raise SystemExit('ComfyUI returned an invalid prompt identifier')
    out = Path(config['output_dir']) / prompt_id
    out.mkdir(parents=True, exist_ok=False)
    (out / 'workflow.json').write_text(json.dumps({'endpoint': base, 'prompt_id': prompt_id, 'seed': args.seed, 'prompt': workflow}, indent=2))
    print('Submitted ' + prompt_id, flush=True)
    deadline = time.monotonic() + config['timeout_seconds']
    while time.monotonic() < deadline:
        entry = request(base, '/history/' + prompt_id).get(prompt_id)
        if entry:
            (out / 'history.json').write_text(json.dumps(entry, indent=2))
            if entry.get('status', {}).get('status_str') == 'error':
                raise SystemExit('Generation failed; inspect ' + str(out / 'history.json'))
            images = [image for node in entry.get('outputs', {}).values() for image in node.get('images', [])]
            if not images:
                raise SystemExit('Completed without images; inspect history')
            for index, image in enumerate(images):
                data = request(base, '/view?' + urllib.parse.urlencode({k: image[k] for k in ('filename', 'subfolder', 'type') if k in image}), binary=True)
                if not data.startswith(b'\x89PNG\r\n\x1a\n'):
                    raise SystemExit('Unexpected image format; expected PNG')
                target = out / (str(index) + '-' + Path(image['filename']).name)
                target.write_bytes(data)
                print(str(target))
            return
        time.sleep(2)
    raise SystemExit('Timed out; job may still be running. Check /history/' + prompt_id + '; do not automatically resubmit.')


if __name__ == '__main__':
    main()
