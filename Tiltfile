# -*- mode: Python -*-
# PCAI Demo Baseline Tiltfile

# Load .env file
load('ext://dotenv', 'dotenv')
dotenv()

allow_k8s_contexts(os.environ['KUBE_CONTEXT'])
default_registry('registry.' + os.environ['DOMAIN'])

# Backend Service
docker_build(
    'erdincka/nexmesh-backend',
    './backend',
    live_update=[
        fall_back_on('backend/Dockerfile'),
        sync('./backend', '/app'),
        run('/app/.venv/bin/pip install -r /app/requirements.txt', trigger='./backend/requirements.txt'),
    ]
)

# Frontend (Next.js)
docker_build(
    'erdincka/nexmesh-frontend',
    './frontend',
    live_update=[
        fall_back_on('frontend/Dockerfile'),
        sync('./frontend/src', '/app/src'),
        sync('./frontend/public', '/app/public'),
    ],
)

k8s_yaml(helm(
    './helm/',
    name="nexmesh",
    namespace=os.environ['NAMESPACE'],
    set=[
        'ezua.virtualService.endpoint=nexmesh.' + os.environ["DOMAIN"]
    ],
))
