# -*- mode: Python -*-
# PCAI Demo Baseline Tiltfile

# Load .env file
load('ext://dotenv', 'dotenv')
dotenv()

allow_k8s_contexts(os.environ['KUBE_CONTEXT'])
default_registry('registry.' + os.environ['DOMAIN'])

# Backend Service
docker_build(
    'erdincka/catchx-backend',
    './backend',
    build_args={'MAPR_REPO': os.environ['MAPR_REPO']},
    live_update=[
        fall_back_on(['backend/Dockerfile', 'backend/requirements.txt']),
        sync('./backend/', '/app/'),
        run('/app/.venv/bin/pip install -r /app/requirements.txt', trigger='./backend/requirements.txt'),
    ]
)

# Frontend (Next.js)
docker_build(
    'erdincka/catchx-frontend',
    './frontend',
    live_update=[
        fall_back_on('frontend/Dockerfile'),
        sync('./frontend/', '/app/'),
    ],
)

k8s_yaml(helm(
    './helm/',
    name="catchx",
    namespace=os.environ['NAMESPACE'],
    set=[
        'ezua.virtualService.endpoint=catchx.' + os.environ["DOMAIN"]
    ],
))
