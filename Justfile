default:
    just list

up:
    docker compose -f docker/dev/compose.yml up -d --build

down:
    docker compose -f docker/dev/compose.yml down --volumes

logs:
    docker compose -f docker/dev/compose.yml logs --follow
