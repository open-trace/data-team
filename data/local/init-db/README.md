# Postgres init scripts

Scripts in this directory run **once** when the Postgres container is first created (when the data volume is empty). They are mounted into the container as `/docker-entrypoint-initdb.d/`.

- **01-create-datateam_local.sql** — Creates the `datateam_local` database so it exists after `docker compose up -d`.

If you already had a running container and volume, init scripts do not run again. To re-run them, remove the volume and start fresh: `docker compose down -v` then `docker compose up -d` (this deletes existing Postgres data).
