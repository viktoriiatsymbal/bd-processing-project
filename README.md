Team members-Mykhailo Ponomarenko, Viktoriia Tsymbal

cd /Users/mykhailoponomarenko/Desktop/UCUyear3/Semester2/BigData/bd-processing-project-transfer

docker compose down -v --remove-orphans

docker rm -f $(docker ps -aq) 2>/dev/null || true

rm -rf minio-data

find . -type d \( -iname "*checkpoint*" -o -iname "chk*" \) -exec rm -rf {} +

docker compose up -d --build kafka cassandra minio kafka-init cassandra-init minio-init consumer-cassandra consumer-minio spark-master spark-worker api

until docker exec cassandra cqlsh -e "DESCRIBE KEYSPACES" >/dev/null 2>&1; do sleep 5; done

until [ "$(docker inspect -f '{{.State.ExitCode}}' cassandra-init 2>/dev/null)" = "0" ]; do sleep 3; done

until curl -sf http://localhost:8000/health >/dev/null; do sleep 3; done

docker compose ps

docker exec cassandra cqlsh -e "USE wiki_analytics; DESCRIBE TABLES;"

perl -0pi -e 's/docker exec -it/docker exec/g' run_streaming.sh

rm -f streaming.log .streaming_pid

./run_streaming.sh > streaming.log 2>&1 & echo $! > .streaming_pid

sleep 30

docker compose up -d --force-recreate ingestion

while [ "$(docker inspect -f '{{.State.Running}}' wikimedia-ingestion 2>/dev/null)" = "true" ]; do sleep 2; done

docker compose logs --tail=120 ingestion consumer-cassandra consumer-minio

sleep 90

docker exec cassandra cqlsh -e "USE wiki_analytics; SELECT COUNT(*) FROM pages_by_domain;"

docker exec cassandra cqlsh -e "USE wiki_analytics; SELECT COUNT(*) FROM pages_by_user;"

docker exec cassandra cqlsh -e "USE wiki_analytics; SELECT * FROM language_activity LIMIT 5;"

docker exec cassandra cqlsh -e "USE wiki_analytics; SELECT * FROM bot_activity_metrics LIMIT 5;"

NETWORK=$(docker inspect minio -f '{{range $k,$v := .NetworkSettings.Networks}}{{println $k}}{{end}}' | head -1)

docker run --rm --network "$NETWORK" --entrypoint /bin/sh minio/mc:latest -c "mc alias set local http://minio:9000 wikiuser wikipass >/dev/null && mc ls -r local/wiki-events | wc -l"

docker run --rm --network "$NETWORK" --entrypoint /bin/sh minio/mc:latest -c "mc alias set local http://minio:9000 wikiuser wikipass >/dev/null && mc ls -r local/wiki-events | head -5"

docker exec spark-master sh -lc "pkill -f spark-submit || true"

kill $(cat .streaming_pid) 2>/dev/null || true

docker compose stop spark-master spark-worker

docker compose rm -f spark-master spark-worker

docker compose up -d spark-master spark-worker

sleep 20

./run_batch.sh

docker exec cassandra cqlsh -e "USE wiki_analytics; SELECT COUNT(*) FROM hourly_activity_by_domain;"

docker exec cassandra cqlsh -e "USE wiki_analytics; SELECT COUNT(*) FROM editor_behavior_patterns;"

docker exec cassandra cqlsh -e "USE wiki_analytics; SELECT * FROM hourly_activity_by_domain LIMIT 5;"

docker exec cassandra cqlsh -e "USE wiki_analytics; SELECT * FROM editor_behavior_patterns LIMIT 5;"

docker run --rm --network "$NETWORK" --entrypoint /bin/sh minio/mc:latest -c "mc alias set local http://minio:9000 wikiuser wikipass >/dev/null && mc ls -r local/wiki-events | grep -i batch | head -10"

USER_ID=$(docker exec cassandra cqlsh -e "USE wiki_analytics; SELECT user_id FROM pages_by_user LIMIT 1;" | awk '/^[[:space:]]*[0-9]+[[:space:]]*$/ {print $1; exit}')

PAGE_ID=$(docker exec cassandra cqlsh -e "USE wiki_analytics; SELECT page_id FROM page_details LIMIT 1;" | awk '/^[[:space:]]*[0-9]+[[:space:]]*$/ {print $1; exit}')

DOMAIN=$(docker exec cassandra cqlsh -e "USE wiki_analytics; SELECT domain FROM pages_by_domain LIMIT 1;" | awk -F'|' '/\./ {gsub(/^[ \t]+|[ \t]+$/,"",$1); print $1; exit}')

limit_json() { python3 -c 'import json,sys; n=int(sys.argv[1]); data=json.load(sys.stdin); trim=lambda x: x[:n] if isinstance(x,list) else ({k:(v[:n] if isinstance(v,list) else v) for k,v in x.items()} if isinstance(x,dict) else x); print(json.dumps(trim(data),indent=2,ensure_ascii=False))' "$1"; }

curl -s http://localhost:8000/health | limit_json 5

curl -s http://localhost:8000/api/domains | limit_json 5

curl -s "http://localhost:8000/api/users/$USER_ID/pages?limit=5" | limit_json 5

curl -s "http://localhost:8000/api/pages/$PAGE_ID" | limit_json 5

curl -s "http://localhost:8000/api/domains/$DOMAIN/pages?from=2026-01-01T00:00:00&to=2027-01-01T00:00:00&limit=5" | limit_json 5

curl -s "http://localhost:8000/api/reports/hourly?domain=$DOMAIN&hours=6" | limit_json 5

curl -s "http://localhost:8000/api/analytics/editor-patterns?min_pages=5" | limit_json 5

docker compose logs --tail=80 api

git status