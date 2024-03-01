
### Test Commands

```bash
# Create a Token Session
curl -X POST -H "Content-Type: application/json" -d '{"Username":"admin", "Password":"admin"}' http://localhost:4040/auth
```

```bash
TOKEN="f09a891a-c696-44ac-b72f-dafc810bc07e"
```

```bash
# Create the Topic
curl -X POST 'http://localhost:4040/management/add_topic?name=test&disk_path=\/tmp\/gpq\/topic\/test&buckets=10&sync_to_disk=false&reprioritize=true&reprioritize_rate=1m' -H "Accept: application/json" -H "Authorization: Bearer $TOKEN"
```

```bash
# Check the topic exists
curl http://localhost:4040/topic/list -H "Accept: application/json" -H "Authorization: Bearer $TOKEN"
```

```bash
# Enqueue the Event
curl -X POST 'http://localhost:4040/topic/test/enqueue?priority=1&should_escalate=true&escalate_every=1m&can_timeout=true&timeout_duration=30m' -H 'Content-Type: text/plain' -H "Accept: application/json" -H "Authorization: Bearer $TOKEN" \
-d 'Hello World!'
```

```bash
# Dequeue the Event
curl 'http://localhost:4040/topic/test/dequeue' -H "Accept: application/json" -H "Authorization: Bearer $TOKEN"
```