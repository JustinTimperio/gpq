
### Test Commands

```bash
curl -X POST 'http://localhost:4040/settings/add_topic?name=test&disk_path=\/tmp\/gpq\/topic\/test&buckets=10&sync_to_disk=false&reprioritize=true&reprioritize_rate=1m'

curl -X POST 'http://localhost:4040/topic/test/enqueue?priority=1&should_escalate=true&escalate_every=1m&can_timeout=true&timeout_duration=30m' \
-H 'Content-Type: text/plain' \
-d 'Hello World!'

curl 'http://localhost:4040/topic/test/dequeue'
```