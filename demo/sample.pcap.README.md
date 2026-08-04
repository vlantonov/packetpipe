# sample.pcap

The `sample.pcap` file is a minimal placeholder capture for offline demo flows.

To regenerate a richer sample capture (for example with DNS + HTTP traffic):

```bash
tcpdump -i any -c 500 -w demo/sample.pcap
```

Keep the file under 5 MB for repository portability.
