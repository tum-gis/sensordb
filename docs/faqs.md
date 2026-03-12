# Frequently Asked Questions

## How can I start a fresh 3DSensorDB?

Stop and remove the existing container (including its volumes):

```bash
docker stop sensordb
docker rm -v sensordb
```

### A task is too slow, uses too much memory, or the database crashes — what can I do?

Performance depends on both the database and the companion tool (import/export).

- If both run on the same machine, they compete for CPU and RAM.
- If they run on different machines, network bandwidth and latency are usually the bottleneck.

Quick guidelines:

- Start conservative: set `--db-max-connections` to 5–10.
- If throughput is low and resources are available:
    - Same machine: gradually increase `--db-max-connections` (e.g., +5 at a time).
    - Different machines: first check network throughput/latency; increasing connections helps only if the network isn’t
      saturated.
- If you see high memory or crashes: lower `--db-max-connections`.

Tips:

- Monitor CPU, RAM, disk I/O, and network (throughput, RTT, packet loss).
- Avoid running other heavy tasks in parallel.
- On bandwidth‑limited links, prefer steady streaming over many aggressive connections.

Goal: use the smallest `--db-max-connections` that saturates the slowest component (often the network across machines)
without exhausting resources.

## How can I ignore features for the point cloud association process?

When building models that include both detailed geometries and LOD1 solid geometries, you may want to exclude certain
features from the point cloud association process to avoid Z fighting.
For example, to exclude all geometries of `Building` features from the association process, run the following SQL query:

```sql
DO
$$
BEGIN
  PERFORM
delete_geometry_data(array_agg(geometry_data.id))
  FROM geometry_data
  LEFT JOIN feature ON feature.id = geometry_data.feature_id
  LEFT JOIN citydb.objectclass ON feature.objectclass_id = objectclass.id
  WHERE objectclass.classname = 'Building';
END$$;
```

This will enable a distinct association with `WallSurface`, `RoofSurface`, and `GroundSurface` features.

## Network-Related Issues with Docker Desktop on Linux

Docker can be installed on Linux in two main ways:

-[Docker Desktop](https://docs.docker.com/desktop/setup/install/linux/): Includes a graphical interface, integrated Kubernetes support, and additional developer tools.
- [Docker Engine](https://docs.docker.com/engine/install/ubuntu/): A lightweight, command-line–based installation that runs natively on Linux.

While Docker Engine runs directly on the Linux kernel, Docker Desktop runs a virtual machine powered by QEMU to ensure [environment consistency across platforms](https://docs.docker.com/desktop/troubleshoot-and-support/faqs/linuxfaqs/#why-does-docker-desktop-for-linux-run-a-vm).
This extra virtualization layer can lead to network connectivity issues, resource overhead, and reduced performance compared to native Docker Engine.

For this reason, it is generally recommended to use Docker Engine on Linux, and use Docker Desktop for MacOS and
Windows, where virtualization is actually required.

## My RAM on my client machine is running low, how can I free up some space?

If you are on a Linux machine, you can increase the swap space by running the following [this tutorial](https://ploi.io/documentation/server/change-swap-size-in-ubuntu).
