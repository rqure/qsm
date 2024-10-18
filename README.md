# qsm (Qureshi Service Manager)

This application monitors the services and reports the statuses to QDB. It monitors both q-services and non-q services such as nginx.

Note that memory usage may not work correctly. Docker requires cgroup to be enabled on your kernel.

For instance on the Raspberry PI this can be done by adding:
```
cgroup_enable=cpuset cgroup_enable=memory cgroup_memory=1
```
In the /boot/cmdline.txt file.
