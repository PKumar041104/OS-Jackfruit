# OS Jackfruit Mini Project  
## User-Space Container Runtime with Kernel-Space Memory Monitoring

---

## 1. Team Information

**Team Member 1:** Pratham Kumar  
**SRN:** `<ENTER_SRN>`  

**Team Member 2:** `<ENTER_NAME_IF_ANY>`  
**SRN:** `<ENTER_SRN_IF_ANY>`  

**Course:** UE24CS242B - Operating Systems  
**Institute:** PES University  
**Project Title:** OS Jackfruit Mini Project  

---

## 2. Project Overview

This project implements a lightweight container runtime in C along with a Linux kernel module for memory monitoring and enforcement. The goal is to build a simplified container system that demonstrates core operating system concepts such as process isolation, namespaces, parent-child supervision, IPC, synchronization, logging pipelines, kernel-user communication, process scheduling, and memory enforcement.

The project is divided into two major parts:

1. **User-space runtime (`engine.c`)**
   - Starts and maintains a long-running supervisor process
   - Accepts CLI commands such as `supervisor`, `start`, `run`, `ps`, `logs`, and `stop`
   - Launches containers using Linux namespaces
   - Tracks container metadata
   - Captures container output through a logging pipeline
   - Communicates with the kernel module through `ioctl`

2. **Kernel-space memory monitor (`monitor.c`)**
   - Creates the device `/dev/container_monitor`
   - Accepts monitoring requests from user space
   - Tracks registered container processes
   - Checks resident memory periodically
   - Produces soft-limit warnings
   - Enforces hard limits by killing offending processes

This project is intentionally close to real OS mechanisms. The containers are not virtual machines. They still share the same host kernel, but they are isolated in terms of PID view, hostname, and mount view using namespaces.

---

## 3. Objectives

The main objectives of this project are:

- To implement a multi-container runtime supervised by one parent process
- To isolate containers using Linux namespaces and separate root filesystems
- To provide CLI-based management of containers
- To implement bounded-buffer-based logging
- To build a kernel module that monitors and enforces memory usage
- To demonstrate scheduling behavior using different workloads
- To connect implementation details with fundamental OS concepts

---

## 4. Repository Structure

```text
OS-Jackfruit/
├── boilerplate/
│   ├── engine.c
│   ├── monitor.c
│   ├── monitor_ioctl.h
│   ├── Makefile
│   ├── cpu_hog.c
│   ├── memory_hog.c
│   ├── io_pulse.c
│   └── environment-check.sh
├── screenshots/
│   ├── Screenshots_1_build_success.png
│   ├── Screenshots_2_rootfs_setup.png
│   ├── Screenshots_3_kernel_module.png
│   ├── Screenshots_4_supervisor_ready.png
│   ├── Screenshots_5_cpu_hog.png
│   ├── Screenshots_6_io_pulse.png
│   ├── Screenshots_7_memory_hog.png
│   └── Screenshots_8_control_run.png
├── docs/
│   └── <report file will be added here>
├── README.md
├── project-guide.md
└── .gitignore

---

## 5. Environment Setup from Scratch

This section explains the setup from the beginning, starting from a clean Ubuntu VM.


### 5.1 Recommended environment

- Ubuntu 22.04 or 24.04 VM
- VMware Workstation / VirtualBox
- Internet connection
- Kernel headers matching the running kernel
- Sudo access


### 5.2 Update package list
