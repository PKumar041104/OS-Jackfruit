// Name: Pratham Kumar  
// SRN No.:PES2UG24CS368
#define _GNU_SOURCE
#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <poll.h>
#include <pthread.h>
#include <sched.h>
#include <signal.h>
#include <stdarg.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/ioctl.h>
#include <sys/mount.h>
#include <sys/resource.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/un.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>

#include "monitor_ioctl.h"

#define STACK_SIZE (1024 * 1024)
#define CONTAINER_ID_LEN 32
#define CONTROL_PATH "/tmp/mini_runtime.sock"
#define LOG_DIR "logs"
#define CONTROL_MESSAGE_LEN 256
#define CHILD_COMMAND_LEN 256
#define LOG_CHUNK_SIZE 4096
#define LOG_BUFFER_CAPACITY 64
#define DEFAULT_SOFT_LIMIT (40UL << 20)
#define DEFAULT_HARD_LIMIT (64UL << 20)

typedef enum {
    CMD_SUPERVISOR = 0,
    CMD_START,
    CMD_RUN,
    CMD_PS,
    CMD_LOGS,
    CMD_STOP
} command_kind_t;

typedef enum {
    CONTAINER_STARTING = 0,
    CONTAINER_RUNNING,
    CONTAINER_STOPPED,
    CONTAINER_KILLED,
    CONTAINER_EXITED
} container_state_t;

typedef struct container_record {
    char id[CONTAINER_ID_LEN];
    char rootfs[PATH_MAX];
    char command[CHILD_COMMAND_LEN];
    pid_t host_pid;
    time_t started_at;
    container_state_t state;
    unsigned long soft_limit_bytes;
    unsigned long hard_limit_bytes;
    int exit_code;
    int exit_signal;
    int nice_value;
    int stop_requested;
    int finished;
    int log_pipe_read_fd;
    int producer_started;
    pthread_t producer_thread;
    void *child_stack;
    char log_path[PATH_MAX];
    struct container_record *next;
} container_record_t;

typedef struct {
    char container_id[CONTAINER_ID_LEN];
    size_t length;
    char data[LOG_CHUNK_SIZE];
} log_item_t;

typedef struct {
    log_item_t items[LOG_BUFFER_CAPACITY];
    size_t head;
    size_t tail;
    size_t count;
    int shutting_down;
    pthread_mutex_t mutex;
    pthread_cond_t not_empty;
    pthread_cond_t not_full;
} bounded_buffer_t;

typedef struct {
    command_kind_t kind;
    char container_id[CONTAINER_ID_LEN];
    char rootfs[PATH_MAX];
    char command[CHILD_COMMAND_LEN];
    unsigned long soft_limit_bytes;
    unsigned long hard_limit_bytes;
    int nice_value;
} control_request_t;

typedef struct {
    char id[CONTAINER_ID_LEN];
    char rootfs[PATH_MAX];
    char command[CHILD_COMMAND_LEN];
    int nice_value;
    int log_write_fd;
} child_config_t;

typedef struct {
    int server_fd;
    int monitor_fd;
    int should_stop;
    pthread_t logger_thread;
    bounded_buffer_t log_buffer;
    pthread_mutex_t metadata_lock;
    pthread_cond_t metadata_cond;
    container_record_t *containers;
} supervisor_ctx_t;

typedef struct {
    supervisor_ctx_t *ctx;
    container_record_t *record;
} producer_arg_t;

static volatile sig_atomic_t g_supervisor_sigchld = 0;
static volatile sig_atomic_t g_supervisor_stop = 0;
static volatile sig_atomic_t g_client_stop_request = 0;

static void usage(const char *prog)
{
    fprintf(stderr,
            "Usage:\n"
            "  %s supervisor <base-rootfs>\n"
            "  %s start <id> <container-rootfs> <command> [--soft-mib N] [--hard-mib N] [--nice N]\n"
            "  %s run <id> <container-rootfs> <command> [--soft-mib N] [--hard-mib N] [--nice N]\n"
            "  %s ps\n"
            "  %s logs <id>\n"
            "  %s stop <id>\n",
            prog, prog, prog, prog, prog, prog);
}

static int write_all(int fd, const void *buf, size_t len)
{
    const char *p = (const char *)buf;
    while (len > 0) {
        ssize_t n = write(fd, p, len);
        if (n < 0) {
            if (errno == EINTR)
                continue;
            return -1;
        }
        if (n == 0)
            return -1;
        p += (size_t)n;
        len -= (size_t)n;
    }
    return 0;
}

static int read_all(int fd, void *buf, size_t len)
{
    char *p = (char *)buf;
    while (len > 0) {
        ssize_t n = read(fd, p, len);
        if (n < 0) {
            if (errno == EINTR)
                continue;
            return -1;
        }
        if (n == 0)
            return 1;
        p += (size_t)n;
        len -= (size_t)n;
    }
    return 0;
}

static int send_response_header(int fd, int status)
{
    return write_all(fd, &status, sizeof(status));
}

static int send_response_text(int fd, int status, const char *text)
{
    if (send_response_header(fd, status) < 0)
        return -1;
    if (text && *text)
        return write_all(fd, text, strlen(text));
    return 0;
}

static int send_responsef(int fd, int status, const char *fmt, ...)
{
    char buffer[8192];
    va_list ap;

    va_start(ap, fmt);
    vsnprintf(buffer, sizeof(buffer), fmt, ap);
    va_end(ap);
    return send_response_text(fd, status, buffer);
}

static int parse_mib_flag(const char *flag,
                          const char *value,
                          unsigned long *target_bytes)
{
    char *end = NULL;
    unsigned long mib;

    errno = 0;
    mib = strtoul(value, &end, 10);
    if (errno != 0 || end == value || *end != '\0') {
        fprintf(stderr, "Invalid value for %s: %s\n", flag, value);
        return -1;
    }

    if (mib > ULONG_MAX / (1UL << 20)) {
        fprintf(stderr, "Value for %s is too large: %s\n", flag, value);
        return -1;
    }

    *target_bytes = mib * (1UL << 20);
    return 0;
}

static int parse_optional_flags(control_request_t *req,
                                int argc,
                                char *argv[],
                                int start_index)
{
    int i;

    for (i = start_index; i < argc; i += 2) {
        char *end = NULL;
        long nice_value;

        if (i + 1 >= argc) {
            fprintf(stderr, "Missing value for option: %s\n", argv[i]);
            return -1;
        }

        if (strcmp(argv[i], "--soft-mib") == 0) {
            if (parse_mib_flag("--soft-mib", argv[i + 1], &req->soft_limit_bytes) != 0)
                return -1;
            continue;
        }

        if (strcmp(argv[i], "--hard-mib") == 0) {
            if (parse_mib_flag("--hard-mib", argv[i + 1], &req->hard_limit_bytes) != 0)
                return -1;
            continue;
        }

        if (strcmp(argv[i], "--nice") == 0) {
            errno = 0;
            nice_value = strtol(argv[i + 1], &end, 10);
            if (errno != 0 || end == argv[i + 1] || *end != '\0' ||
                nice_value < -20 || nice_value > 19) {
                fprintf(stderr,
                        "Invalid value for --nice (expected -20..19): %s\n",
                        argv[i + 1]);
                return -1;
            }
            req->nice_value = (int)nice_value;
            continue;
        }

        fprintf(stderr, "Unknown option: %s\n", argv[i]);
        return -1;
    }

    if (req->soft_limit_bytes > req->hard_limit_bytes) {
        fprintf(stderr, "Invalid limits: soft limit cannot exceed hard limit\n");
        return -1;
    }

    return 0;
}

static const char *state_to_string(container_state_t state)
{
    switch (state) {
    case CONTAINER_STARTING:
        return "starting";
    case CONTAINER_RUNNING:
        return "running";
    case CONTAINER_STOPPED:
        return "stopped";
    case CONTAINER_KILLED:
        return "killed";
    case CONTAINER_EXITED:
        return "exited";
    default:
        return "unknown";
    }
}

static int bounded_buffer_init(bounded_buffer_t *buffer)
{
    int rc;

    memset(buffer, 0, sizeof(*buffer));

    rc = pthread_mutex_init(&buffer->mutex, NULL);
    if (rc != 0)
        return rc;

    rc = pthread_cond_init(&buffer->not_empty, NULL);
    if (rc != 0) {
        pthread_mutex_destroy(&buffer->mutex);
        return rc;
    }

    rc = pthread_cond_init(&buffer->not_full, NULL);
    if (rc != 0) {
        pthread_cond_destroy(&buffer->not_empty);
        pthread_mutex_destroy(&buffer->mutex);
        return rc;
    }

    return 0;
}

static void bounded_buffer_destroy(bounded_buffer_t *buffer)
{
    pthread_cond_destroy(&buffer->not_full);
    pthread_cond_destroy(&buffer->not_empty);
    pthread_mutex_destroy(&buffer->mutex);
}

static void bounded_buffer_begin_shutdown(bounded_buffer_t *buffer)
{
    pthread_mutex_lock(&buffer->mutex);
    buffer->shutting_down = 1;
    pthread_cond_broadcast(&buffer->not_empty);
    pthread_cond_broadcast(&buffer->not_full);
    pthread_mutex_unlock(&buffer->mutex);
}

static int bounded_buffer_push(bounded_buffer_t *buffer, const log_item_t *item)
{
    pthread_mutex_lock(&buffer->mutex);
    while (!buffer->shutting_down && buffer->count == LOG_BUFFER_CAPACITY)
        pthread_cond_wait(&buffer->not_full, &buffer->mutex);

    if (buffer->shutting_down) {
        pthread_mutex_unlock(&buffer->mutex);
        return -1;
    }

    buffer->items[buffer->tail] = *item;
    buffer->tail = (buffer->tail + 1U) % LOG_BUFFER_CAPACITY;
    buffer->count++;
    pthread_cond_signal(&buffer->not_empty);
    pthread_mutex_unlock(&buffer->mutex);
    return 0;
}

static int bounded_buffer_pop(bounded_buffer_t *buffer, log_item_t *item)
{
    pthread_mutex_lock(&buffer->mutex);
    while (buffer->count == 0 && !buffer->shutting_down)
        pthread_cond_wait(&buffer->not_empty, &buffer->mutex);

    if (buffer->count == 0 && buffer->shutting_down) {
        pthread_mutex_unlock(&buffer->mutex);
        return -1;
    }

    *item = buffer->items[buffer->head];
    buffer->head = (buffer->head + 1U) % LOG_BUFFER_CAPACITY;
    buffer->count--;
    pthread_cond_signal(&buffer->not_full);
    pthread_mutex_unlock(&buffer->mutex);
    return 0;
}

static int state_is_live(container_record_t *rec)
{
    return rec && !rec->finished &&
           (rec->state == CONTAINER_STARTING || rec->state == CONTAINER_RUNNING);
}

static container_record_t *find_container_locked(supervisor_ctx_t *ctx, const char *id)
{
    container_record_t *cur = ctx->containers;
    while (cur) {
        if (strncmp(cur->id, id, sizeof(cur->id)) == 0)
            return cur;
        cur = cur->next;
    }
    return NULL;
}

static container_record_t *find_container_by_pid_locked(supervisor_ctx_t *ctx, pid_t pid)
{
    container_record_t *cur = ctx->containers;
    while (cur) {
        if (cur->host_pid == pid)
            return cur;
        cur = cur->next;
    }
    return NULL;
}

static void format_time_string(time_t t, char *buf, size_t len)
{
    struct tm tmv;
    localtime_r(&t, &tmv);
    strftime(buf, len, "%Y-%m-%d %H:%M:%S", &tmv);
}

static void append_text(char *dst, size_t dst_size, const char *fmt, ...)
{
    size_t used = strlen(dst);
    va_list ap;

    if (used >= dst_size)
        return;

    va_start(ap, fmt);
    vsnprintf(dst + used, dst_size - used, fmt, ap);
    va_end(ap);
}

static int register_with_monitor(int monitor_fd,
                                 const char *container_id,
                                 pid_t host_pid,
                                 unsigned long soft_limit_bytes,
                                 unsigned long hard_limit_bytes)
{
    struct monitor_request req;

    if (monitor_fd < 0)
        return 0;

    memset(&req, 0, sizeof(req));
    req.pid = host_pid;
    req.soft_limit_bytes = soft_limit_bytes;
    req.hard_limit_bytes = hard_limit_bytes;
    strncpy(req.container_id, container_id, sizeof(req.container_id) - 1);

    if (ioctl(monitor_fd, MONITOR_REGISTER, &req) < 0)
        return -1;

    return 0;
}

static int unregister_from_monitor(int monitor_fd, const char *container_id, pid_t host_pid)
{
    struct monitor_request req;

    if (monitor_fd < 0)
        return 0;

    memset(&req, 0, sizeof(req));
    req.pid = host_pid;
    strncpy(req.container_id, container_id, sizeof(req.container_id) - 1);

    if (ioctl(monitor_fd, MONITOR_UNREGISTER, &req) < 0)
        return -1;

    return 0;
}

static void *logging_thread(void *arg)
{
    supervisor_ctx_t *ctx = (supervisor_ctx_t *)arg;
    log_item_t item;

    while (bounded_buffer_pop(&ctx->log_buffer, &item) == 0) {
        char path[PATH_MAX];
        int fd;

        snprintf(path, sizeof(path), "%s/%s.log", LOG_DIR, item.container_id);
        fd = open(path, O_CREAT | O_WRONLY | O_APPEND, 0644);
        if (fd < 0)
            continue;

        (void)write_all(fd, item.data, item.length);
        close(fd);
    }

    return NULL;
}

static void *log_producer_thread(void *arg)
{
    producer_arg_t *parg = (producer_arg_t *)arg;
    supervisor_ctx_t *ctx = parg->ctx;
    container_record_t *rec = parg->record;
    char id[CONTAINER_ID_LEN];
    int fd;

    strncpy(id, rec->id, sizeof(id) - 1);
    id[sizeof(id) - 1] = '\0';
    fd = rec->log_pipe_read_fd;
    free(parg);

    while (1) {
        log_item_t item;
        ssize_t n;

        memset(&item, 0, sizeof(item));
        n = read(fd, item.data, sizeof(item.data));
        if (n < 0) {
            if (errno == EINTR)
                continue;
            break;
        }
        if (n == 0)
            break;

        strncpy(item.container_id, id, sizeof(item.container_id) - 1);
        item.length = (size_t)n;
        if (bounded_buffer_push(&ctx->log_buffer, &item) != 0)
            break;
    }

    close(fd);
    rec->log_pipe_read_fd = -1;
    return NULL;
}

static void supervisor_signal_handler(int signo)
{
    if (signo == SIGCHLD)
        g_supervisor_sigchld = 1;
    else if (signo == SIGINT || signo == SIGTERM)
        g_supervisor_stop = 1;
}

static void client_signal_handler(int signo)
{
    (void)signo;
    g_client_stop_request = 1;
}

static int child_fn(void *arg)
{
    child_config_t *cfg = (child_config_t *)arg;

    if (cfg->nice_value != 0)
        (void)setpriority(PRIO_PROCESS, 0, cfg->nice_value);

    if (sethostname(cfg->id, strlen(cfg->id)) != 0) {
        perror("sethostname");
        return 1;
    }

    if (mount(NULL, "/", NULL, MS_REC | MS_PRIVATE, NULL) != 0) {
        perror("mount-private-root");
        return 1;
    }

    if (chroot(cfg->rootfs) != 0) {
        perror("chroot");
        return 1;
    }

    if (chdir("/") != 0) {
        perror("chdir");
        return 1;
    }

    mkdir("/proc", 0555);
    if (mount("proc", "/proc", "proc", 0, NULL) != 0) {
        perror("mount-proc");
        return 1;
    }

    if (dup2(cfg->log_write_fd, STDOUT_FILENO) < 0) {
        perror("dup2-stdout");
        return 1;
    }
    if (dup2(cfg->log_write_fd, STDERR_FILENO) < 0) {
        perror("dup2-stderr");
        return 1;
    }
    close(cfg->log_write_fd);

    execl(cfg->command, cfg->command, (char *)NULL);
    perror("exec");
    return 127;
}

static void reap_children(supervisor_ctx_t *ctx)
{
    int status;
    pid_t pid;

    while ((pid = waitpid(-1, &status, WNOHANG)) > 0) {
        container_record_t *rec;

        pthread_mutex_lock(&ctx->metadata_lock);
        rec = find_container_by_pid_locked(ctx, pid);
        if (rec) {
            rec->finished = 1;
            if (WIFEXITED(status)) {
                rec->exit_code = WEXITSTATUS(status);
                rec->exit_signal = 0;
                rec->state = rec->stop_requested ? CONTAINER_STOPPED : CONTAINER_EXITED;
            } else if (WIFSIGNALED(status)) {
                rec->exit_code = -1;
                rec->exit_signal = WTERMSIG(status);
                if (rec->stop_requested)
                    rec->state = CONTAINER_STOPPED;
                else if (WTERMSIG(status) == SIGKILL)
                    rec->state = CONTAINER_KILLED;
                else
                    rec->state = CONTAINER_EXITED;
            }
            pthread_cond_broadcast(&ctx->metadata_cond);
        }
        pthread_mutex_unlock(&ctx->metadata_lock);

        if (rec)
            (void)unregister_from_monitor(ctx->monitor_fd, rec->id, rec->host_pid);
    }
}

static int validate_start_request_locked(supervisor_ctx_t *ctx,
                                         const control_request_t *req,
                                         char *err,
                                         size_t err_len)
{
    container_record_t *cur;
    struct stat st;

    if (strlen(req->container_id) == 0 || strlen(req->container_id) >= CONTAINER_ID_LEN) {
        snprintf(err, err_len, "invalid container id\n");
        return -1;
    }

    if (req->command[0] != '/') {
        snprintf(err, err_len, "command must be an absolute path inside the container rootfs\n");
        return -1;
    }

    if (stat(req->rootfs, &st) != 0 || !S_ISDIR(st.st_mode)) {
        snprintf(err, err_len, "container rootfs does not exist: %s\n", req->rootfs);
        return -1;
    }

    cur = ctx->containers;
    while (cur) {
        if (strncmp(cur->id, req->container_id, sizeof(cur->id)) == 0 && !cur->finished) {
            snprintf(err, err_len, "container id already in use: %s\n", req->container_id);
            return -1;
        }
        if (strcmp(cur->rootfs, req->rootfs) == 0 && state_is_live(cur)) {
            snprintf(err, err_len, "rootfs already in use by live container: %s\n", req->rootfs);
            return -1;
        }
        cur = cur->next;
    }

    return 0;
}

static int start_container_locked(supervisor_ctx_t *ctx,
                                  const control_request_t *req,
                                  char *msg,
                                  size_t msg_len)
{
    container_record_t *rec = NULL;
    child_config_t *cfg = NULL;
    producer_arg_t *parg = NULL;
    int pipefd[2] = {-1, -1};
    pid_t pid;

    if (mkdir(LOG_DIR, 0755) != 0 && errno != EEXIST) {
        snprintf(msg, msg_len, "failed to create log dir: %s\n", strerror(errno));
        return -1;
    }

    if (validate_start_request_locked(ctx, req, msg, msg_len) != 0)
        return -1;

    rec = calloc(1, sizeof(*rec));
    cfg = calloc(1, sizeof(*cfg));
    parg = calloc(1, sizeof(*parg));
    if (!rec || !cfg || !parg) {
        snprintf(msg, msg_len, "memory allocation failed\n");
        free(rec);
        free(cfg);
        free(parg);
        return -1;
    }

    rec->child_stack = malloc(STACK_SIZE);
    if (!rec->child_stack) {
        snprintf(msg, msg_len, "failed to allocate clone stack\n");
        free(rec);
        free(cfg);
        free(parg);
        return -1;
    }

    if (pipe(pipefd) != 0) {
        snprintf(msg, msg_len, "pipe failed: %s\n", strerror(errno));
        free(rec->child_stack);
        free(rec);
        free(cfg);
        free(parg);
        return -1;
    }

    strncpy(rec->id, req->container_id, sizeof(rec->id) - 1);
    strncpy(rec->rootfs, req->rootfs, sizeof(rec->rootfs) - 1);
    strncpy(rec->command, req->command, sizeof(rec->command) - 1);
    snprintf(rec->log_path, sizeof(rec->log_path), "%s/%s.log", LOG_DIR, rec->id);
    rec->started_at = time(NULL);
    rec->state = CONTAINER_STARTING;
    rec->soft_limit_bytes = req->soft_limit_bytes;
    rec->hard_limit_bytes = req->hard_limit_bytes;
    rec->nice_value = req->nice_value;
    rec->log_pipe_read_fd = pipefd[0];

    strncpy(cfg->id, rec->id, sizeof(cfg->id) - 1);
    strncpy(cfg->rootfs, rec->rootfs, sizeof(cfg->rootfs) - 1);
    strncpy(cfg->command, rec->command, sizeof(cfg->command) - 1);
    cfg->nice_value = rec->nice_value;
    cfg->log_write_fd = pipefd[1];

    pid = clone(child_fn,
                (char *)rec->child_stack + STACK_SIZE,
                CLONE_NEWPID | CLONE_NEWUTS | CLONE_NEWNS | SIGCHLD,
                cfg);
    if (pid < 0) {
        snprintf(msg, msg_len, "clone failed: %s\n", strerror(errno));
        close(pipefd[0]);
        close(pipefd[1]);
        free(rec->child_stack);
        free(rec);
        free(cfg);
        free(parg);
        return -1;
    }

    close(pipefd[1]);
    rec->host_pid = pid;
    rec->state = CONTAINER_RUNNING;
    rec->next = ctx->containers;
    ctx->containers = rec;

    if (register_with_monitor(ctx->monitor_fd,
                              rec->id,
                              rec->host_pid,
                              rec->soft_limit_bytes,
                              rec->hard_limit_bytes) != 0) {
        snprintf(msg, msg_len, "warning: failed to register with monitor: %s\n", strerror(errno));
    } else {
        snprintf(msg, msg_len, "container %s started with pid %d\n", rec->id, rec->host_pid);
    }

    parg->ctx = ctx;
    parg->record = rec;
    if (pthread_create(&rec->producer_thread, NULL, log_producer_thread, parg) != 0) {
        close(rec->log_pipe_read_fd);
        rec->log_pipe_read_fd = -1;
        rec->producer_started = 0;
        free(parg);
        snprintf(msg, msg_len, "container %s started, but log thread creation failed\n", rec->id);
    } else {
        rec->producer_started = 1;
    }

    free(cfg);
    return 0;
}

static int build_ps_output_locked(supervisor_ctx_t *ctx, char *buf, size_t buf_len)
{
    container_record_t *cur = ctx->containers;

    buf[0] = '\0';
    append_text(buf, buf_len, "ID\tPID\tSTATE\tSTARTED\tSOFT_MIB\tHARD_MIB\tEXIT\tROOTFS\n");
    while (cur) {
        char ts[64];
        char exit_desc[64];

        format_time_string(cur->started_at, ts, sizeof(ts));
        if (!cur->finished)
            snprintf(exit_desc, sizeof(exit_desc), "-");
        else if (cur->exit_signal)
            snprintf(exit_desc, sizeof(exit_desc), "sig:%d", cur->exit_signal);
        else
            snprintf(exit_desc, sizeof(exit_desc), "code:%d", cur->exit_code);

        append_text(buf,
                    buf_len,
                    "%s\t%d\t%s\t%s\t%lu\t%lu\t%s\t%s\n",
                    cur->id,
                    cur->host_pid,
                    state_to_string(cur->state),
                    ts,
                    cur->soft_limit_bytes >> 20,
                    cur->hard_limit_bytes >> 20,
                    exit_desc,
                    cur->rootfs);
        cur = cur->next;
    }
    return 0;
}

static int handle_stop_locked(supervisor_ctx_t *ctx,
                              const char *id,
                              char *msg,
                              size_t msg_len)
{
    container_record_t *rec = find_container_locked(ctx, id);

    if (!rec) {
        snprintf(msg, msg_len, "no such container: %s\n", id);
        return -1;
    }
    if (rec->finished) {
        snprintf(msg, msg_len, "container %s already finished\n", id);
        return 0;
    }

    rec->stop_requested = 1;
    if (kill(rec->host_pid, SIGTERM) != 0) {
        snprintf(msg, msg_len, "failed to stop %s: %s\n", id, strerror(errno));
        return -1;
    }

    snprintf(msg, msg_len, "stop signal sent to %s (pid=%d)\n", id, rec->host_pid);
    return 0;
}

static void cleanup_supervisor(supervisor_ctx_t *ctx)
{
    container_record_t *cur;

    pthread_mutex_lock(&ctx->metadata_lock);
    cur = ctx->containers;
    while (cur) {
        if (!cur->finished) {
            cur->stop_requested = 1;
            kill(cur->host_pid, SIGTERM);
        }
        cur = cur->next;
    }
    pthread_mutex_unlock(&ctx->metadata_lock);

    for (;;) {
        int live = 0;
        pthread_mutex_lock(&ctx->metadata_lock);
        cur = ctx->containers;
        while (cur) {
            if (!cur->finished)
                live = 1;
            cur = cur->next;
        }
        pthread_mutex_unlock(&ctx->metadata_lock);
        if (!live)
            break;
        reap_children(ctx);
        usleep(100000);
    }

    pthread_mutex_lock(&ctx->metadata_lock);
    cur = ctx->containers;
    while (cur) {
        if (cur->producer_started)
            pthread_join(cur->producer_thread, NULL);
        cur = cur->next;
    }
    pthread_mutex_unlock(&ctx->metadata_lock);

    bounded_buffer_begin_shutdown(&ctx->log_buffer);
    pthread_join(ctx->logger_thread, NULL);

    if (ctx->monitor_fd >= 0)
        close(ctx->monitor_fd);
    if (ctx->server_fd >= 0)
        close(ctx->server_fd);
    unlink(CONTROL_PATH);

    cur = ctx->containers;
    while (cur) {
        container_record_t *next = cur->next;
        if (cur->log_pipe_read_fd >= 0)
            close(cur->log_pipe_read_fd);
        free(cur->child_stack);
        free(cur);
        cur = next;
    }

    bounded_buffer_destroy(&ctx->log_buffer);
    pthread_cond_destroy(&ctx->metadata_cond);
    pthread_mutex_destroy(&ctx->metadata_lock);
}

static void handle_client(supervisor_ctx_t *ctx, int cfd)
{
    control_request_t req;
    char response[16384];
    int rc;

    memset(&req, 0, sizeof(req));
    rc = read_all(cfd, &req, sizeof(req));
    if (rc != 0) {
        (void)send_response_text(cfd, 1, "failed to read control request\n");
        return;
    }

    reap_children(ctx);

    switch (req.kind) {
    case CMD_START:
        pthread_mutex_lock(&ctx->metadata_lock);
        rc = start_container_locked(ctx, &req, response, sizeof(response));
        pthread_mutex_unlock(&ctx->metadata_lock);
        (void)send_response_text(cfd, rc == 0 ? 0 : 1, response);
        break;

    case CMD_RUN: {
        container_record_t *rec;
        int exit_status;

        pthread_mutex_lock(&ctx->metadata_lock);
        rc = start_container_locked(ctx, &req, response, sizeof(response));
        pthread_mutex_unlock(&ctx->metadata_lock);
        if (rc != 0) {
            (void)send_response_text(cfd, 1, response);
            break;
        }

        for (;;) {
            pthread_mutex_lock(&ctx->metadata_lock);
            rec = find_container_locked(ctx, req.container_id);
            if (rec && rec->finished)
                break;
            pthread_mutex_unlock(&ctx->metadata_lock);
            reap_children(ctx);
            usleep(100000);
        }

        if (!rec) {
            pthread_mutex_unlock(&ctx->metadata_lock);
            (void)send_response_text(cfd, 1, "container disappeared from metadata\n");
            break;
        }

        if (rec->exit_signal)
            exit_status = 128 + rec->exit_signal;
        else
            exit_status = rec->exit_code;

        snprintf(response,
                 sizeof(response),
                 "container %s finished state=%s exit_code=%d exit_signal=%d\n",
                 rec->id,
                 state_to_string(rec->state),
                 rec->exit_code,
                 rec->exit_signal);
        pthread_mutex_unlock(&ctx->metadata_lock);
        (void)send_response_text(cfd, exit_status, response);
        break;
    }

    case CMD_PS:
        pthread_mutex_lock(&ctx->metadata_lock);
        build_ps_output_locked(ctx, response, sizeof(response));
        pthread_mutex_unlock(&ctx->metadata_lock);
        (void)send_response_text(cfd, 0, response);
        break;

    case CMD_LOGS: {
        container_record_t *rec;
        int fd;
        ssize_t n;
        char chunk[4096];

        pthread_mutex_lock(&ctx->metadata_lock);
        rec = find_container_locked(ctx, req.container_id);
        if (!rec) {
            pthread_mutex_unlock(&ctx->metadata_lock);
            (void)send_responsef(cfd, 1, "no such container: %s\n", req.container_id);
            break;
        }
        strncpy(response, rec->log_path, sizeof(response) - 1);
        response[sizeof(response) - 1] = '\0';
        pthread_mutex_unlock(&ctx->metadata_lock);

        fd = open(response, O_RDONLY);
        if (fd < 0) {
            (void)send_responsef(cfd, 1, "log file not available yet for %s\n", req.container_id);
            break;
        }

        if (send_response_header(cfd, 0) < 0) {
            close(fd);
            break;
        }
        while ((n = read(fd, chunk, sizeof(chunk))) > 0) {
            if (write_all(cfd, chunk, (size_t)n) < 0)
                break;
        }
        close(fd);
        break;
    }

    case CMD_STOP:
        pthread_mutex_lock(&ctx->metadata_lock);
        rc = handle_stop_locked(ctx, req.container_id, response, sizeof(response));
        pthread_mutex_unlock(&ctx->metadata_lock);
        (void)send_response_text(cfd, rc == 0 ? 0 : 1, response);
        break;

    default:
        (void)send_response_text(cfd, 1, "unsupported command\n");
        break;
    }
}

static int run_supervisor(const char *rootfs)
{
    supervisor_ctx_t ctx;
    struct sockaddr_un addr;
    struct sigaction sa;
    int optval = 1;
    int rc;

    memset(&ctx, 0, sizeof(ctx));
    ctx.server_fd = -1;
    ctx.monitor_fd = -1;

    rc = pthread_mutex_init(&ctx.metadata_lock, NULL);
    if (rc != 0) {
        errno = rc;
        perror("pthread_mutex_init");
        return 1;
    }

    rc = pthread_cond_init(&ctx.metadata_cond, NULL);
    if (rc != 0) {
        errno = rc;
        perror("pthread_cond_init");
        pthread_mutex_destroy(&ctx.metadata_lock);
        return 1;
    }

    rc = bounded_buffer_init(&ctx.log_buffer);
    if (rc != 0) {
        errno = rc;
        perror("bounded_buffer_init");
        pthread_cond_destroy(&ctx.metadata_cond);
        pthread_mutex_destroy(&ctx.metadata_lock);
        return 1;
    }

    if (mkdir(LOG_DIR, 0755) != 0 && errno != EEXIST) {
        perror("mkdir logs");
        cleanup_supervisor(&ctx);
        return 1;
    }

    ctx.monitor_fd = open("/dev/container_monitor", O_RDWR);
    if (ctx.monitor_fd < 0) {
        fprintf(stderr,
                "warning: could not open /dev/container_monitor (%s); continuing without kernel monitor\n",
                strerror(errno));
    }

    ctx.server_fd = socket(AF_UNIX, SOCK_STREAM, 0);
    if (ctx.server_fd < 0) {
        perror("socket");
        cleanup_supervisor(&ctx);
        return 1;
    }

    (void)setsockopt(ctx.server_fd, SOL_SOCKET, SO_REUSEADDR, &optval, sizeof(optval));
    unlink(CONTROL_PATH);
    memset(&addr, 0, sizeof(addr));
    addr.sun_family = AF_UNIX;
    strncpy(addr.sun_path, CONTROL_PATH, sizeof(addr.sun_path) - 1);

    if (bind(ctx.server_fd, (struct sockaddr *)&addr, sizeof(addr)) != 0) {
        perror("bind");
        cleanup_supervisor(&ctx);
        return 1;
    }
    if (listen(ctx.server_fd, 32) != 0) {
        perror("listen");
        cleanup_supervisor(&ctx);
        return 1;
    }

    memset(&sa, 0, sizeof(sa));
    sa.sa_handler = supervisor_signal_handler;
    sigemptyset(&sa.sa_mask);
    sigaction(SIGCHLD, &sa, NULL);
    sigaction(SIGINT, &sa, NULL);
    sigaction(SIGTERM, &sa, NULL);

    if (pthread_create(&ctx.logger_thread, NULL, logging_thread, &ctx) != 0) {
        perror("pthread_create logger");
        cleanup_supervisor(&ctx);
        return 1;
    }

    fprintf(stderr,
            "Supervisor ready. base-rootfs=%s control=%s\n",
            rootfs,
            CONTROL_PATH);

    while (!ctx.should_stop) {
        int cfd;

        if (g_supervisor_sigchld) {
            g_supervisor_sigchld = 0;
            reap_children(&ctx);
        }
        if (g_supervisor_stop) {
            ctx.should_stop = 1;
            break;
        }

        cfd = accept(ctx.server_fd, NULL, NULL);
        if (cfd < 0) {
            if (errno == EINTR)
                continue;
            perror("accept");
            break;
        }

        handle_client(&ctx, cfd);
        close(cfd);
    }

    cleanup_supervisor(&ctx);
    return 0;
}

static int send_stop_request_now(const char *container_id)
{
    control_request_t req;
    int fd;
    struct sockaddr_un addr;
    int status = 1;
    char discard[256];

    memset(&req, 0, sizeof(req));
    req.kind = CMD_STOP;
    strncpy(req.container_id, container_id, sizeof(req.container_id) - 1);

    fd = socket(AF_UNIX, SOCK_STREAM, 0);
    if (fd < 0)
        return -1;

    memset(&addr, 0, sizeof(addr));
    addr.sun_family = AF_UNIX;
    strncpy(addr.sun_path, CONTROL_PATH, sizeof(addr.sun_path) - 1);
    if (connect(fd, (struct sockaddr *)&addr, sizeof(addr)) != 0) {
        close(fd);
        return -1;
    }
    if (write_all(fd, &req, sizeof(req)) != 0) {
        close(fd);
        return -1;
    }
    shutdown(fd, SHUT_WR);
    if (read_all(fd, &status, sizeof(status)) == 0) {
        while (read(fd, discard, sizeof(discard)) > 0) {
        }
    }
    close(fd);
    return status;
}

static int send_control_request(const control_request_t *req)
{
    int fd;
    struct sockaddr_un addr;
    int status = 1;
    char buffer[4096];
    ssize_t n;

    fd = socket(AF_UNIX, SOCK_STREAM, 0);
    if (fd < 0) {
        perror("socket");
        return 1;
    }

    memset(&addr, 0, sizeof(addr));
    addr.sun_family = AF_UNIX;
    strncpy(addr.sun_path, CONTROL_PATH, sizeof(addr.sun_path) - 1);

    if (connect(fd, (struct sockaddr *)&addr, sizeof(addr)) != 0) {
        perror("connect");
        close(fd);
        return 1;
    }

    if (write_all(fd, req, sizeof(*req)) != 0) {
        perror("write request");
        close(fd);
        return 1;
    }
    shutdown(fd, SHUT_WR);

    if (req->kind == CMD_RUN) {
        struct sigaction sa;
        int forwarded = 0;

        memset(&sa, 0, sizeof(sa));
        sa.sa_handler = client_signal_handler;
        sigemptyset(&sa.sa_mask);
        sigaction(SIGINT, &sa, NULL);
        sigaction(SIGTERM, &sa, NULL);

        while (1) {
            struct pollfd pfd;
            int prc;

            pfd.fd = fd;
            pfd.events = POLLIN;
            prc = poll(&pfd, 1, 250);
            if (prc < 0) {
                if (errno == EINTR)
                    continue;
                perror("poll");
                close(fd);
                return 1;
            }

            if (g_client_stop_request && !forwarded) {
                forwarded = 1;
                (void)send_stop_request_now(req->container_id);
            }

            if (prc > 0 && (pfd.revents & POLLIN))
                break;
        }
    }

    if (read_all(fd, &status, sizeof(status)) != 0) {
        fprintf(stderr, "failed to read response header from supervisor\n");
        close(fd);
        return 1;
    }

    while ((n = read(fd, buffer, sizeof(buffer))) > 0) {
        FILE *stream = (status == 0 || req->kind == CMD_RUN) ? stdout : stderr;
        fwrite(buffer, 1, (size_t)n, stream);
    }

    close(fd);
    return status;
}

static int cmd_start(int argc, char *argv[])
{
    control_request_t req;

    if (argc < 5) {
        fprintf(stderr,
                "Usage: %s start <id> <container-rootfs> <command> [--soft-mib N] [--hard-mib N] [--nice N]\n",
                argv[0]);
        return 1;
    }

    memset(&req, 0, sizeof(req));
    req.kind = CMD_START;
    strncpy(req.container_id, argv[2], sizeof(req.container_id) - 1);
    strncpy(req.rootfs, argv[3], sizeof(req.rootfs) - 1);
    strncpy(req.command, argv[4], sizeof(req.command) - 1);
    req.soft_limit_bytes = DEFAULT_SOFT_LIMIT;
    req.hard_limit_bytes = DEFAULT_HARD_LIMIT;

    if (parse_optional_flags(&req, argc, argv, 5) != 0)
        return 1;

    return send_control_request(&req);
}

static int cmd_run(int argc, char *argv[])
{
    control_request_t req;

    if (argc < 5) {
        fprintf(stderr,
                "Usage: %s run <id> <container-rootfs> <command> [--soft-mib N] [--hard-mib N] [--nice N]\n",
                argv[0]);
        return 1;
    }

    memset(&req, 0, sizeof(req));
    req.kind = CMD_RUN;
    strncpy(req.container_id, argv[2], sizeof(req.container_id) - 1);
    strncpy(req.rootfs, argv[3], sizeof(req.rootfs) - 1);
    strncpy(req.command, argv[4], sizeof(req.command) - 1);
    req.soft_limit_bytes = DEFAULT_SOFT_LIMIT;
    req.hard_limit_bytes = DEFAULT_HARD_LIMIT;

    if (parse_optional_flags(&req, argc, argv, 5) != 0)
        return 1;

    return send_control_request(&req);
}

static int cmd_ps(void)
{
    control_request_t req;
    memset(&req, 0, sizeof(req));
    req.kind = CMD_PS;
    return send_control_request(&req);
}

static int cmd_logs(int argc, char *argv[])
{
    control_request_t req;

    if (argc < 3) {
        fprintf(stderr, "Usage: %s logs <id>\n", argv[0]);
        return 1;
    }

    memset(&req, 0, sizeof(req));
    req.kind = CMD_LOGS;
    strncpy(req.container_id, argv[2], sizeof(req.container_id) - 1);

    return send_control_request(&req);
}

static int cmd_stop(int argc, char *argv[])
{
    control_request_t req;

    if (argc < 3) {
        fprintf(stderr, "Usage: %s stop <id>\n", argv[0]);
        return 1;
    }

    memset(&req, 0, sizeof(req));
    req.kind = CMD_STOP;
    strncpy(req.container_id, argv[2], sizeof(req.container_id) - 1);

    return send_control_request(&req);
}

int main(int argc, char *argv[])
{
    if (argc < 2) {
        usage(argv[0]);
        return 1;
    }

    if (strcmp(argv[1], "supervisor") == 0) {
        if (argc < 3) {
            fprintf(stderr, "Usage: %s supervisor <base-rootfs>\n", argv[0]);
            return 1;
        }
        return run_supervisor(argv[2]);
    }

    if (strcmp(argv[1], "start") == 0)
        return cmd_start(argc, argv);

    if (strcmp(argv[1], "run") == 0)
        return cmd_run(argc, argv);

    if (strcmp(argv[1], "ps") == 0)
        return cmd_ps();

    if (strcmp(argv[1], "logs") == 0)
        return cmd_logs(argc, argv);

    if (strcmp(argv[1], "stop") == 0)
        return cmd_stop(argc, argv);

    usage(argv[0]);
    return 1;
}
