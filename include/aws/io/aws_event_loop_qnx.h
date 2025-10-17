#ifndef AWS_EVENT_LOOP_QNX_POLL_H
#define AWS_EVENT_LOOP_QNX_POLL_H

#include <aws/io/event_loop.h>
#include <poll.h>
#include <fcntl.h>
#include <unistd.h>
#include <aws/common/mutex.h>
#include <aws/common/thread.h>
#include <aws/common/clock.h>
#include <aws/common/task_scheduler.h>
#include <aws/io/io.h>
#include <sys/iomsg.h>
#include <sys/netmgr.h>
#include <sys/neutrino.h>
#include <sys/select.h>
#include <sys/siginfo.h>
#include <sys/types.h>

enum event_thread_state {
    EVENT_THREAD_STATE_READY_TO_RUN,
    EVENT_THREAD_STATE_RUNNING,
    EVENT_THREAD_STATE_STOPPING,
};

struct handle_data {
    uint32_t handle_id;
    // File-descriptor associated to this handle_data
    int fd;
    // Read or Write event conditions already existing at the time of event arming
    int pre_arm_conditions;
    // QNX event that will be armed for the file-descriptor
    struct sigevent qnx_event;
    // Pointer to aws_io_handle that is provided to the event-loop user
    struct aws_io_handle *owner;
    // Pointer to the event loop that manages this handle_data object
    struct aws_event_loop *event_loop;
    // The callback to be called when the event is triggered
    aws_event_loop_on_event_fn *on_event;
    // Data that will be passed to the callback
    void *on_event_user_data;
    // aws_io_event_types this handle should be subscribed to
    int events_subscribed;
    // aws_io_event_types received during current loop of the event-thread
    int events_this_loop;
    // Either AWS_IO_EVENT_TYPE_WRITABLE or AWS_IO_EVENT_TYPE_READABLE
    int aws_io_event_type;
    // The current subscription state
    enum { HANDLE_STATE_SUBSCRIBING, HANDLE_STATE_SUBSCRIBED, HANDLE_STATE_UNSUBSCRIBED } state;
    // The task that will be created on subscribe - the subscription will actually be carried out in this task
    struct aws_task subscribe_task;
    // The task that will be created on unsubscribe - the subscription will actually be removed in this task
    struct aws_task unsubscribe_task;
};

// Implementation of the abstract aws_event_loop for QNX based on ionotify
struct qnx_poll_loop {
    uint32_t next_handle_id;
    // QNX channel used for ionotify
    int qnx_channel;
    // QNX connection used for ionotify
    int qnx_connection;
    // Event associated to the event_loop timer. The timer will be set accordingly to the next task to be executed.
    struct sigevent qnx_timer_event;
    // List of handles associated to each file descriptor
    struct aws_array_list handle_datas;
    // Scheduler that holds the tasks to be executed
    struct aws_task_scheduler scheduler;
    // The thread that is actually executing this event_loop
    struct aws_thread thread;
    /* running_thread_id is NULL if the event loop thread is stopped or points-to the thread_id of the thread running
     * the event loop (either thread_created_on or thread_joined_to). Atomic because of concurrent writes (e.g.,
     * run/stop) and reads (e.g., is_event_loop_thread).
     * An aws_thread_id_t variable itself cannot be atomic because it is an opaque type that is platform-dependent. */
    struct aws_atomic_var running_thread_id;
    // Options provided by the user for creating the event_loop thread
    struct aws_thread_options thread_options;
    // Current state of the event_loop
    enum event_thread_state current_state;

    /* cross_thread_data holds things that must be communicated across threads.
     * When the event-thread is running, the mutex must be locked while anyone touches anything in cross_thread_data.
     * If this data is modified outside the thread, the thread is signaled via activity on a pipe. */
    struct {
        // lock to be locked for protecting the whole chross_thread_data
        struct aws_mutex mutex;
        // Whether thread has been signaled about changes to cross_thread_data
        bool thread_signaled;
        // List of tasks to be added to the scheduler
        struct aws_linked_list tasks_to_schedule;
        // Next state to reach - used during initialization and shutdown
        enum event_thread_state target_state;
    } cross_thread_data;
};

#endif /* AWS_EVENT_LOOP_QNX_POLL_H */
