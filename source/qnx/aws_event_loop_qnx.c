 #include <aws/common/clock.h>
 #include <aws/common/mutex.h>
 #include <aws/common/task_scheduler.h>
 #include <aws/common/thread.h>
 #include <aws/io/aws_event_loop_qnx.h>
 #include <aws/io/io.h>
 #include <aws/io/logging.h>
 #include <stdlib.h>
 
 #define PULSE_FD_EVENT_CODE _PULSE_CODE_MINAVAIL
 #define PULSE_FD_CROSS_THREAD_CODE (_PULSE_CODE_MINAVAIL + 1)
 #define EVENT_LOOP_TIMER_PULSE_CODE (_PULSE_CODE_MINAVAIL + 2)
 
 enum {
   DEFAULT_TIMEOUT_SEC = 30, /* Max timeout per loop of the event-thread */
   MAX_EVENTS = 500,         /* Max number of events to process per loop of the event-thread */
 };
 
 static void s_destroy(struct aws_event_loop* event_loop);
 static int s_run(struct aws_event_loop* event_loop);
 static int s_stop(struct aws_event_loop* event_loop);
 static int s_wait_for_stop_completion(struct aws_event_loop* event_loop);
 static void s_schedule_task_now(struct aws_event_loop* event_loop, struct aws_task* task);
 static void s_schedule_task_future(struct aws_event_loop* event_loop, struct aws_task* task, uint64_t run_at_nanos);
 static int s_subscribe_to_io_events(struct aws_event_loop* event_loop, struct aws_io_handle* handle, int events,
                                     aws_event_loop_on_event_fn* on_event, void* user_data);
 static int s_unsubscribe_from_io_events(struct aws_event_loop* event_loop, struct aws_io_handle* handle);
 static bool s_is_event_thread(struct aws_event_loop* event_loop);
 static void s_cancel_task(struct aws_event_loop* event_loop, struct aws_task* task);
 
 static void aws_event_loop_thread(void* user_data);
 
 static struct aws_event_loop_vtable s_qnx_poll_vtable = {
     .destroy = s_destroy,
     .run = s_run,
     .stop = s_stop,
     .wait_for_stop_completion = s_wait_for_stop_completion,
     .schedule_task_now = s_schedule_task_now,
     .schedule_task_future = s_schedule_task_future,
     .cancel_task = s_cancel_task,
     .subscribe_to_io_events = s_subscribe_to_io_events,
     .unsubscribe_from_io_events = s_unsubscribe_from_io_events,
     .is_on_callers_thread = s_is_event_thread,
 };
 
 /** Find a handle_data in the list of handle_datas by its ID.
 * We cannot use a pointer since the data associated to the QNX pulse is 32bit long
 * thus not suitable for holding a pointer */
 static struct handle_data* s_find_handle_by_id(struct qnx_poll_loop* impl, uint32_t id) {
   int nfds = aws_array_list_length(&impl->handle_datas);
   for (int i = 0; i < nfds; ++i) {
     struct handle_data* hd = NULL;
     aws_array_list_get_at(&impl->handle_datas, &hd, i);
     if (hd && hd->handle_id == id) {
       return hd;
     }
   }
   return NULL;
 }
 
 struct aws_event_loop* aws_event_loop_new_default_with_options(struct aws_allocator* alloc,
                                                                const struct aws_event_loop_options* options) {
   AWS_ASSERT(alloc);
   AWS_ASSERT(options);
   AWS_ASSERT(options->clock);
 
   struct aws_event_loop* event_loop = aws_mem_acquire(alloc, sizeof(struct aws_event_loop));
   aws_event_loop_init_base(event_loop, alloc, options->clock);
   if (!event_loop) {
     AWS_LOGF_FATAL(AWS_LS_IO_EVENT_LOOP, "aws_event_loop_init_base failed");
     return NULL;
   }
 
   struct qnx_poll_loop* impl = aws_mem_calloc(alloc, 1, sizeof(struct qnx_poll_loop));
   if (!impl) {
     AWS_LOGF_FATAL(AWS_LS_IO_EVENT_LOOP, "aws_mem_calloc failed");
     aws_mem_release(alloc, event_loop);
     return NULL;
   }
   impl->qnx_channel = -1;
   impl->qnx_connection = -1;
 
   
   impl->next_handle_id = 1; /* 0 reserved as "invalid" */
 
   if (options->thread_options) {
     impl->thread_options = *options->thread_options;
   } else {
     impl->thread_options = *aws_default_thread_options();
   }
 
   aws_atomic_init_ptr(&impl->running_thread_id, NULL);
   aws_thread_init(&impl->thread, alloc);
 
   impl->cross_thread_data.target_state = EVENT_THREAD_STATE_READY_TO_RUN;
   impl->current_state = EVENT_THREAD_STATE_READY_TO_RUN;
 
   aws_array_list_init_dynamic(&impl->handle_datas, alloc, 100, sizeof(struct handle_data*));
 
   int mutex_init_err = aws_mutex_init(&impl->cross_thread_data.mutex);
   if (mutex_init_err) {
   } else {
   }
   impl->cross_thread_data.thread_signaled = false;
   aws_linked_list_init(&impl->cross_thread_data.tasks_to_schedule);
   impl->cross_thread_data.target_state = EVENT_THREAD_STATE_READY_TO_RUN;
 
   if (aws_task_scheduler_init(&impl->scheduler, alloc) != 0) {
     AWS_LOGF_FATAL(AWS_LS_IO_EVENT_LOOP, "aws_task_scheduler_init failed");
     return NULL;
   };
 
   impl->current_state = EVENT_THREAD_STATE_READY_TO_RUN;
 
   event_loop->vtable = &s_qnx_poll_vtable;
   event_loop->alloc = alloc;
   event_loop->clock = options->clock;
   event_loop->impl_data = impl;
 
   return event_loop;
 }
 
 static void s_destroy(struct aws_event_loop* event_loop) {
   struct qnx_poll_loop* impl = event_loop->impl_data;
   aws_event_loop_stop(event_loop);
   s_wait_for_stop_completion(event_loop);
 
   aws_thread_id_t thread_joined_to = aws_thread_current_thread_id();
   aws_atomic_store_ptr(&impl->running_thread_id, &thread_joined_to);
   aws_task_scheduler_clean_up(&impl->scheduler);
   while (!aws_linked_list_empty(&impl->cross_thread_data.tasks_to_schedule)) {
     struct aws_linked_list_node* node = aws_linked_list_pop_front(&impl->cross_thread_data.tasks_to_schedule);
     struct aws_task* task = AWS_CONTAINER_OF(node, struct aws_task, node);
     task->fn(task, task->arg, AWS_TASK_STATUS_CANCELED);
   }
 
   if (impl->qnx_connection != -1) {
     ConnectDetach(impl->qnx_connection);
     impl->qnx_connection = -1;
   }
   if (impl->qnx_channel != -1) {
     ChannelDestroy(impl->qnx_channel);
     impl->qnx_channel = -1;
   }
 
   aws_mutex_clean_up(&impl->cross_thread_data.mutex);
   aws_array_list_clear(&impl->handle_datas);
   aws_array_list_clean_up(&impl->handle_datas);
   aws_thread_clean_up(&impl->thread);
   aws_mem_release(event_loop->alloc, impl);
   aws_event_loop_clean_up_base(event_loop);
   aws_mem_release(event_loop->alloc, event_loop);
 }
 
 void signal_cross_thread_data_changed(struct aws_event_loop* event_loop) {
   struct qnx_poll_loop* impl = event_loop->impl_data;
   MsgSendPulse(impl->qnx_connection, SIGEV_PULSE_PRIO_INHERIT, PULSE_FD_CROSS_THREAD_CODE, 0);
 }
 
 static int s_run(struct aws_event_loop* event_loop) {
   struct qnx_poll_loop* impl = event_loop->impl_data;
   AWS_ASSERT(impl->cross_thread_data.state == EVENT_THREAD_STATE_READY_TO_RUN);
   AWS_ASSERT(impl->thread_data.state == EVENT_THREAD_STATE_READY_TO_RUN);
   impl->cross_thread_data.target_state = EVENT_THREAD_STATE_RUNNING;
 
   aws_thread_increment_unjoined_count();
   if (aws_thread_launch(&impl->thread, aws_event_loop_thread, (void*)event_loop, &impl->thread_options) != 0) {
     AWS_LOGF_FATAL(AWS_LS_IO_EVENT_LOOP, "aws_thread_launch failed");
     aws_thread_decrement_unjoined_count();
     return AWS_OP_ERR;
   }
 
   return AWS_OP_SUCCESS;
 }
 
 static int s_stop(struct aws_event_loop* event_loop) {
   struct qnx_poll_loop* impl = event_loop->impl_data;
 
   aws_atomic_store_ptr(&impl->running_thread_id, 0);
   bool signal_thread = false;
 
   {
     aws_mutex_lock(&impl->cross_thread_data.mutex);
     if (impl->cross_thread_data.target_state == EVENT_THREAD_STATE_RUNNING) {
       impl->cross_thread_data.target_state = EVENT_THREAD_STATE_STOPPING;
       signal_thread = true;
       impl->cross_thread_data.thread_signaled = true;
     }
     aws_mutex_unlock(&impl->cross_thread_data.mutex);
   }
 
   if (signal_thread) {
     signal_cross_thread_data_changed(event_loop);
   }
 
   return AWS_OP_SUCCESS;
 }
 
 static int s_wait_for_stop_completion(struct aws_event_loop* event_loop) {
   struct qnx_poll_loop* impl = event_loop->impl_data;
 
   // TODO: check and complete pending tasks
 
   int err = aws_thread_join(&impl->thread);
   if (err) {
     AWS_LOGF_FATAL(AWS_LS_IO_EVENT_LOOP, "Error when joining event loop thread");
     return AWS_OP_ERR;
   }
   aws_thread_decrement_unjoined_count();
   return AWS_OP_SUCCESS;
 }
 
 static void s_schedule_task_common(struct aws_event_loop* event_loop, struct aws_task* task, uint64_t run_at_nanos) {
   struct qnx_poll_loop* impl = event_loop->impl_data;
 
   if (s_is_event_thread(event_loop)) {
     if (run_at_nanos == 0) {
       aws_task_scheduler_schedule_now(&impl->scheduler, task);
     } else {
       aws_task_scheduler_schedule_future(&impl->scheduler, task, run_at_nanos);
     }
   } else {
     task->timestamp = run_at_nanos;
 
     aws_mutex_lock(&impl->cross_thread_data.mutex);
 
     aws_linked_list_push_back(&impl->cross_thread_data.tasks_to_schedule, &task->node);
 
     bool should_signal_thread = false;
     /* Signal thread that cross_thread_data has changed (unless it's been signaled already) */
     if (!impl->cross_thread_data.thread_signaled) {
       should_signal_thread = true;
       impl->cross_thread_data.thread_signaled = true;
     }
 
     aws_mutex_unlock(&impl->cross_thread_data.mutex);
     /* End critical section */
 
     if (should_signal_thread) {
       signal_cross_thread_data_changed(event_loop);
     }
   }
 }
 
 static void s_schedule_task_now(struct aws_event_loop* event_loop, struct aws_task* task) {
   s_schedule_task_common(event_loop, task, 0);
 }
 
 static void s_schedule_task_future(struct aws_event_loop* event_loop, struct aws_task* task, uint64_t run_at_nanos) {
   s_schedule_task_common(event_loop, task, run_at_nanos);
 }
 
 
 
 
 
 static void s_subscribe_task(struct aws_task* task, void* user_data, enum aws_task_status status) {
   struct handle_data* handle_data = user_data;
   struct aws_event_loop* event_loop = handle_data->event_loop;
   struct qnx_poll_loop* impl = event_loop->impl_data;
 
   aws_array_list_push_back(&impl->handle_datas, &handle_data);
 
   int qnx_notify_cond = 0;
   if (handle_data->events_subscribed & AWS_IO_EVENT_TYPE_READABLE) {
     qnx_notify_cond |= _NOTIFY_COND_INPUT;
     handle_data->aws_io_event_type = AWS_IO_EVENT_TYPE_READABLE;
   }
   if (handle_data->events_subscribed & AWS_IO_EVENT_TYPE_WRITABLE) {
     qnx_notify_cond |= _NOTIFY_COND_OUTPUT;
     handle_data->aws_io_event_type = AWS_IO_EVENT_TYPE_WRITABLE;
   }
 
   
   handle_data->handle_id = impl->next_handle_id++;
   if (impl->next_handle_id == 0) { /* avoid 0 if it wraps */
     impl->next_handle_id = 1;
   }
 
   SIGEV_PULSE_INIT(&handle_data->qnx_event,
                    impl->qnx_connection,
                    SIGEV_PULSE_PRIO_INHERIT,
                    PULSE_FD_EVENT_CODE,
                    (int)handle_data->handle_id);
 
   if (MsgRegisterEvent(&handle_data->qnx_event, handle_data->fd) == -1) {
     AWS_LOGF_ERROR(AWS_LS_IO_EVENT_LOOP, "MsgRegisterEvent failed for FD %d", handle_data->fd);
   }
   handle_data->pre_arm_conditions = 0;
   int ionotify_ret = ionotify(handle_data->fd, _NOTIFY_ACTION_EDGEARM, qnx_notify_cond, &handle_data->qnx_event);
   if (ionotify_ret == 0) {
   } else if (ionotify_ret & _NOTIFY_COND_INPUT) {
     handle_data->pre_arm_conditions = AWS_IO_EVENT_TYPE_READABLE;
   } else if (ionotify_ret & _NOTIFY_COND_OUTPUT) {
     handle_data->pre_arm_conditions = AWS_IO_EVENT_TYPE_WRITABLE;
   } else if (ionotify_ret > 0) {
   } else {
     AWS_LOGF_ERROR(AWS_LS_IO_EVENT_LOOP, "ionotify failed for FD %d - error: %d=%s", handle_data->fd, errno,
                    strerror(errno));
   }
 }
 
 static void s_unsubscribe_task(struct aws_task* task, void* user_data, enum aws_task_status status) {
   struct handle_data* handle_data = user_data;
   struct aws_event_loop* event_loop = handle_data->event_loop;
   struct qnx_poll_loop* impl = event_loop->impl_data;
 
   int nfds = aws_array_list_length(&impl->handle_datas);
   int to_remove = -1;
   for (int i = 0; i < nfds; i++) {
     struct handle_data* this_handle_data = NULL;
     aws_array_list_get_at(&impl->handle_datas, &this_handle_data, i);
     if (this_handle_data == handle_data) {
       to_remove = i;
     }
   }
 
   if (to_remove == -1) {
     AWS_LOGF_FATAL(AWS_LS_IO_EVENT_LOOP, "Cannot find handle_data p=%p", (void*)handle_data);
     aws_raise_error(AWS_ERROR_SYS_CALL_FAILURE);
     return;
   }
 
   if (MsgUnregisterEvent(&handle_data->qnx_event)) {
     AWS_LOGF_ERROR(AWS_LS_IO_EVENT_LOOP, "MsgUnregisterEvent failed for FD %d", handle_data->fd);
   }
 
   aws_array_list_erase(&impl->handle_datas, to_remove);
   aws_mem_release(event_loop->alloc, handle_data);
 }
 
 static int s_subscribe_to_io_events(struct aws_event_loop* event_loop, struct aws_io_handle* handle, int events,
                                     aws_event_loop_on_event_fn* on_event, void* user_data) {
   AWS_ASSERT(event_loop);
   AWS_ASSERT(handle->data.fd != -1);
   AWS_ASSERT(handle->additional_data == NULL);
   AWS_ASSERT(on_event);
   AWS_ASSERT(events & (AWS_IO_EVENT_TYPE_READABLE | AWS_IO_EVENT_TYPE_WRITABLE));
 
   struct handle_data* handle_data = aws_mem_calloc(event_loop->alloc, 1, sizeof(struct handle_data));
 
   handle_data->owner = handle;
   handle_data->event_loop = event_loop;
   handle_data->on_event = on_event;
   handle_data->on_event_user_data = user_data;
   handle_data->events_subscribed = events;
   handle_data->events_this_loop = 0;
   handle_data->state = HANDLE_STATE_SUBSCRIBED;
   handle_data->fd = handle->data.fd;
   handle->additional_data = handle_data;
 
   aws_task_init(&handle_data->subscribe_task, s_subscribe_task, handle_data, "qnx_queue_event_loop_subscribe");
   s_schedule_task_now(event_loop, &handle_data->subscribe_task);
 
   return AWS_OP_SUCCESS;
 }
 
 static int s_unsubscribe_from_io_events(struct aws_event_loop* event_loop, struct aws_io_handle* handle) {
   struct handle_data* handle_data = handle->additional_data;
   AWS_ASSERT(event_loop == handle_data->event_loop);
   AWS_ASSERT(handle->additional_data);
 
   handle_data->state = HANDLE_STATE_UNSUBSCRIBED;
   handle->additional_data = NULL;
 
   aws_task_init(&handle_data->unsubscribe_task, s_unsubscribe_task, handle_data, "qnx_queue_event_loop_unsubscribe");
   s_schedule_task_now(event_loop, &handle_data->unsubscribe_task);
 
   return AWS_OP_SUCCESS;
 }
 
 static bool s_is_event_thread(struct aws_event_loop* event_loop) {
   struct qnx_poll_loop* impl = event_loop->impl_data;
   aws_thread_id_t* thread_id = aws_atomic_load_ptr(&impl->running_thread_id);
   return thread_id && aws_thread_thread_id_equal(*thread_id, aws_thread_current_thread_id());
 }
 
 static void s_process_tasks_to_schedule(struct aws_event_loop* event_loop, struct aws_linked_list* tasks_to_schedule) {
   struct qnx_poll_loop* impl = event_loop->impl_data;
   while (!aws_linked_list_empty(tasks_to_schedule)) {
     struct aws_linked_list_node* node = aws_linked_list_pop_front(tasks_to_schedule);
     struct aws_task* task = AWS_CONTAINER_OF(node, struct aws_task, node);
 
     if (task->timestamp == 0) {
       aws_task_scheduler_schedule_now(&impl->scheduler, task);
     } else {
       uint64_t now_ns = 0;
       event_loop->clock(&now_ns);
       aws_task_scheduler_schedule_future(&impl->scheduler, task, task->timestamp);
     }
   }
 }
 
 static void s_process_cross_thread_data(struct aws_event_loop* event_loop) {
   struct qnx_poll_loop* impl = event_loop->impl_data;
   struct aws_linked_list tasks_to_schedule;
   aws_linked_list_init(&tasks_to_schedule);
 
   {
     aws_mutex_lock(&impl->cross_thread_data.mutex);
     impl->cross_thread_data.thread_signaled = false;
 
     bool initiate_stop = (impl->cross_thread_data.target_state == EVENT_THREAD_STATE_STOPPING) &&
                          (impl->current_state == EVENT_THREAD_STATE_RUNNING);
     if (AWS_UNLIKELY(initiate_stop)) {
       impl->current_state = EVENT_THREAD_STATE_STOPPING;
     }
 
     aws_linked_list_swap_contents(&impl->cross_thread_data.tasks_to_schedule, &tasks_to_schedule);
 
     aws_mutex_unlock(&impl->cross_thread_data.mutex);
   }
 
   s_process_tasks_to_schedule(event_loop, &tasks_to_schedule);
 }
 
 static void aws_event_loop_thread(void* user_data) {
   struct aws_event_loop* event_loop = user_data;
   struct qnx_poll_loop* impl = event_loop->impl_data;
 
   aws_atomic_store_ptr(&impl->running_thread_id, &impl->thread.thread_id);
 
   AWS_ASSERT(impl->current_state == EVENT_THREAD_STATE_READY_TO_RUN);
   AWS_ASSERT(impl->cross_thread_data.target_state == EVENT_THREAD_STATE_RUNNING);
   impl->current_state = EVENT_THREAD_STATE_RUNNING;
 
   struct timespec timeout = {
       .tv_sec = DEFAULT_TIMEOUT_SEC,
       .tv_nsec = 0,
   };
   // List of IO Handle events to be handled in a loop iteration
   struct handle_data* io_handle_events[MAX_EVENTS];
 
   impl->qnx_channel = -1;
   impl->qnx_connection = -1;
   timer_t timer_id = (timer_t)0;
   struct itimerspec timer;
   bool timer_created = false;
 
   impl->qnx_channel = ChannelCreate(0);
   if (impl->qnx_channel == -1) {
     AWS_LOGF_ERROR(AWS_LS_IO_EVENT_LOOP, "ChannelCreate failed (errno=%d: %s)", errno, strerror(errno));
     aws_raise_error(AWS_ERROR_SYS_CALL_FAILURE);
     // No resources to clean up yet
     aws_atomic_store_ptr(&impl->running_thread_id, NULL);
     return;
   }
 
   impl->qnx_connection = ConnectAttach(0, 0, impl->qnx_channel, _NTO_SIDE_CHANNEL, 0);
   if (impl->qnx_connection == -1) {
     AWS_LOGF_ERROR(AWS_LS_IO_EVENT_LOOP, "ConnectAttach failed (errno=%d: %s)", errno, strerror(errno));
     aws_raise_error(AWS_ERROR_SYS_CALL_FAILURE);
     ChannelDestroy(impl->qnx_channel);
     impl->qnx_channel = -1;
     aws_atomic_store_ptr(&impl->running_thread_id, NULL);
     return;
   }
 
   SIGEV_PULSE_INIT(&impl->qnx_timer_event, impl->qnx_connection, SIGEV_PULSE_PRIO_INHERIT, EVENT_LOOP_TIMER_PULSE_CODE,
                    0);
 
   if (timer_create(CLOCK_REALTIME, &impl->qnx_timer_event, &timer_id) == -1) {
     AWS_LOGF_ERROR(AWS_LS_IO_EVENT_LOOP, "timer_create failed (errno=%d: %s)", errno, strerror(errno));
     aws_raise_error(AWS_ERROR_SYS_CALL_FAILURE);
     ConnectDetach(impl->qnx_connection);
     impl->qnx_connection = -1;
     ChannelDestroy(impl->qnx_channel);
     impl->qnx_channel = -1;
     aws_atomic_store_ptr(&impl->running_thread_id, NULL);
     return;
   } else {
     timer_created = true;
   }
 
   int loop_should_exit = 0;
   while (impl->current_state == EVENT_THREAD_STATE_RUNNING) {
     bool should_process_cross_thread_data = impl->cross_thread_data.thread_signaled;
     if (timeout.tv_sec == 0 && timeout.tv_nsec == 0) {
       timeout.tv_nsec = 10;
     }
 
     timer.it_value.tv_sec = timeout.tv_sec;
     timer.it_value.tv_nsec = timeout.tv_nsec;
     timer.it_interval.tv_sec = 0;
     timer.it_interval.tv_nsec = 0;
     if (timer_settime(timer_id, 0, &timer, NULL) != 0) {
       AWS_LOGF_ERROR(AWS_LS_IO_EVENT_LOOP, "timer_settime failed");
     }
 
     int num_io_handle_events = 0;
     // process pre-arm events
     int nfds = aws_array_list_length(&impl->handle_datas);
     for (int fd_index = 0; fd_index < nfds; fd_index++) {
       struct handle_data* this_handle_data = NULL;
       aws_array_list_get_at(&impl->handle_datas, &this_handle_data, fd_index);
       bool pre_triggered = false;
       if (this_handle_data->pre_arm_conditions & AWS_IO_EVENT_TYPE_WRITABLE) {
         pre_triggered = true;
       }
       if (this_handle_data->pre_arm_conditions & AWS_IO_EVENT_TYPE_READABLE) {
         pre_triggered = true;
       }
       if (pre_triggered) {
         // Add a pre-triggered handle to the list of events to be processed
         if (num_io_handle_events >= MAX_EVENTS - 1) {
           AWS_LOGF_FATAL(AWS_LS_IO_EVENT_LOOP, "Reached maximum number of events that can be handled");
           aws_raise_error(AWS_ERROR_SYS_CALL_FAILURE);
           loop_should_exit = 1;
           break;
         }
         io_handle_events[num_io_handle_events++] = this_handle_data;
         // TODO: optimization: differentiate among different events for the same fd
         this_handle_data->events_this_loop |= this_handle_data->pre_arm_conditions;
       }
       this_handle_data->pre_arm_conditions = 0;
     }
     if (loop_should_exit) {
       break;
     }
 
     struct _pulse pulse;
     int msg_receive_pulse_result = 0;
     if (num_io_handle_events > 0) {
       // pretriggered -> go ahead and process the pretriggered events now.
       // MsgReceivePulse will be called in the next iteration of the loop
     } else {
       // not pretriggered, call MsgReceivePulse
       msg_receive_pulse_result = MsgReceivePulse(impl->qnx_channel, &pulse, sizeof(pulse), NULL);
       if (msg_receive_pulse_result < 0) {
         AWS_LOGF_ERROR(AWS_LS_IO_EVENT_LOOP, "MsgReceivePulse failed (errno=%d: %s)", errno, strerror(errno));
         // Only break the loop for critical errors
         if (errno != EINTR && errno != EAGAIN) {
           AWS_LOGF_FATAL(AWS_LS_IO_EVENT_LOOP, "MsgReceivePulse failed - Unrecoverable error");
           impl->current_state = EVENT_THREAD_STATE_STOPPING;
           aws_raise_error(AWS_ERROR_SYS_CALL_FAILURE);
           break;
         } else {
           // For EINTR/EAGAIN, continue to next loop iteration
           continue;
         }
       }
 
       if (pulse.code == PULSE_FD_EVENT_CODE) {
         
         uint32_t id = (uint32_t)pulse.value.sival_int;
         struct handle_data* triggered_handle_data = s_find_handle_by_id(impl, id);
         if (triggered_handle_data) {
           if (triggered_handle_data->aws_io_event_type & AWS_IO_EVENT_TYPE_WRITABLE) {
           }
           if (triggered_handle_data->aws_io_event_type & AWS_IO_EVENT_TYPE_READABLE) {
           }
           if (triggered_handle_data->events_this_loop == 0) {
             if (num_io_handle_events >= MAX_EVENTS - 1) {
               AWS_LOGF_FATAL(AWS_LS_IO_EVENT_LOOP, "Reached maximum number of events that can be handled");
               aws_raise_error(AWS_ERROR_SYS_CALL_FAILURE);
               loop_should_exit = 1;
               break;
             }
             io_handle_events[num_io_handle_events++] = triggered_handle_data;
           }
           triggered_handle_data->events_this_loop |= triggered_handle_data->aws_io_event_type;
         } else {
           /* Stale/unknown id; ignore safely. */
           AWS_LOGF_DEBUG(AWS_LS_IO_EVENT_LOOP, "Received pulse for unknown handle id=%u", (unsigned)id);
         }
       } else if (pulse.code == EVENT_LOOP_TIMER_PULSE_CODE) {
         // timer event, nothing to do here
       } else if (pulse.code == PULSE_FD_CROSS_THREAD_CODE) {
         should_process_cross_thread_data = true;
       } else {
         // no pre-triggered and unknown pulse code
         AWS_LOGF_ERROR(AWS_LS_IO_EVENT_LOOP, "unexpected branch: no event or pulse code in loop (pulse.code=%d)",
                        pulse.code);
         aws_raise_error(AWS_ERROR_INVALID_STATE);
         impl->current_state = EVENT_THREAD_STATE_STOPPING;
         break;
       }
     }
 
     // Now that we have collected all the events to be processed, we can process them.
     // Invoke each handle's event callback (unless the handle has been unsubscribed)
     for (int i = 0; i < num_io_handle_events; ++i) {
       struct handle_data* handle_data = io_handle_events[i];
 
       if (handle_data->state == HANDLE_STATE_SUBSCRIBED) {
         int to_arm_again = 0;
 
         if (handle_data->events_subscribed & AWS_IO_EVENT_TYPE_READABLE) {
           to_arm_again |= _NOTIFY_COND_INPUT;
         }
         if (handle_data->events_subscribed & AWS_IO_EVENT_TYPE_WRITABLE) {
           to_arm_again |= _NOTIFY_COND_OUTPUT;
         }
 
         int ionotify_ret = ionotify(handle_data->fd, _NOTIFY_ACTION_EDGEARM, to_arm_again, &handle_data->qnx_event);
         if (ionotify_ret == 0) {
         } else if (ionotify_ret & _NOTIFY_COND_INPUT) {
           handle_data->pre_arm_conditions = AWS_IO_EVENT_TYPE_READABLE;
         } else if (ionotify_ret & _NOTIFY_COND_OUTPUT) {
           handle_data->pre_arm_conditions = AWS_IO_EVENT_TYPE_WRITABLE;
         } else if (ionotify_ret > 0) {
           AWS_LOGF_ERROR(AWS_LS_IO_EVENT_LOOP, "ionotify failed for FD %d - error: %d\n", handle_data->fd,
                          ionotify_ret);
         } else {
           AWS_LOGF_ERROR(AWS_LS_IO_EVENT_LOOP, "ionotify failed for FD %d - error: %d=%s\n", handle_data->fd, errno,
                          strerror(errno));
         }
 
         handle_data->on_event(event_loop, handle_data->owner, handle_data->events_this_loop,
                               handle_data->on_event_user_data);
       } else {
         // do nothing
       }
 
       handle_data->events_this_loop = 0;
     }
 
     if (should_process_cross_thread_data) {
       s_process_cross_thread_data(event_loop);
     }
 
     uint64_t now_ns = 0;
     event_loop->clock(&now_ns);
     aws_task_scheduler_run_all(&impl->scheduler, now_ns);
 
     bool use_default_timeout = false;
 
     int err = event_loop->clock(&now_ns);
     if (err) {
       use_default_timeout = true;
     }
 
     uint64_t next_run_time_ns;
     if (!aws_task_scheduler_has_tasks(&impl->scheduler, &next_run_time_ns)) {
       use_default_timeout = true;
     }
 
     if (use_default_timeout) {
       timeout.tv_sec = DEFAULT_TIMEOUT_SEC;
       timeout.tv_nsec = 0;
     } else {
       /* Convert from timestamp in nanoseconds, to timeout in seconds with nanosecond remainder */
       uint64_t timeout_ns = next_run_time_ns > now_ns ? next_run_time_ns - now_ns : 0;
 
       uint64_t timeout_remainder_ns = 0;
       uint64_t timeout_sec =
           aws_timestamp_convert(timeout_ns, AWS_TIMESTAMP_NANOS, AWS_TIMESTAMP_SECS, &timeout_remainder_ns);
 
       if (timeout_sec > LONG_MAX) { /* Check for overflow. On Darwin, these values are stored as longs */
         timeout_sec = LONG_MAX;
         timeout_remainder_ns = 0;
       }
 
       int64_t time_delta = (int)next_run_time_ns - (int)now_ns;
       AWS_LOGF_DEBUG(AWS_LS_IO_EVENT_LOOP,
                      "id=%p: detected more scheduled tasks with the next occurring at "
                      "%llu in %ld using timeout of %ds %lluns.\n",
                      (void*)event_loop, (unsigned long long)timeout_ns, time_delta, (int)timeout_sec,
                      (unsigned long long)timeout_remainder_ns);
       timeout.tv_sec = (time_t)(timeout_sec);
       timeout.tv_nsec = (long)(timeout_remainder_ns);
     }
   }
 
   if (timer_created) {
     if (timer_delete(timer_id) != 0) {
       AWS_LOGF_ERROR(AWS_LS_IO_EVENT_LOOP, "timer_delete failed (errno=%d: %s)", errno, strerror(errno));
     }
   }
   if (impl->qnx_connection != -1) {
     ConnectDetach(impl->qnx_connection);
     impl->qnx_connection = -1;
   }
   if (impl->qnx_channel != -1) {
     ChannelDestroy(impl->qnx_channel);
     impl->qnx_channel = -1;
   }
   aws_atomic_store_ptr(&impl->running_thread_id, NULL);
 }
 
 static void s_cancel_task(struct aws_event_loop* event_loop, struct aws_task* task) {
   struct qnx_poll_loop* impl = event_loop->impl_data;
   AWS_LOGF_TRACE(AWS_LS_IO_EVENT_LOOP, "id=%p: cancelling task %p", (void*)event_loop, (void*)task);
   aws_task_scheduler_cancel_task(&impl->scheduler, task);
 }
