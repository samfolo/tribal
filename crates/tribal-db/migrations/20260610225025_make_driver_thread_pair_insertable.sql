-- Make the driver-task/thread pair insertable. The two rows reference
-- each other (agent_threads.driver_task_id and agent_driver_tasks.thread_id
-- are both NOT NULL for the pair), and the XOR constraint forbids
-- inserting the thread half with a NULL and patching it afterwards — so
-- with both foreign keys immediate, no insert order can ever satisfy
-- them. Deferring the thread side lets one transaction insert the thread
-- (naming the driver task's client-generated id), insert the driver
-- task, and have the deferred check pass at commit.
ALTER TABLE agent_threads
    ALTER CONSTRAINT fk_agent_threads_driver_task
    DEFERRABLE INITIALLY DEFERRED;
