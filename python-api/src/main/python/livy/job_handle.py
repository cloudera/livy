#
# Licensed to Cloudera, Inc. under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  Cloudera, Inc. licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
import base64
import cloudpickle
import sys
import threading
import traceback
from concurrent.futures import Future
from threading import Timer, current_thread

# Possible job states.
PENDING = 'PENDING'
RUNNING = 'RUNNING'
CANCELLED = 'CANCELLED'
FINISHED = 'FINISHED'
SENT = 'SENT'
QUEUED = 'QUEUED'


class JobHandle(Future):

    """A child class of concurrent.futures.Future. Allows for monitoring and
        controlling of the running remote job

    """
    # Poll intervals in seconds
    _JOB_INITIAL_POLL_INTERVAL = 0.1
    _JOB_MAX_POLL_INTERVAL = 5

    def __init__(self, conn, session_id, executor):
        Future.__init__(self)
        self._conn = conn
        self._session_id = session_id
        self._executor = executor
        self._cancelled = False
        self._job_id = -1
        self._done = False
        self._job_handle_condition = threading.Condition()
        self._running_callbacks = []
        self._queued_callbacks = []

    def _start(self, command, serialized_job):
        self._executor.submit(self._send_job_task, command, serialized_job)

    def _send_job_task(self, command, job):
        suffix_url = "/" + str(self._session_id) + "/" + command
        job_status = self._conn.send_request('POST', suffix_url,
            headers=self._conn._JSON_HEADERS, data=job)
        self._job_id = job_status.json()['id']
        self._poll_result()

    def queued(self):
        """
        Returns
        -------
        True if the job is currently queued.
        """
        with self._job_handle_condition:
            return self._state == QUEUED

    def _invoke_queued_callbacks(self):
        for callback in self._queued_callbacks:
            try:
                callback(self)
            except Exception:
                traceback.print_exc()
            self._queued_callbacks.remove(callback)

    def _invoke_running_callbacks(self):
        for callback in self._running_callbacks:
            try:
                callback(self)
            except Exception:
                traceback.print_exc()
            self._running_callbacks.remove(callback)

    def add_queued_callback(self, fn):
        """
        Attaches a callable that will be called when the job is queued.

        Parameters
        ----------
        fn : Function
            A callable that will be called with this future as its only
            argument when the job is queued.
        """
        with self._job_handle_condition:
            if self._state == PENDING:
                self._queued_callbacks.append(fn)
            elif self._state == QUEUED:
                fn(self)

    def add_running_callback(self, fn):
        """
        Attaches a callable that will be called when the job is running.

        Parameters
        ----------
        fn : Function
            A callable that will be called with this future as its only
            argument when the job is running.
        """
        with self._job_handle_condition:
            if self._state in [PENDING, QUEUED]:
                self._running_callbacks.append(fn)
            elif self._state == RUNNING:
                fn(self)

    def _update_state(self, state):
        with self._job_handle_condition:
            if state == 'STARTED':
                self._state = RUNNING
                self._invoke_running_callbacks()
            elif state == 'QUEUED':
                self._state = QUEUED
                self._invoke_queued_callbacks()
            elif state == 'CANCELLED':
                self._state = CANCELLED
                self._invoke_callbacks()
            else:
                raise RuntimeError('Future in unexpected state::', self._state)
            self._job_handle_condition.notifyAll()

    def cancel(self):
        """
        Cancel the job if possible.

        Returns
        -------
        True if the job was cancelled, False if the job has already finished.
        """
        with self._job_handle_condition:
            if self._state == FINISHED:
                return False
            if self._state == CANCELLED:
                return True
            if self._job_id > -1:
                self._executor.submit(self._send_cancel_request)
            self._job_handle_condition.notify_all()
        return True

    def _send_cancel_request(self):
        try:
            end_point = "/" + str(self._session_id) + "/jobs/" + \
                str(self._job_id) + "/cancel"
            self._conn.send_json(None, end_point)
        except Exception as err:
            self.set_job_exception(err, traceback.format_exc())
            traceback.print_exc()

    def _poll_result(self):
        def do_poll_result():
            try:
                suffix_url = "/" + str(self._session_id) + "/jobs/" + \
                    str(self._job_id)
                job_status = self._conn.send_request('GET', suffix_url,
                    headers={'Accept': 'application/json'}).json()
                job_state = job_status['state']
                job_result = None
                has_finished = False
                job_error = None
                if job_state == 'SUCCEEDED':
                    job_result = job_status['result']
                    has_finished = True
                elif job_state == 'FAILED':
                    job_error = job_status['error']
                    has_finished = True
                elif job_state == 'CANCELLED':
                    repeated_timer.stop()
                else:
                    pass
                if has_finished:
                    if job_result is not None:
                        b64_decoded = base64.b64decode(job_result)
                        b64_decoded_decoded = base64.b64decode(b64_decoded)
                        deserialized_object = cloudpickle.loads(
                            b64_decoded_decoded)
                        super(JobHandle, self).set_result(deserialized_object)
                    if job_error is not None:
                        self.set_job_exception(Exception(job_error))
                    repeated_timer.stop()
                else:
                    self._update_state(job_state)
            except Exception as err:
                repeated_timer.stop()
                traceback.print_exc()
                self.set_job_exception(err, traceback.format_exc())

        repeated_timer = self._RepeatedTimer(self._JOB_INITIAL_POLL_INTERVAL,
            do_poll_result, self._executor)
        repeated_timer.start()

    def set_running_or_notify_cancel(self):
        raise NotImplementedError("This operation is not supported.")

    def set_result(self, result):
        raise NotImplementedError("This operation is not supported.")

    def set_exception_info(self, exception, traceback):
        raise NotImplementedError("This operation is not supported.")

    def set_exception(self, exception):
        raise NotImplementedError("This operation is not supported.")

    def set_job_exception(self, exception, error_msg=None):
        if sys.version >= '3':
            super(JobHandle, self).set_exception(exception)
        else:
            super(JobHandle, self).set_exception_info(exception, error_msg)

    class _RepeatedTimer(object):
        def __init__(self, interval, polling_job, executor):
            self._timer = None
            self.polling_job = polling_job
            self.interval = interval
            self.is_running = False
            self.stop_called = False
            self.executor = executor

        def _run(self):
            self.is_running = False
            self.executor.submit(self.polling_job)
            self.start()

        def start(self):
            if not self.is_running and not self.stop_called:
                self._timer = Timer(self.interval, self._run)
                self._timer.start()
                self.interval = min(self.interval * 2,
                    JobHandle._JOB_MAX_POLL_INTERVAL)
                self.is_running = True

        def stop(self):
            self._timer.cancel()
            self.stop_called = True
            self.is_running = False
