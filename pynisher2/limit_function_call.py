#! /bin/python
import logging
import multiprocessing
import os
import resource
import signal
import sys
import tempfile
import threading
import time
import traceback
from multiprocessing import Process, Pipe
from typing import Callable, Set

import psutil


class CpuTimeoutException(Exception):
    pass


class TimeoutException(Exception):
    pass


class MemorylimitException(Exception):
    pass


class SubprocessException(Exception):
    pass


class AnythingException(Exception):
    pass


class FailsafeProcess(Process):
    def __init__(self, *args, **kwargs):
        Process.__init__(self, *args, **kwargs)
        self._pconn, self._cconn = Pipe()
        self._exception = None

    def run(self):
        try:
            Process.run(self)
            self._cconn.send(None)
        except Exception as e:
            tb = traceback.format_exc()
            self._cconn.send((e, tb))
            # raise e  # You can still rise this exception if you need to

    def clean_up(self):
        self._pconn.close()
        self._cconn.close()

    @property
    def exception(self):
        if self._pconn.poll():
            self._exception = self._pconn.recv()
        return self._exception


# create the function the subprocess can execute
def subprocess_func(func: Callable,
                    pipe,
                    mem_in_mb: float,
                    cpu_time_limit_in_s: int,
                    wall_time_limit_in_s: int,
                    grace_period_in_s: int,
                    affinity: Set[int],
                    tmp_dir: str,
                    *args, **kwargs):
    # simple signal handler to catch the signals for time limits
    def handler(signum, frame):
        if signum == signal.SIGXCPU:
            # when process reaches soft limit --> a SIGXCPU signal is sent (it normally terminates the process)
            raise CpuTimeoutException
        elif signum == signal.SIGALRM:
            # SIGALRM is sent to process when the specified time limit to an alarm function elapses (real or clock time)
            raise TimeoutException
        raise AnythingException

    # temporary directory to store stdout and stderr
    if tmp_dir is not None:
        stdout = open(os.path.join(tmp_dir, 'std.out'), 'a', buffering=1)
        sys.stdout = stdout

        stderr = open(os.path.join(tmp_dir, 'std.err'), 'a', buffering=1)
        sys.stderr = stderr

    # catching all signals at this point turned out to interfere with the subprocess (e.g. using ROS)
    signal.signal(signal.SIGALRM, handler)
    signal.signal(signal.SIGXCPU, handler)
    signal.signal(signal.SIGQUIT, handler)

    # code to catch EVERY catchable signal (even X11 related ones ... )
    # only use for debugging/testing as this seems to be too intrusive.
    """
    for i in [x for x in dir(signal) if x.startswith("SIG")]:
        try:
            signum = getattr(signal,i)
            print("register {}, {}".format(signum, i))
            signal.signal(signum, handler)
        except:
            print("Skipping %s"%i)
    """

    # set the memory limit
    if mem_in_mb is not None:
        # byte --> megabyte
        mem_in_b = mem_in_mb * 1024 * 1024
        # the maximum area (in bytes) of address space which may be taken by the process.
        resource.setrlimit(resource.RLIMIT_AS, (mem_in_b, mem_in_b))

    # schedule an alarm in specified number of seconds
    if wall_time_limit_in_s is not None:
        signal.alarm(wall_time_limit_in_s)

    if cpu_time_limit_in_s is not None:
        # From the Linux man page:
        # When the process reaches the soft limit, it is sent a SIGXCPU signal.
        # The default action for this signal is to terminate the process.
        # However, the signal can be caught, and the handler can return control
        # to the main program. If the process continues to consume CPU time,
        # it will be sent SIGXCPU once per second until the hard limit is reached,
        # at which time it is sent SIGKILL.
        resource.setrlimit(resource.RLIMIT_CPU, (cpu_time_limit_in_s, cpu_time_limit_in_s + grace_period_in_s))

    if affinity is not None:
        os.sched_setaffinity(0, affinity)

    os.setsid()
    # the actual function call
    try:
        return_value = (func(*args, **kwargs), 0)
    except MemoryError:
        return_value = (None, MemorylimitException)

    except OSError as ex:
        if ex.errno == 11:
            return_value = (None, SubprocessException)
        else:
            return_value = ((ex, traceback.format_exc()), AnythingException)

    except CpuTimeoutException:
        return_value = (None, CpuTimeoutException)

    except TimeoutException:
        return_value = (None, TimeoutException)

    except Exception as ex:
        return_value = ((ex, traceback.format_exc()), AnythingException)

    finally:
        try:
            pipe.send(return_value)
            pipe.close()

        except:
            # this part should only fail if the parent process is already dead, so there is not much to do anymore :)
            pass
        finally:
            # recursively kill all children
            p = psutil.Process()
            for child in p.children(recursive=True):
                child.kill()


class enforce_limits(object):
    def __init__(self,
                 mem_in_mb: float = None,
                 cpu_time_in_s: int = None,
                 wall_time_in_s: int = None,
                 grace_period_in_s: int = None,
                 affinity: Set[int] = None,
                 logger: logging.Logger = None,
                 capture_output: bool = False):
        self.mem_in_mb = mem_in_mb
        self.cpu_time_in_s = cpu_time_in_s
        self.wall_time_in_s = wall_time_in_s
        self.grace_period_in_s = 0 if grace_period_in_s is None else grace_period_in_s
        self.affinity = affinity
        self.logger = logger if logger is not None else multiprocessing.get_logger()
        self.capture_output = capture_output

    def __call__(self, func):

        class function_wrapper(object):
            def __init__(self2, func):
                self2.func = func
                self2._reset_attributes()

            def _reset_attributes(self2):
                self2.result = None
                self2.exit_status = None
                self2.resources_function = None
                self2.resources_pynisher = None
                self2.wall_clock_time = None
                self2.stdout = None
                self2.stderr = None

                self2.default_handlers = {
                    signal.SIGINT: signal.getsignal(signal.SIGINT),
                    signal.SIGTERM: signal.getsignal(signal.SIGTERM)
                }

            def __call__(self2, *args, **kwargs):

                self2._reset_attributes()

                # create a pipe to retrieve the return value
                parent_conn, child_conn = multiprocessing.Pipe(False)
                # import pdb; pdb.set_trace()

                if self.capture_output:
                    tmp_dir = tempfile.TemporaryDirectory()
                    tmp_dir_name = tmp_dir.name
                else:
                    tmp_dir_name = None

                # create and start the process
                subproc = FailsafeProcess(target=subprocess_func, name="pynisher function call",
                                          args=(self2.func, child_conn, self.mem_in_mb, self.cpu_time_in_s,
                                                self.wall_time_in_s, self.grace_period_in_s, self.affinity,
                                                tmp_dir_name) + args,
                                          kwargs=kwargs)

                # start the process
                start = time.time()
                subproc.start()
                child_conn.close()

                # The subprocess runs in a dedicated GID and is therefore not terminated if the parent terminates.
                # We tap into SIGINT and SIGTERM to terminate the child process and re-raise the original signal
                def handler(signum, frame):
                    if subproc is not None and subproc.is_alive():
                        os.killpg(os.getpgid(subproc.pid), signal.SIGTERM)
                        signal.signal(signum, self2.default_handlers[signum])
                        os.kill(os.getpid(), signum)

                if threading.current_thread() is threading.main_thread():
                    signal.signal(signal.SIGTERM, handler)
                    signal.signal(signal.SIGINT, handler)

                try:
                    # read the return value
                    if self.wall_time_in_s is not None:
                        if parent_conn.poll(self.wall_time_in_s):
                            self2.result, self2.exit_status = parent_conn.recv()
                        else:
                            self.logger.debug('Timeout reached. Stopping process with SIGTERM')
                            os.killpg(os.getpgid(subproc.pid), signal.SIGTERM)
                            subproc.terminate()
                            subproc.join(self.grace_period_in_s)
                            if subproc.is_alive():
                                self.logger.debug('Grace period exceeded. Stopping process with SIGKILL')
                                os.killpg(os.getpgid(subproc.pid), signal.SIGKILL)
                                subproc.kill()
                            self2.exit_status = TimeoutException

                    else:
                        self2.result, self2.exit_status = parent_conn.recv()

                except EOFError:
                    self2.result, self2.exit_status = subproc.exception, AnythingException
                except Exception as ex:
                    self.logger.exception('Unhandled exception')
                    self2.result = (ex, traceback.format_exc())
                    self2.exit_status = AnythingException
                finally:
                    self2.resources_function = resource.getrusage(resource.RUSAGE_CHILDREN)
                    self2.resources_pynisher = resource.getrusage(resource.RUSAGE_SELF)
                    self2.wall_clock_time = time.time() - start
                    self2.exit_status = 5 if self2.exit_status is None else self2.exit_status

                    # recover stdout and stderr if requested
                    if self.capture_output:
                        with open(os.path.join(tmp_dir.name, 'std.out'), 'r') as fh:
                            self2.stdout = fh.read()
                        with open(os.path.join(tmp_dir.name, 'std.err'), 'r') as fh:
                            self2.stderr = fh.read()

                        tmp_dir.cleanup()

                    # Restore original signal handlers again
                    if threading.current_thread() is threading.main_thread():
                        signal.signal(signal.SIGTERM, self2.default_handlers[signal.SIGTERM])
                        signal.signal(signal.SIGINT, self2.default_handlers[signal.SIGINT])

                    # don't leave zombies behind
                    parent_conn.close()
                    subproc.clean_up()
                    subproc.join()
                return self2.result

        return function_wrapper(func)
