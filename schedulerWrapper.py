import time
import threading
import Queue
import logging

from mesos.interface import Scheduler as _Scheduler
from mesos.native import MesosSchedulerDriver as _MesosSchedulerDriver
from mesos.native import MesosSchedulerDriverImpl as _MesosSchedulerDriverImpl

# messages got from mesos master
from_mesos_queue = Queue.Queue(10000)
# messages will be sent to mesos master
to_mesos_queue = Queue.Queue(10000)


def redirect_to_queue(queue):
    def wrapper(f):
        func_name = f.func_name
        def _inner(self, *args, **kwargs):
            try:
                args = list(args)
                if isinstance(args[0],(_MesosSchedulerDriverImpl,
                                       _MesosSchedulerDriver)):
                    driver = args[0]
                    args[0] = MesosSchedulerDriverCallProxy(driver)
                if isinstance(self, MesosSchedulerDriverCallProxy):
                    args.insert(0, self)
                queue.put_nowait((func_name, args, kwargs))
            except Queue.Full:
                logging.warn("queue is full")
        return _inner
    return wrapper


class MesosSchedulerDriverCallProxy(object):
    def __init__(self, driver):
        self.driver = driver

    @redirect_to_queue(to_mesos_queue)
    def launchTasks(self):
        pass

    @redirect_to_queue(to_mesos_queue)
    def killTask(self):
        pass

    @redirect_to_queue(to_mesos_queue)
    def declineOffer(self):
        pass

    @redirect_to_queue(to_mesos_queue)
    def declineOffer(self):
        pass

    @redirect_to_queue(to_mesos_queue)
    def suppressOffers(self):
        pass

    @redirect_to_queue(to_mesos_queue)
    def sendFrameworkMessage(self):
        pass

    @redirect_to_queue(to_mesos_queue)
    def reconcileTasks(self):
        pass

    @classmethod
    def start_proxy(cls):
        while True:
            func_name, args, kwargs = to_mesos_queue.get()
            self, args = args[0], args[1:]
            getattr(self.driver, func_name)(*args, **kwargs)


class MesosSchedulerProxy(_Scheduler):
    @redirect_to_queue(from_mesos_queue)
    def registered(self, driver, framework_id, master_info):
        pass

    @redirect_to_queue(from_mesos_queue)
    def reregistered(self, driver, master_info):
        pass

    @redirect_to_queue(from_mesos_queue)
    def disconnected(self, driver):
        pass

    @redirect_to_queue(from_mesos_queue)
    def offerRecinded(self, driver, offer_id):
        pass

    @redirect_to_queue(from_mesos_queue)
    def frameworkMessage(self, driver, executor_id, slave_id, msg):
        pass

    @redirect_to_queue(from_mesos_queue)
    def resourceOffers(self, driver, offers):
        pass

    @redirect_to_queue(from_mesos_queue)
    def statusUpdate(self, driver, update):
        pass


class Scheduler(_Scheduler):
    def __init__(self, *args, **kwargs):
        self.running = True

    def start(self):
        while self.running:
            try:
                func_name, args, kwargs = from_mesos_queue.get(timeout=0.02)
            except Queue.Empty:
                continue
            func = getattr(self, func_name)
            func(*args, **kwargs)

    def stop(self):
        self.running = False



class MesosSchedulerDriver(object):
    def __init__(self, scheduler, framework, master,
                 implicitAcknowledgements=False, credential=None):
        self.driver = _MesosSchedulerDriver(
            MesosSchedulerProxy(),
            framework,
            master
        )
        self.scheduler = scheduler
        self.threads = []
        self.running = True

    def start(self):
        self.threads.append(threading.Thread(target=self._start_driver, name='mesos-driver'))
        self.threads.append(threading.Thread(target=self._start_proxy, name='mesos-msg-proxy'))
        for thread in self.threads:
            thread.start()

    def stop(self):
        self.running = False

    def join(self):
        for thread in self.threads:
            thread.join()

    def _start_driver(self):
        self.driver.start()
        self.driver.join()

    def _start_proxy(self):
        while self.running:
            func_name, args, kwargs = to_mesos_queue.get()
            driver, args = args[0].driver, args[1:]
            getattr(driver, func_name)(*args, **kwargs)
