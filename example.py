from gevent import monkey; monkey.patch_all(thread=False)
import gevent

import logging

from schedulerWrapper import MesosSchedulerDriver, Scheduler
from gevent.backdoor import BackdoorServer

from mesos.interface import mesos_pb2

class ExampleScheduler(Scheduler):

    def registered(self, driver, framework_id, master_info):
        logging.info("registered with framework id: %s on: %s",
                     framework_id, master_info.hostname)
        self.framework_id = framework_id

    def reregistered(self, driver, master_info):
        logging.info("reregistered on %s", master_info.hostname)

    def disconnected(self, driver):
        logging.info("disconnected with mesos master")

    def offerRecinded(self, driver, offer_id):
        logging.info("offer %s rescinded", offer_id.value)

    def frameworkMessage(self, driver, executor_id, slave_id, msg):
        logging.info("got msg %s from executor %s on slave %s", msg,
                     executor_id.value, slave_id.value)

    def resourceOffers(self, driver, offers):
        logging.info("Recieved resource offers: %s",
                     [o.id.value for o in offers])
        for offer in offers:
            driver.declineOffer(offer.id)

    def statusUpdate(self, driver, update):
        '''
        when a task is started, over,
        killed or lost (slave crash, ....), this method
        will be triggered with a status message.
        '''
        task_id = update.task_id.value
        state = update.state
        logging.info("Task %s is in state %s" %
                     (task_id,
                      mesos_pb2.TaskState.Name(state)))



if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(asctime)s %(module)s.%(funcName)s %(message)s")
    framework = mesos_pb2.FrameworkInfo()
    framework.user = 'tester'
    framework.name = 'test-scheduler-pywrap'
    master = 'zk://127.0.0.1:2181/mesos'

    scheduler = ExampleScheduler()

    driver = MesosSchedulerDriver(scheduler, framework, master)
    driver.start()

    gevent.spawn(scheduler.start)

    backdoor = BackdoorServer(('127.0.0.1', 5001),
                              banner="hello from gevent backdoor!",
                              locals={'foo': 'from defined scope!'})
    backdoor.serve_forever()
