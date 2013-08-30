
import sys
import shutil
import logging


from scalarizr import linux, handlers
from scalarizr.util import software
from scalarizr.messaging import Messages

def get_handlers():
    if linux.os.windows_family:
        return [OpenstackRebundleWindowsHandler()]
    else:
        return []


class OpenstackRebundleWindowsHandler(handlers.Handler):
    logger = None

    def __init__(self):
        self._logger = logging.getLogger(__name__)

    def accept(self, message, queue, behaviour=None, platform=None, os=None, dist=None):
        return message.name == Messages.WIN_PREPARE_BUNDLE

    def on_Win_PrepareBundle(self, message):
        try:
            # XXX: server is terminated during sysprep.
            # we should better understand how it works
            #shutil.copy(r'C:\Windows\System32\sysprep\RunSysprep_2.cmd', r'C:\windows\system32\sysprep\RunSysprep.cmd')
            #shutil.copy(r'C:\Windows\System32\sysprep\SetupComplete_2.cmd', r'C:\windows\setup\scripts\SetupComplete.cmd')
            #linux.system((r'C:\windows\system32\sysprep\RunSysprep.cmd', ))

            result = dict(
                status = "ok",
                bundle_task_id = message.bundle_task_id              
            )
            result.update(software.system_info())
            self.send_message(Messages.WIN_PREPARE_BUNDLE_RESULT, result)

        except:
            e = sys.exc_info()[1]
            self._logger.exception(e)
            last_error = hasattr(e, "error_message") and e.error_message or str(e)
            self.send_message(Messages.WIN_PREPARE_BUNDLE_RESULT, dict(
                status = "error",
                last_error = last_error,
                bundle_task_id = message.bundle_task_id
            ))
