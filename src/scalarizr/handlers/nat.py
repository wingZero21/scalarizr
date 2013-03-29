
import logging

from scalarizr import handlers


LOG = logging.getLogger(__name__)


def get_handlers():
	return [NatHandler()]


class NatHandler(handlers.Handler):
	pass