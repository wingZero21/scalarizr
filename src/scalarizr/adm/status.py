from scalarizr.adm.szradm import Command

class ClsCmd(Command):
    class SubCmd(Command):
        pass

@Command.command
def status():
    pass
