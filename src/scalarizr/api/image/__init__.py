

class ImageAPIDelegate(object):

    def prepare(self, role_name):
        raise NotImplementedError()

    def snapshot(self, role_name):
        raise NotImplementedError()

    def finalize(self, role_name):
        raise NotImplementedError()


class ImageAPIError(BaseException):
    pass
