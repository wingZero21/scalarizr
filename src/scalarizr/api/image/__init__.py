

class ImageAPIDelegate(object):

    def prepare(self, operation, role_name):
        raise NotImplementedError()

    def snapshot(self, operation, role_name):
        raise NotImplementedError()

    def finalize(self, operation, role_name):
        raise NotImplementedError()


class ImageAPIError(BaseException):
    pass
