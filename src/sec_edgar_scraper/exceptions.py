class TooManyRequestException(Exception):

    def __init__(self, message):
        super().__init__(message)


class GetRequestException(Exception):
    def __int__(self, message):
        super.__init__(message)


class InvalidStatementLinkException(Exception):
    def __init__(self, message):
        super().__init__(message)
