class MosaicoError(Exception):
    pass


class ConnectError(MosaicoError, ConnectionError):
    pass


class SequenceNotFoundError(MosaicoError):
    pass


class TopicNotFoundError(MosaicoError):
    pass


class InvalidArgumentError(MosaicoError, ValueError):
    pass


class OntologyMismatchError(MosaicoError):
    pass


class ServerError(MosaicoError):
    pass


class StreamError(MosaicoError):
    pass


class SchemaError(MosaicoError, ValueError):
    pass


class StreamClosedError(MosaicoError, ValueError):
    pass
