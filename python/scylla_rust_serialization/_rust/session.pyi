class Session:
    async def execute(self, request: str) -> RequestResult: ...
    async def execute_with_values(self, request: str, values: Any) -> RequestResult: ...
    async def prepare(self, request: str) -> PyPreparedStatement: ...

class RequestResult:
    def __str__(self) -> str: ...

class PyPreparedStatement:
    _inner: Any

def serialize_rust_and_return(
        values: Any,
        prepared: PyPreparedStatement,
) -> tuple[int, bytes]: ...

def serialize_rust(
        values: Any,
        prepared: PyPreparedStatement,
) -> None: ...
