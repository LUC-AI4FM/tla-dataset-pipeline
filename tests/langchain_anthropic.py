from pydantic import SecretStr


class ChatAnthropic:
    def __init__(
        self,
        model_name: str,
        api_key: SecretStr,
        temperature: float = 0,
        timeout: int = 30,
        stop: list[str] | None = None,
    ):
        self.model_name = model_name
        self.api_key = api_key
        self.temperature = temperature
        self.timeout = timeout
        self.stop = stop or []

    def invoke(self, messages):
        class Resp:
            def __init__(self, content: str):
                self.content = content

        return Resp(content='{}')
