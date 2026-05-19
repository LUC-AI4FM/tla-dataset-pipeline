from pydantic import SecretStr


class ChatAnthropic:
    def __init__(self, model_name: str, api_key: SecretStr | None = None, temperature: float = 0):
        self.model_name = model_name
        self.api_key = api_key
        self.temperature = temperature

    def invoke(self, messages):
        class Resp:
            def __init__(self, content: str):
                self.content = content

        return Resp(content='{}')
