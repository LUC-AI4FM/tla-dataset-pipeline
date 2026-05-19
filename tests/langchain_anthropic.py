class ChatAnthropic:
    def __init__(self, model: str, api_key: str | None = None, temperature: float = 0):
        self.model = model
        self.api_key = api_key
        self.temperature = temperature

    def invoke(self, messages):
        class Resp:
            def __init__(self, content: str):
                self.content = content

        return Resp(content='{}')
