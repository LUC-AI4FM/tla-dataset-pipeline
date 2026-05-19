class ChatOllama:
    def __init__(self, model: str, temperature: float = 0):
        self.model = model
        self.temperature = temperature

    def invoke(self, messages):
        class Resp:
            def __init__(self, content: str):
                self.content = content

        return Resp(content='{}')
