class HuggingFaceEndpoint:
    def __init__(self, repo_id: str, model: str, huggingfacehub_api_token: str | None = None, temperature: float = 0):
        self.repo_id = repo_id
        self.model = model
        self.huggingfacehub_api_token = huggingfacehub_api_token
        self.temperature = temperature


class ChatHuggingFace:
    def __init__(self, llm):
        self.llm = llm

    def invoke(self, messages):
        class Resp:
            def __init__(self, content: str):
                self.content = content

        return Resp(content='{}')
