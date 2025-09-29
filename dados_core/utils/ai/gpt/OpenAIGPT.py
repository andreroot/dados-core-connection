import openai
import orjson
import os

openai.api_key = os.getenv("OPENAI_API_KEY")

class OpenAIGPT:

    @staticmethod
    def get_json_answer(prompt, model="text-davinci-002"):
        response = openai.Completion.create(
            engine=model,
            prompt=prompt,
            max_tokens=200,
            n=1,
            stop=None,
            temperature=0,
            presence_penalty=2
        )  

        response = response.choices[0].text.strip().strip("A:").strip()
        return orjson.loads(response)