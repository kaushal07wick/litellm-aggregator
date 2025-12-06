from litellm import completion
import os

## set ENV variables
os.environ["OPENAI_API_KEY"] = "your-api-key"
messages = [{ "content": "Hello, how are you?","role": "user"}]

# openai call
response = completion(model="openai/gpt-4o", messages=messages)
print(response)