import ollama

c = ollama.Client(host="http://localhost:11434")
print("MODELS:", [getattr(m, "model", m) for m in c.list().models])
r = c.generate(model="gemma4:e4b", prompt="Say hello in exactly one word.")
print("TYPE:", type(r))
print("FULL RESPONSE:", r)
print("response field:", repr(r["response"]))
print("thinking field:", repr(getattr(r, "thinking", None)))
