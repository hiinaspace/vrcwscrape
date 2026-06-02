import ollama
from toponymy import llm_wrappers

namer = llm_wrappers.OllamaNamer(model="gemma4:e4b", host="http://localhost:11434")
print("supports_system_prompts:", getattr(namer, "supports_system_prompts", "MISSING"))

p = (
    'Worlds: "Horror Asylum", "Haunted House Escape", "Spooky Mansion". '
    'Reply ONLY with JSON {"topic_name": "<name>", "topic_specificity": 0.5}'
)

print("\n_call_llm (generate):", repr(namer._call_llm(p, 0.5, 256)))

c = ollama.Client(host="http://localhost:11434")
r = c.chat(
    model="gemma4:e4b",
    messages=[{"role": "user", "content": p}],
    options={"temperature": 0.5, "num_predict": 256},
)
print("\nchat user-only:", repr(r["message"]["content"]))

try:
    r2 = c.chat(
        model="gemma4:e4b",
        messages=[
            {"role": "system", "content": "You name topics concisely."},
            {"role": "user", "content": p},
        ],
        options={"temperature": 0.5, "num_predict": 256},
    )
    print("\nchat with-system:", repr(r2["message"]["content"]))
except Exception as e:
    print("\nchat with-system ERROR:", e)
