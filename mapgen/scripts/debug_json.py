import ollama

c = ollama.Client(host="http://localhost:11434")
jsonp = (
    'Worlds: "Horror Asylum", "Haunted House Escape", "Spooky Mansion". '
    'Reply with JSON {"topic_name": "<name>", "topic_specificity": 0.5}'
)
for model in ("gemma4:e4b", "gemma4:26b"):
    r = c.generate(
        model=model,
        prompt=jsonp,
        format="json",
        options={"temperature": 0.3, "num_predict": 300},
    )
    print(f"{model}: {r['response']!r:.200} | done={r.done_reason} eval={r.eval_count}")
