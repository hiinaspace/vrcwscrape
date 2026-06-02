import ollama

c = ollama.Client(host="http://localhost:11434")
jsonp = (
    'Worlds: "Horror Asylum", "Haunted House Escape". '
    'Reply ONLY with JSON {"topic_name": "<name>", "topic_specificity": 0.5}'
)
simple = "Say hello in exactly one word."


def gen(prompt, opts):
    r = (
        c.generate(model="gemma4:e4b", prompt=prompt, options=opts)
        if opts
        else c.generate(model="gemma4:e4b", prompt=prompt)
    )
    return (repr(r["response"])[:80], "eval", r.eval_count, r.done_reason)


print("simple no-opts:   ", gen(simple, None))
print("simple +opts:     ", gen(simple, {"temperature": 0.5, "num_predict": 256}))
print("json   no-opts:   ", gen(jsonp, None))
print("json   +opts:     ", gen(jsonp, {"temperature": 0.5, "num_predict": 256}))
print("json   temp-only: ", gen(jsonp, {"temperature": 0.5}))
print("json   np-only:   ", gen(jsonp, {"num_predict": 256}))
