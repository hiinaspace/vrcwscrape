import inspect

from toponymy import llm_wrappers

print("=== OllamaNamer source ===")
print(inspect.getsource(llm_wrappers.OllamaNamer))

print("\n=== live call ===")
namer = llm_wrappers.OllamaNamer(model="gemma4:e4b", host="http://localhost:11434")
sys_p = (
    "You are an expert at naming topics. You will be given information about a "
    "cluster of VRChat worlds and must produce a concise topic name."
)
usr_p = (
    'Worlds in this cluster: "Horror Asylum", "Haunted House Escape", '
    '"Spooky Mansion", "Backrooms Nightmare". '
    'Respond ONLY with a JSON object of the form '
    '{"topic_name": "<name>", "topic_specificity": <0..1>}'
)
resp = namer._call_llm_with_system_prompt(sys_p, usr_p, 0.5, 256)
print("RAW RESPONSE:", repr(resp))
