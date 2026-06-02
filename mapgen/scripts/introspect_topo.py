import inspect

import toponymy
from toponymy import Toponymy, ToponymyClusterer, llm_wrappers

print("toponymy version:", getattr(toponymy, "__version__", "?"))
print("toponymy dir:", [x for x in dir(toponymy) if not x.startswith("_")])
print()
W = llm_wrappers.LLMWrapper
print("LLMWrapper.__init__:", inspect.signature(W.__init__))
print("LLMWrapper abstractmethods:", getattr(W, "__abstractmethods__", None))
print("LLMWrapper public methods:", [x for x in dir(W) if not x.startswith("__")])
for m in ("_call_llm", "_call_llm_with_system_prompt"):
    if hasattr(W, m):
        print(f"  {m}:", inspect.signature(getattr(W, m)))
print("has OpenAINamer:", hasattr(llm_wrappers, "OpenAINamer"))
print("llm_wrappers classes:", [x for x in dir(llm_wrappers) if x[0].isupper()])
print()
print("ToponymyClusterer.__init__:", inspect.signature(ToponymyClusterer.__init__))
print("ToponymyClusterer.fit:", inspect.signature(ToponymyClusterer.fit))
print("Toponymy.__init__:", inspect.signature(Toponymy.__init__))
print("Toponymy.fit:", inspect.signature(Toponymy.fit))
