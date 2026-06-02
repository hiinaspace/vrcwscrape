import inspect

from toponymy import llm_wrappers
from toponymy.cluster_layer import ClusterLayer, ClusterLayerText

print("OllamaNamer.__init__:", inspect.signature(llm_wrappers.OllamaNamer.__init__))
src = inspect.getsource(llm_wrappers.OllamaNamer)
print("--- OllamaNamer source (first 1500 chars) ---")
print(src[:1500])
print("ClusterLayer attrs:", [x for x in dir(ClusterLayer) if not x.startswith("_")])
print("ClusterLayerText attrs:", [x for x in dir(ClusterLayerText) if not x.startswith("_")])
