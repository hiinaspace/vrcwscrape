"""Are the open-CLIP thumbnail embeddings discriminative at all?

If CLIP maps most VRChat 3D screenshots to a similar region (high mean pairwise
cosine vs text's 0.445), that explains why the image block dilutes rather than
refines: it pulls everything slightly together. Also show a couple of thin-text
worlds' nearest image-neighbours to sanity-check visual coherence.
"""

from __future__ import annotations

import numpy as np
import polars as pl

img = np.load("artifacts/image_embeddings.npy").astype("float32")
names = pl.read_parquet("artifacts/embed_meta.parquet")["name"].to_list()

norms = np.linalg.norm(img, axis=1)
present = norms > 1e-6
s = img[present] / norms[present][:, None]

rng = np.random.default_rng(0)
idx = rng.choice(s.shape[0], 2000, replace=False)
sim = s[idx] @ s[idx].T
off = sim[~np.eye(2000, dtype=bool)]
print(f"image emb mean pairwise cos = {off.mean():.3f} std {off.std():.3f}")
print("  (text emb was 0.445 / 0.073)")

# a few image-NN samples (full-set, on normalized vectors)
sn = img / np.maximum(norms, 1e-9)[:, None]
for probe in (10, 5000, 12000):
    q = sn[probe]
    order = np.argsort(-(sn @ q))[:6]
    print(f"\nNN of #{probe} '{names[probe][:40]}':")
    for j in order[1:]:
        print(f"   - {names[j][:50]}")
