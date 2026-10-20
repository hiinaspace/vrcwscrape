# Embedding eval

Index: 234,798 worlds, title+tags-only embeddings. Query sets (n per set): cjk2en=400, jp2en=400, en2en=400.

## Retrieval: cjk2en  (recall@10 % / MRR@100 x100)

| variant | qwen3 | gemma2 | static |
|---|---|---|---|
| full-float | 36.5 / 23.9 | 36.5 / 23.0 | 20.2 / 12.7 |
| full-int8 | 36.2 / 24.9 | 37.0 / 25.3 | 20.2 / 12.4 |
| full-binary | 34.2 / 25.4 | 33.2 / 24.9 | 19.2 / 13.6 |
| full-binary(float q) | 34.5 / 25.0 | 35.0 / 25.2 | 19.2 / 13.7 |
| mrl256-float | 30.5 / 18.7 | 36.2 / 22.9 | 19.0 / 12.1 |
| mrl256-int8 | 30.2 / 19.1 | 36.5 / 22.3 | 18.5 / 11.3 |
| mrl256-binary | 23.8 / 17.2 | 24.5 / 16.5 | 15.0 / 10.0 |
| mrl256-binary(float q) | 24.8 / 15.9 | 27.8 / 17.3 | 16.5 / 9.8 |

## Retrieval: jp2en  (recall@10 % / MRR@100 x100)

| variant | qwen3 | gemma2 | static |
|---|---|---|---|
| full-float | 27.3 / 16.2 | 24.8 / 16.1 | 12.5 / 7.8 |
| full-int8 | 27.3 / 16.6 | 25.2 / 18.2 | 12.5 / 7.7 |
| full-binary | 24.0 / 17.3 | 21.8 / 15.4 | 10.8 / 8.2 |
| full-binary(float q) | 24.8 / 16.2 | 23.2 / 15.8 | 12.2 / 7.6 |
| mrl256-float | 21.2 / 12.5 | 25.0 / 14.3 | 10.8 / 6.2 |
| mrl256-int8 | 21.5 / 11.4 | 25.2 / 14.9 | 10.8 / 6.5 |
| mrl256-binary | 15.0 / 10.1 | 12.8 / 9.3 | 7.8 / 5.3 |
| mrl256-binary(float q) | 16.0 / 9.0 | 18.0 / 9.5 | 9.0 / 4.9 |

## Retrieval: en2en  (recall@10 % / MRR@100 x100)

| variant | qwen3 | gemma2 | static |
|---|---|---|---|
| full-float | 39.8 / 25.8 | 41.5 / 25.0 | 19.2 / 12.3 |
| full-int8 | 40.0 / 27.2 | 42.5 / 28.7 | 19.2 / 11.5 |
| full-binary | 38.8 / 27.8 | 34.8 / 27.7 | 18.0 / 13.8 |
| full-binary(float q) | 39.2 / 27.5 | 40.0 / 28.3 | 17.8 / 12.3 |
| mrl256-float | 34.8 / 20.9 | 36.2 / 22.7 | 18.2 / 9.6 |
| mrl256-int8 | 35.5 / 21.5 | 36.8 / 21.7 | 18.2 / 9.5 |
| mrl256-binary | 24.0 / 16.5 | 25.8 / 18.2 | 14.8 / 9.6 |
| mrl256-binary(float q) | 29.2 / 16.1 | 30.5 / 18.4 | 15.5 / 8.3 |

## Similar-worlds sanity (500 sampled worlds, top-10 neighbours on full-text embeddings)

| model | tag Jaccard (nbr / random) | same author % (nbr / random) | same title script % (nbr / random) |
|---|---|---|---|
| qwen3 | 0.126 / 0.008 | 7.7 / 0.0 | 85.0 / 65.4 |
| gemma2 | 0.126 / 0.008 | 8.4 / 0.0 | 83.5 / 65.4 |
| static | 0.110 / 0.008 | 8.0 / 0.0 | 82.3 / 65.4 |

## Notes and decision
- Hard task by design: index = title+tags only, query = held-out description (<=300 chars). n=400 per set, so +-2.4 pt (1 s.e.)
  at 40% recall; differences under ~4 pt between qwen3 and gemma2 are noise. Binary rows: sign vs sign (Hamming); `(float q)`
  keeps the query in float. int8 = global-scale symmetric, float query vs int8 docs.
- Not run: bge-m3 baseline. The sayu GPU lock was held by other jobs (a 110-minute training run queued ahead) and the
  job was cancelled after waiting 15 min; qwen3 + gemma2 were embedded before that. No hand-written eyeball query list yet.
- static-similarity-mrl-multilingual-v1 (browser-sized): about half the recall of the big models (cjk2en 20 vs 36.5) but still
  finds the source world in the top 10 ~1 time in 5, with the whole 256-d int8 token table at 27.1 MB (4 files of 6.8 MB;
  25.5 MB gzipped, so no gain) + 2.6 MB tokenizer.json (0.8 MB gz). 256-d binary world vectors for it are 7.5 MB (cost: 19 -> 15 recall@10).
- Neighbours ("similar worlds"): all three models are far above random (tag Jaccard 0.11-0.13 vs 0.008; same author 8% vs 0%;
  same title script 82-85% vs 65%), qwen3 ~= gemma2 > static. Same-author includes series/clone worlds, which is what users want.
- Chosen for precomputed neighbours: **google/embeddinggemma-2** (text tower, Apache-2.0), 768-d fp16 full-text vectors.
  Equal to qwen3 on neighbours and on retrieval within noise, half the compute (6 vs 12.5 GPU-minutes), 25% smaller vectors, and far
  more robust to MRL truncation (256-d keeps 36.2 vs qwen3 30.5 on cjk2en), which matters if the browser ships truncated vectors.
  Qwen3-Embedding-0.6B (Apache-2.0) is the runner-up; all models here are permissively licensed (no NC models were run).
- Browser query path: static model, 256-d, both queries and world vectors from the same static model (separate index from the
  gemma neighbours). Caveat: [CLS]/[SEP] are part of the library's mean pooling; replicate in JS.
