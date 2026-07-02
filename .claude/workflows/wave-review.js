export const meta = {
  name: 'wave-review',
  description: 'Multi-lens adversarial review of a wave diff (args: {base, head, notes})',
  whenToUse:
    'At the end of an implementation wave, or to re-review a PR branch before merge. args = {base, head, notes}; refs default to master...HEAD. notes = extra reviewer context.',
  phases: [
    { title: 'Scope', detail: 'partition the diff into review units' },
    { title: 'Review', detail: 'one reviewer per lens over the whole diff' },
    { title: 'Verify', detail: 'adversarial refutation of each finding' },
    { title: 'Synthesize', detail: 'dedup + rank confirmed findings into a report' },
  ],
}

const base = (args && args.base) || 'master'
const head = (args && args.head) || 'HEAD'
const notes = (args && args.notes) || ''
const range = `${base}...${head}`

const REPO_CONTEXT = `
Repo: vrcwscrape (VRChat worlds map). Review scope is the git diff ${range}.
The working tree may be on any ref — ALWAYS read changed files via
\`git show ${head}:<path>\` and the diff via \`git diff ${range} -- <path>\`;
never assume the working tree matches ${head}.
Project rules live in AGENTS.md. The Chen/Yang layout contract lives in
docs/chen-strict-reimplementation.md: any change under mapgen/src/mapgen/chen_*
must be optional, default-off, and byte-identical on the default path, and the
replication-debt ledger must not gain unowned Blocking items. Python runs with
uv from mapgen/ (ruff / ty / pytest).
${notes ? `Extra context from the invoker: ${notes}` : ''}
`

const SCOPE_SCHEMA = {
  type: 'object',
  properties: {
    summary: { type: 'string', description: '2-4 sentence overview of what the diff does' },
    units: {
      type: 'array',
      maxItems: 10,
      items: {
        type: 'object',
        properties: {
          name: { type: 'string' },
          files: { type: 'array', items: { type: 'string' } },
          description: { type: 'string' },
        },
        required: ['name', 'files', 'description'],
      },
    },
    docs_claims: {
      type: 'array',
      items: { type: 'string' },
      description: 'Verifiable claims the diff makes in docs/ (metrics, invariants, behavior)',
    },
  },
  required: ['summary', 'units'],
}

const FINDINGS_SCHEMA = {
  type: 'object',
  properties: {
    findings: {
      type: 'array',
      maxItems: 10,
      items: {
        type: 'object',
        properties: {
          title: { type: 'string' },
          severity: { enum: ['blocking', 'major', 'minor'] },
          file: { type: 'string' },
          line: { type: 'integer' },
          claim: { type: 'string', description: 'The defect, stated precisely' },
          evidence: { type: 'string', description: 'Code/doc evidence with file:line refs' },
        },
        required: ['title', 'severity', 'file', 'claim', 'evidence'],
      },
    },
  },
  required: ['findings'],
}

const VERDICT_SCHEMA = {
  type: 'object',
  properties: {
    real: { type: 'boolean' },
    reasoning: { type: 'string' },
  },
  required: ['real', 'reasoning'],
}

const LENSES = [
  {
    key: 'contract',
    prompt:
      'Lens: CONTRACT COMPLIANCE. Check the diff against AGENTS.md and ' +
      'docs/chen-strict-reimplementation.md. Specifically: do chen_* changes keep the ' +
      'default path byte-identical and default-off (read the code, do not trust doc ' +
      'claims)? Is the ledger consistent with the changes? Do docs added/edited in the ' +
      'diff make claims the code or committed artifacts do not support?',
  },
  {
    key: 'correctness',
    prompt:
      'Lens: CORRECTNESS. Hunt real bugs in the changed code: geometry/topology errors ' +
      '(shapely validity, polygonization edge cases, coordinate transforms), numerical ' +
      'issues, off-by-one/iteration-order bugs, nondeterminism (set/dict ordering, ' +
      'unseeded randomness, float-order dependence), and silent failure modes (bare ' +
      'except, fallbacks that mask errors). Prefer a few deep, concrete findings with a ' +
      'failure scenario over many shallow ones.',
  },
  {
    key: 'tests',
    prompt:
      'Lens: TEST ADEQUACY. For each behavior the diff introduces, would the added/existing ' +
      'tests catch its regression? Identify load-bearing logic with no test, tests that ' +
      'assert too weakly to fail on a plausible bug, and any test that pins incidental ' +
      'behavior. Also check the verification commands in the docs actually cover the new code.',
  },
  {
    key: 'architecture',
    prompt:
      'Lens: ARCHITECTURE & REPRODUCIBILITY. Is the approach structurally sound and well ' +
      'integrated, or does it fight the existing design (hidden coupling, duplicated logic, ' +
      'wrong module boundaries, extension points that should have been reused)? Are the ' +
      'scripts/pipeline reproducible from a fresh clone + documented commands (inputs they ' +
      'read actually produced by upstream scripts; no out-of-band state)? Flag structural ' +
      'choices that will make the NEXT waves (connectivity, terrain, scale-out) harder, ' +
      'with a concrete better alternative.',
  },
]

phase('Scope')
const scope = await agent(
  `${REPO_CONTEXT}
You are the scoping agent for a code review of ${range}.
Run: \`git log --oneline ${base}..${head}\`, \`git diff --stat ${range}\`, and skim the
largest changed files (via git show/diff as instructed above). Read AGENTS.md and, if any
chen_* file changed, docs/chen-strict-reimplementation.md.
Partition the diff into 3-10 coherent review units (module/feature granularity) and list
verifiable claims made in changed docs (metrics, invariant counts, behavior statements).`,
  { label: 'scope', schema: SCOPE_SCHEMA },
)

const scopeJson = JSON.stringify(scope, null, 1)

const results = await pipeline(
  LENSES,
  (lens) =>
    agent(
      `${REPO_CONTEXT}
You are one reviewer on a panel; your assignment:
${lens.prompt}

Scope summary from a prior agent (verify, don't trust):
${scopeJson}

Review the FULL diff ${range} through your lens (all units; prioritize the riskiest).
Read enough surrounding unchanged code (via git show ${head}:<path>) to judge in context.
Report at most 10 findings, each with severity:
- blocking: wrong enough that merging would bake in a defect or unsound structure
- major: real defect or debt, but containable/fixable after merge
- minor: worth noting, not load-bearing
Only report findings you can support with specific evidence (file:line). No style nits.`,
      { label: `review:${lens.key}`, phase: 'Review', schema: FINDINGS_SCHEMA },
    ),
  (review, lens) => {
    const findings = (review && review.findings) || []
    log(`review:${lens.key} -> ${findings.length} findings`)
    return parallel(
      findings.map((f) => () => {
        const nVoters = f.severity === 'minor' ? 1 : 3
        return parallel(
          Array.from({ length: nVoters }, (_, i) =>
            () =>
              agent(
                `${REPO_CONTEXT}
You are adversarial skeptic #${i + 1}. A reviewer (lens: ${lens.key}) claims this defect
in the diff ${range}:

title: ${f.title}
severity: ${f.severity}
file: ${f.file}${f.line ? ` line ~${f.line}` : ''}
claim: ${f.claim}
evidence: ${f.evidence}

Try hard to REFUTE it by reading the actual code (git show ${head}:<path>, git diff
${range}). It is refuted if the claimed behavior cannot occur, is intentional and
documented, or the evidence misreads the code. If you cannot decisively refute it and
the core claim checks out, real=true. If uncertain after genuine investigation,
real=false.`,
                { label: `verify:${lens.key}:${i}`, phase: 'Verify', schema: VERDICT_SCHEMA },
              ),
          ),
        ).then((verdicts) => {
          const votes = verdicts.filter(Boolean)
          const upheld = votes.filter((v) => v.real).length
          const survives = votes.length > 0 && upheld * 2 > votes.length
          return { ...f, lens: lens.key, upheld, voters: votes.length, survives }
        })
      }),
    )
  },
)

const all = results.filter(Boolean).flat().filter(Boolean)
const confirmed = all.filter((f) => f.survives)
const killed = all.length - confirmed.length
log(`${confirmed.length}/${all.length} findings survived adversarial verification`)

phase('Synthesize')
const report = await agent(
  `${REPO_CONTEXT}
You are the synthesis agent. Below are review findings for ${range} that survived
adversarial verification (${killed} were refuted and dropped). Deduplicate overlapping
findings across lenses (keep the strongest statement, note corroborating lenses), sanity-
check severity, and write the final review report as markdown:

1. One-paragraph overall assessment (is the diff sound to merge? what's the risk shape?)
2. Findings grouped by severity (blocking, major, minor), each with file:line, the claim,
   and a concrete suggested fix
3. A short "not verified here" list: docs claims from the scope that this review could
   not check (e.g. claims needing artifact regeneration or long runs)

Scope: ${scopeJson}

Confirmed findings:
${JSON.stringify(confirmed, null, 1)}

Return ONLY the markdown report.`,
  { label: 'synthesize' },
)

return {
  report,
  confirmed,
  stats: {
    total_findings: all.length,
    confirmed: confirmed.length,
    refuted: killed,
    lenses: LENSES.map((l) => l.key),
    range,
  },
}
