# Prompt Patterns And Evaluation

Use this reference when the core workflow requires a pattern choice or an eval plan.

## Pattern Selection

| Situation | Pattern To Consider | Validation Need |
| --- | --- | --- |
| Clear single task and format | Direct prompt with output constraints | Normal and boundary inputs |
| Output format or tone is unstable | Few-shot examples | Examples plus unseen variants |
| Long source material | Structured context with focused question | Relevant and distracting passages |
| Tool-using agent | Goal, tools, side effects, stopping rules | Tool failure and permission cases |
| Machine-consumed response | Structured output/schema when supported | Invalid and missing-field cases |
| Initial output can be judged | Draft then critique/revise | Cases with known defects |

Use reasoning or planning scaffolds only when the task warrants them; extra instructions are not automatically better.

## Failure Diagnosis

| Symptom | Likely Problem | Smallest Useful Revision |
| --- | --- | --- |
| Correct format, wrong content | Missing grounding or criteria | Add authoritative context or acceptance criteria |
| Inconsistent format | Underspecified output | Add schema, tags, or one representative example |
| Ignores constraint in long prompt | Poor placement or excess context | Remove noise and place critical requirement clearly |
| Invents facts | No uncertainty rule or grounding | Require source-based claims and allow unknowns |
| Tool misuse | Tool behavior underspecified | State permitted operations, errors, and stopping condition |
| Overly verbose output | No audience or length bounds | Set audience and concise output rules |

## Evaluation Checklist

Build a small test set before treating a revised prompt as stable:

- Typical successful input.
- Edge input: missing, empty, ambiguous, or conflicting information.
- Failure case that motivated the revision.
- Adversarial or irrelevant context where applicable.
- Machine-format validation when output is consumed programmatically.

Score results on correctness, instruction adherence, output validity, useful uncertainty handling, and unnecessary verbosity. Compare revisions on the same test set and preserve failures as regression cases.
