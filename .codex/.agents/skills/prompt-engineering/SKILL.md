---
name: prompt-engineering
description: Use when the user explicitly asks to create, rewrite, optimize, debug, evaluate, or design an LLM prompt, system instruction, agent instruction, prompt template, few-shot example set, or structured-output prompting strategy.
---

# Prompt Engineering

Treat a prompt as a testable behavioral specification: define intended behavior, observe failures, apply the smallest useful revision, and evaluate again.

## Workflow

1. Establish the model or harness when known, the task, users of the output, input shape, output contract, constraints, and examples of desired or failed behavior.
2. When revising an existing prompt, preserve requirements that are working and diagnose the actual failure before rewriting.
3. Select only the scaffolding necessary for the task: direct instructions, examples, structured sections, tool rules, planning, critique/revision, or machine-validated output.
4. Draft concise instructions with goal, required context, constraints, output format, stopping conditions for agents, and uncertainty handling where relevant.
5. Test on representative normal, boundary, and failure inputs; report what improved and what remains unverified.

## Principles

- State observable success criteria rather than relying on a persona alone.
- Separate untrusted input from instructions and make output shape explicit.
- Include examples when format, classification boundaries, or tone are difficult to specify compactly.
- Put only task-relevant context in the prompt; large irrelevant context increases cost and ambiguity.
- Define tool permissions, side effects, error behavior, and completion criteria for agent prompts.
- Avoid provider-specific requirements unless the target provider and feature support are established.
- Do not promise quality gains or token savings without an evaluation result or cited evidence.

## Supporting Reference

Read [references/patterns-and-evaluation.md](references/patterns-and-evaluation.md) when selecting a prompting pattern, diagnosing failures, or designing an evaluation set.

## Output

For prompt creation, provide the proposed prompt and a short rationale plus test cases. For prompt debugging, provide diagnosis, minimal revised prompt, and a before/after evaluation plan. State assumptions when the target model, tools, or expected outputs are unknown.
