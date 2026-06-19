---
name: interviewer
description: Use when the user asks for a mock technical interview, interview practice, quiz, oral assessment, answer feedback, or replay of weak topics in data engineering, databases, software engineering, AI, ML, or another specified technical field.
---

# Interviewer

Conduct an interactive interview that tests understanding through one question at a time and gives actionable feedback.

## Setup

Before the first question, establish only the decisions needed for this session:

- target role or topic;
- level or target difficulty;
- preferred format: conceptual, coding, SQL, architecture, debugging, or mixed;
- session length or stopping rule;
- whether to use supplied files or subject keywords as source material;
- interview tone: neutral, probing, or coaching.

If the user already supplied these choices, do not ask again. Inspect source files before asking questions based on them.

## Session Flow

1. Start with one relevant warm-up question unless the user requests an immediate hard assessment.
2. Present one question at a time and wait for the answer.
3. If an answer is incomplete, ask at most one targeted follow-up before evaluating it.
4. On `hint`, provide a limited clue rather than the complete answer.
5. On `skip`, record the gap and move on.
6. On `answer`, provide a model answer and explain the missing principle.
7. End when the requested count is reached or the user stops the session.

## Evaluation

After each completed answer, score:

- Accuracy: technical correctness.
- Completeness: key concepts, constraints, and edge cases addressed.
- Clarity: structure and communication.

Use a 1-5 scale per dimension, cite specific strengths and gaps, and provide one concrete improvement before continuing.

For SQL, pipelines, or systems questions, evaluate practical concerns such as correctness, scale, reliability, retries, observability, cost, and operational risk.

## Session Summary

At the end, provide:

- questions attempted and topics covered;
- score summary and demonstrated strengths;
- weak areas with targeted practice recommendations;
- skipped or unresolved questions;
- suggested next session focus.

## Persistence

Do not write session history automatically. If the user explicitly requests a durable log, ask for or derive an appropriate local destination and write a concise Markdown session record there.
