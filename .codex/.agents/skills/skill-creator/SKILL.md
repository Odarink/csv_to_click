---
name: skill-creator
description: Use when creating, migrating, updating, packaging, or validating a Codex Agent Skill with SKILL.md instructions, optional references or scripts, and optional agents/openai.yaml metadata.
---

# Skill Creator

Create focused Codex Agent Skills that load only the instructions needed for a recurring workflow.

This project skill intentionally overlaps the bundled `$skill-creator`. Prefer explicit invocation of this project copy when repo-scoped authoring behavior is desired.

## Required Format

A skill is a directory containing:

```text
skill-name/
  SKILL.md
  agents/openai.yaml       # optional UI metadata and invocation policy
  references/              # optional on-demand documentation
  scripts/                 # optional deterministic tools
  assets/                  # optional output resources
```

In `SKILL.md`, use YAML frontmatter containing only `name` and `description`. Use lowercase letters, digits, and hyphens for the directory and `name`; make them identical. Put trigger conditions in `description`, because it is read before the body.

## Authoring Workflow

1. Establish concrete example requests that should trigger the skill and adjacent requests that should not.
2. Choose destination scope: use `.agents/skills` for a repository skill or `$HOME/.agents/skills` for a user skill unless the user specifies another valid location.
3. Keep the primary `SKILL.md` concise: core workflow, guardrails, output requirements, and direct links to optional resources.
4. Put large reference material in `references/`, deterministic repeatable operations in `scripts/`, and output templates or media in `assets/`.
5. Add `agents/openai.yaml` when UI metadata or an invocation policy is useful. Read [references/openai_yaml.md](references/openai_yaml.md) before generating it.
6. Validate structure, metadata, referenced resources, and representative invocation behavior before delivery.

## Companion Scripts

- Use `scripts/init_skill.py` to initialize a new bundle when a working Python interpreter is available.
- Use `scripts/generate_openai_yaml.py` to regenerate UI metadata after a substantial change.
- Use `scripts/quick_validate.py` to check naming and frontmatter format.

If those scripts cannot run in the current environment, perform equivalent static checks and explicitly report that limitation.

## Quality Rules

- Prefer an instruction-only skill unless deterministic scripting or substantial on-demand reference material adds clear value.
- Do not add README, changelog, installation guide, or duplicated reference content to a skill bundle.
- Remove references to files that do not exist.
- Do not embed provider- or tool-specific behavior unless it is needed for the intended workflow.
- For updated skills, verify that UI metadata and trigger descriptions still match actual behavior.
