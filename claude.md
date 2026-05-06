## Workflow

1. First, think through the problem. Read the codebase and write a plan in tasks/todo.md.
2. The plan should be a checklist of todo items.
3. Check in with me before starting work—I’ll verify the plan.
4. Then, complete the todos one by one, marking them off as you go.
5. At every step, give me a high-level explanation of what you changed.
6. Keep every change simple and minimal. Avoid big rewrites.
7. At the end, add a review section in todo.md summarizing the changes.

## Rules
1. NEVER move secrets out of .env files or hardcode credentials.
2. Go through the code you just wrote and confirm it follows security best practices. Check that no sensitive data is left in the frontend, and that there are no vulnerabilities an attacker could exploit.