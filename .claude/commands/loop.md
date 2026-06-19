Run a prompt or slash command on a recurring interval using the loop skill.

Usage: /loop [interval] <prompt>

Intervals: Ns, Nm, Nh, Nd (minimum 1 minute). Defaults to 10m if no interval given.

Examples:
  /loop 5m check if the ECS service is healthy
  /loop 30m /deploy
  /loop 1h check if the backfill job has finished
  /loop check the LingFeng pipeline status

Use the `loop` skill to schedule the provided prompt at the given interval.
