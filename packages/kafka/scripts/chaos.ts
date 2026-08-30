const composeFile = `${import.meta.dir}/../test/chaos.compose.yml`;
const project = "bun-kafka-chaos";
const started = new Date();
const run = async (command: string[], env?: Record<string, string>) => {
  const child = Bun.spawn(command, {
    stdin: "inherit",
    stdout: "inherit",
    stderr: "inherit",
    env: { ...process.env, ...env },
  });
  return child.exited;
};
const compose = (...args: string[]) =>
  run(["docker", "compose", "-f", composeFile, "-p", project, ...args]);

let exitCode = 1;
try {
  if (await compose("up", "-d", "--wait", "--build")) {
    throw new Error("Could not start the chaos cluster");
  }
  exitCode = await run(["bun", "test", "test/chaos.test.ts", "test/chaos-cluster.test.ts"], {
    CHAOS_CLUSTER: "1",
    CHAOS_BROKERS: "127.0.0.1:19092,127.0.0.1:29092,127.0.0.1:39092",
    CHAOS_COMPOSE_FILE: composeFile,
    CHAOS_COMPOSE_PROJECT: project,
    CHAOS_FAULT_CYCLES: process.env.CHAOS_FAULT_CYCLES ?? "1000",
    CHAOS_NETEM: process.env.CHAOS_NETEM ?? "1",
  });
} finally {
  if (process.env.CHAOS_KEEP !== "1") {
    await compose("down", "--volumes", "--remove-orphans");
  }
  const ended = new Date();
  const commit = Bun.spawnSync(["git", "rev-parse", "HEAD"], { stdout: "pipe" })
    .stdout.toString()
    .trim();
  const result = {
    commit,
    started: started.toISOString(),
    ended: ended.toISOString(),
    duration_seconds: (ended.getTime() - started.getTime()) / 1000,
    brokers: 3,
    fault_cycles: Number(process.env.CHAOS_FAULT_CYCLES ?? 1000),
    passed: exitCode === 0,
  };
  const directory = `${import.meta.dir}/../out/chaos`;
  Bun.spawnSync(["mkdir", "-p", directory]);
  const stamp = started.toISOString().replaceAll(":", "-");
  await Bun.write(`${directory}/${stamp}.json`, `${JSON.stringify(result, null, 2)}\n`);
  await Bun.write(
    `${directory}/${stamp}.md`,
    `# Chaos test result\n\n- Commit: \`${commit}\`\n- Started: ${result.started}\n- Duration: ${result.duration_seconds} seconds\n- Brokers: 3\n- Fault cycles: ${result.fault_cycles}\n- Result: **${result.passed ? "PASS" : "FAIL"}**\n`,
  );
}

process.exit(exitCode);
