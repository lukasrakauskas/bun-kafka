import assert from "node:assert/strict";

// This snapshot check needs no broker. It also verifies every reported percentile.
for (const [name, count, expectedFailures] of [
  ["samples", 36, 0],
  ["pause", 12, 0],
  ["restart", 12, 9],
] as const) {
  const { samples } = await Bun.file(new URL(`./${name}.json`, import.meta.url)).json();
  assert.equal(samples.length, count);
  const seen = new Set<string>();
  let failures = 0;
  for (const sample of samples) {
    const id = `${sample.profile.name}/${sample.lib}/${sample.mode}/${sample.repeat}`;
    assert(!seen.has(id), `duplicate sample: ${id}`);
    seen.add(id);
    assert(sample.sampled_peak_rss_kib > 0);
    if (sample.exit_code) {
      assert.equal(name, "restart");
      assert(sample.stderr.length > 0);
      failures++;
      continue;
    }
    assert.equal(sample.count, sample.profile.batch * sample.profile.windows);
    assert.equal(sample.consume_count, sample.profile.batch * (sample.profile.windows - 1));
    assert.equal(sample.validated, sample.profile.batch * (sample.profile.windows + 100));
    assert.equal(sample.window_ack_ms.length, sample.profile.windows);
    const sorted: number[] = sample.window_ack_ms.toSorted((a: number, b: number) => a - b);
    assert(sorted.every((n) => Number.isFinite(n) && n >= 0));
    for (const percentile of [50, 95, 99]) {
      assert.equal(
        sample[`p${percentile}_ms`],
        sorted[Math.ceil((sorted.length * percentile) / 100) - 1],
      );
    }
    for (const key of ["produce_ms", "consume_ms", "resume_ms"]) {
      assert(Number.isFinite(sample[key]) && sample[key] > 0);
    }
    if (name === "pause") assert(Math.max(...sorted) >= 500);
  }
  assert.equal(failures, expectedFailures);
  console.log(`${name}: ${count - failures} successes, ${failures} recorded failures`);
}

const { samples } = await Bun.file(new URL("./retained.json", import.meta.url)).json();
assert.equal(samples.length, 6);
const seen = new Set<string>();
for (const sample of samples) {
  assert.equal(typeof sample.copy, "boolean");
  assert([0, 1, 2].includes(sample.repeat));
  const id = `${sample.copy}/${sample.repeat}`;
  assert(!seen.has(id));
  seen.add(id);
  assert.equal(sample.retained_values, 128);
  assert.equal(sample.retained_value_bytes, 8_388_608);
  assert.equal(sample.retained_backing_bytes, sample.copy ? 8_388_608 : 134_269_952);
}
console.log("retained: 6 verified samples");
