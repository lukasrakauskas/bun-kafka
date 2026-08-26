import { marked } from "marked";
import { mkdir, cp, rm } from "node:fs/promises";

const ROOT = new URL("..", import.meta.url).pathname;
const DIST = new URL("./dist", import.meta.url).pathname;

type Page = { src: string; out: string; title: string; section: string };

const PAGES: Page[] = [
  {
    src: "docs/guide/getting-started.md",
    out: "guide/getting-started.html",
    title: "Getting started",
    section: "Guide",
  },
  {
    src: "docs/guide/producing.md",
    out: "guide/producing.html",
    title: "Producing",
    section: "Guide",
  },
  {
    src: "docs/guide/consuming.md",
    out: "guide/consuming.html",
    title: "Consuming",
    section: "Guide",
  },
  {
    src: "docs/guide/transactions.md",
    out: "guide/transactions.html",
    title: "Transactions",
    section: "Guide",
  },
  {
    src: "docs/guide/admin.md",
    out: "guide/admin.html",
    title: "Administration",
    section: "Guide",
  },
  {
    src: "docs/guide/security.md",
    out: "guide/security.html",
    title: "Security",
    section: "Guide",
  },
  {
    src: "docs/guide/configuration.md",
    out: "guide/configuration.html",
    title: "Configuration reference",
    section: "Guide",
  },
  {
    src: "docs/guide/observability.md",
    out: "guide/observability.html",
    title: "Observability",
    section: "Guide",
  },
  {
    src: "docs/guide/kafkajs-migration.md",
    out: "guide/kafkajs-migration.html",
    title: "Migrating from kafkajs",
    section: "Guide",
  },
  { src: "README.md", out: "index.html", title: "bun-kafka", section: "Reference" },
  {
    src: "docs/feature-completeness.md",
    out: "feature-completeness.html",
    title: "Feature completeness",
    section: "Reference",
  },
  {
    src: "docs/kafka-versions-and-kips.md",
    out: "kafka-versions-and-kips.html",
    title: "Kafka versions & KIPs",
    section: "Reference",
  },
  {
    src: "docs/client-gap-audit.md",
    out: "client-gap-audit.html",
    title: "Gap audit vs other clients",
    section: "Reference",
  },
  { src: "docs/benchmarks.md", out: "benchmarks.html", title: "Benchmarks", section: "Reference" },
  {
    src: "docs/performance-validation.md",
    out: "performance-validation.html",
    title: "Performance validation",
    section: "Reference",
  },
  {
    src: "docs/chaos-testing.md",
    out: "chaos-testing.html",
    title: "Chaos testing",
    section: "Reference",
  },
];

function rel(from: string): string {
  return from.includes("/") ? "../".repeat(from.split("/").length - 1) : "";
}

const CSS = `
:root{--bg:#0d1117;--panel:#161b22;--text:#e6edf3;--muted:#8b949e;--accent:#f59e0b;--border:#30363d}
*{box-sizing:border-box}html{-webkit-text-size-adjust:100%}
body{margin:0;font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",Roboto,sans-serif;background:var(--bg);color:var(--text);line-height:1.65}
a{color:var(--accent);text-decoration:none}a:hover{text-decoration:underline}
.layout{display:flex;min-height:100vh;max-width:1200px;margin:0 auto}
nav{width:250px;flex-shrink:0;padding:24px 16px;border-right:1px solid var(--border);position:sticky;top:0;height:100vh;overflow-y:auto}
nav h2{font-size:.75rem;text-transform:uppercase;letter-spacing:.08em;color:var(--muted);margin:20px 0 6px}
nav .brand{font-size:1.05rem;font-weight:700;color:var(--text)}
nav a{display:block;padding:4px 8px;border-radius:6px;color:var(--muted);font-size:.9rem}
nav a:hover{background:var(--panel);color:var(--text);text-decoration:none}
main{flex:1;min-width:0;padding:32px 40px}
article{max-width:800px}
h1,h2,h3,h4{line-height:1.25;margin-top:1.5em}
h1:first-child{margin-top:0}
code{font-family:ui-monospace,SFMono-Regular,Menlo,monospace;font-size:.875em;background:var(--panel);padding:.15em .4em;border-radius:4px}
pre{background:var(--panel);border:1px solid var(--border);border-radius:8px;padding:14px 16px;overflow-x:auto}
pre code{background:none;padding:0}
table{border-collapse:collapse;width:100%;margin:1em 0;font-size:.92rem;display:block;overflow-x:auto}
th,td{border:1px solid var(--border);padding:6px 10px;text-align:left;vertical-align:top}
th{background:var(--panel)}
blockquote{border-left:3px solid var(--accent);margin:1em 0;padding:.2em 1em;color:var(--muted)}
@media(max-width:800px){nav{position:static;width:auto;height:auto;border-right:none;border-bottom:1px solid var(--border)}.layout{flex-direction:column}}
`;

function nav(current: string): string {
  let html = `<div class="brand">bun-kafka</div><div style="font-size:.85rem;color:var(--muted)">Zero-dependency Kafka for Bun</div>`;
  let section = "";
  for (const page of PAGES) {
    if (page.section !== section) {
      section = page.section;
      html += `<h2>${section}</h2>`;
    }
    const href = rel(current) + page.out;
    const active = page.out === current ? ` style="color:var(--text)"` : "";
    html += `<a href="${href}"${active}>${page.title}</a>`;
  }
  html += `<h2>Project</h2><a href="https://github.com/lukasrakauskas/bun-kafka">GitHub ↗</a><a href="https://www.npmjs.com/package/bun-kafka">npm ↗</a>`;
  return html;
}

async function build(): Promise<void> {
  await rm(DIST, { recursive: true, force: true });
  await mkdir(DIST, { recursive: true });
  await mkdir(`${DIST}/guide`, { recursive: true });
  await cp(`${ROOT}/LICENSE`, `${DIST}/LICENSE`);

  for (const page of PAGES) {
    const markdown = await Bun.file(`${ROOT}/${page.src}`).text();
    // Rewrite intra-site .md links to their rendered .html pages.
    const linked = markdown.replaceAll(
      /\]\(([^)#]+\.md)(#[^)]*)?\)/g,
      (_all, target: string, hash?: string) => {
        const match = PAGES.find(
          (p) => p.src === `docs/${target}` || p.src === target || p.src.endsWith(`/${target}`),
        );
        return match ? `](${rel(page.out)}${match.out}${hash ?? ""})` : `](${target}${hash ?? ""})`;
      },
    );
    const body = marked.parse(linked, { async: false });
    const html = `<!doctype html>
<html lang="en"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>${page.title} · bun-kafka</title>
<style>${CSS}</style></head>
<body><div class="layout">
<nav>${nav(page.out)}</nav>
<main><article>${body}</article></main>
</div></body></html>`;
    await Bun.write(`${DIST}/${page.out}`, html);
  }
  await Bun.write(`${DIST}/.nojekyll`, "");
  console.log(`built ${PAGES.length} pages into website/dist`);
}

await build();

if (process.argv.includes("--serve")) {
  const port = Number(process.env.PORT || 4173);
  Bun.serve({
    port,
    fetch: (req) => {
      const path = new URL(req.url).pathname;
      return new Response(Bun.file(`${DIST}${path === "/" ? "/index.html" : path}`));
    },
  });
  console.log(`serving website/dist on http://localhost:${port}`);
}
