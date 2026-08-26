import { defineConfig } from "astro/config";
import starlight from "@astrojs/starlight";

export default defineConfig({
  integrations: [
    starlight({
      title: "bun-kafka",
      description: "Zero-dependency Kafka wire protocol client for Bun",
      social: [
        { icon: "github", label: "GitHub", href: "https://github.com/lukasrakauskas/bun-kafka" },
      ],
      sidebar: [
        {
          label: "Guide",
          items: [
            "guide/getting-started",
            "guide/producing",
            "guide/consuming",
            "guide/transactions",
            "guide/admin",
            "guide/security",
            "guide/configuration",
            "guide/observability",
            "guide/kafkajs-migration",
          ],
        },
        {
          label: "Reference",
          items: [
            "feature-completeness",
            "kafka-versions-and-kips",
            "client-gap-audit",
            "benchmarks",
            "performance-validation",
            "chaos-testing",
            "chaos-testing-handoff",
            "compat-test-gap-audit",
          ],
        },
      ],
    }),
  ],
});
