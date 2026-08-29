import { defineConfig } from "astro/config";
import starlight from "@astrojs/starlight";

export default defineConfig({
  integrations: [
    starlight({
      title: "bun-kafka",
      description: "Kafka for Bun, without native dependencies",
      social: [
        { icon: "github", label: "GitHub", href: "https://github.com/lukasrakauskas/bun-kafka" },
      ],
      sidebar: [
        {
          label: "Start here",
          items: ["guide/getting-started", "guide/kafkajs-migration"],
        },
        {
          label: "Use bun-kafka",
          items: [
            "guide/producing",
            "guide/consuming",
            "guide/transactions",
            "guide/admin",
            "guide/security",
            "guide/observability",
          ],
        },
        {
          label: "Reference",
          items: [
            "guide/configuration",
            "reference/supported-features",
            "reference/kafka-compatibility",
            "reference/benchmarks",
          ],
        },
      ],
    }),
  ],
});
