import { defineRule } from "@oxlint/plugins";
import type { ESTree } from "@oxlint/plugins";

const VAGUE_CLASS_SUFFIX = /(?:Manager|Service)$/;

/** Disallow generic Manager and Service class suffixes. */
export const noVagueClassNamesRule = defineRule({
  meta: {
    type: "problem",
    docs: {
      description:
        "Disallow vague Manager and Service class suffixes; name classes after their concrete responsibility.",
    },
    messages: {
      vagueClassName:
        'Rename class "{{name}}" after its concrete responsibility; do not use a generic Manager or Service suffix.',
    },
  },
  createOnce(context) {
    return {
      ClassDeclaration(node: ESTree.ClassDeclaration) {
        const name = node.id?.name;
        if (!name || !VAGUE_CLASS_SUFFIX.test(name)) {
          return;
        }
        context.report({
          node: node.id ?? node,
          messageId: "vagueClassName",
          data: { name },
        });
      },
    };
  },
});
