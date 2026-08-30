import { defineRule } from "@oxlint/plugins";
import type { ESTree } from "@oxlint/plugins";

const FORBIDDEN_IMPORTS = new Set([
  "Reader",
  "Writer",
  "KafkaEncoder",
  "KafkaDecoder",
  "encoder",
  "decoder",
  "encodeRequest",
  "decodeResponse",
  "decodeBytes",
]);

const FORBIDDEN_METHODS = new Set([
  "i8",
  "i16",
  "i32",
  "u32",
  "i64",
  "f64",
  "bool",
  "string",
  "bytes",
  "array",
  "raw",
  "varInt",
  "varLong",
  "uvarint",
  "compactString",
  "compactBytes",
  "compactArray",
  "tags",
  "skipTags",
  "patchI32",
  "patchU32",
]);

function isProtocolInternal(filename: string): boolean {
  const normalized = filename.replaceAll("\\", "/");
  return normalized.includes("/src/protocol/") || normalized.includes("/tools/oxlint/");
}

/** Ban importing or constructing private wire Reader/Writer outside src/protocol. */
export const noPrivateWireTypesRule = defineRule({
  meta: {
    type: "problem",
    docs: {
      description:
        "Kafka wire Reader/Writer stay inside src/protocol. Use a named protocol write*/read* operation.",
    },
    schema: [],
    messages: {
      importForbidden:
        "Do not import '{{name}}' outside src/protocol. Use a named protocol write*/read* operation.",
      constructForbidden:
        "Do not construct '{{name}}' outside src/protocol. Use a named protocol write*/read* operation.",
      methodForbidden:
        "Do not call wire method '{{name}}' outside src/protocol. Use a named protocol operation.",
    },
  },
  create(context) {
    if (isProtocolInternal(context.filename)) {
      return {};
    }
    return {
      ImportDeclaration(node: ESTree.ImportDeclaration) {
        for (const specifier of node.specifiers) {
          if (specifier.type !== "ImportSpecifier") {
            continue;
          }
          const name =
            specifier.imported.type === "Identifier"
              ? specifier.imported.name
              : String(specifier.imported.value);
          if (FORBIDDEN_IMPORTS.has(name)) {
            context.report({
              node: specifier,
              messageId: "importForbidden",
              data: { name },
            });
          }
        }
      },
      NewExpression(node: ESTree.NewExpression) {
        const name =
          node.callee.type === "Identifier"
            ? node.callee.name
            : node.callee.type === "MemberExpression" && node.callee.property.type === "Identifier"
              ? node.callee.property.name
              : undefined;
        if (!name || !FORBIDDEN_IMPORTS.has(name)) {
          return;
        }
        context.report({
          node,
          messageId: "constructForbidden",
          data: { name },
        });
      },
      CallExpression(node: ESTree.CallExpression) {
        if (node.callee.type !== "MemberExpression") {
          return;
        }
        const name =
          node.callee.property.type === "Identifier"
            ? node.callee.property.name
            : node.callee.property.type === "Literal" &&
                typeof node.callee.property.value === "string"
              ? node.callee.property.value
              : undefined;
        if (!name || !FORBIDDEN_METHODS.has(name)) {
          return;
        }
        context.report({
          node: node.callee.property,
          messageId: "methodForbidden",
          data: { name },
        });
      },
    };
  },
});
