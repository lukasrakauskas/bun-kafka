import { eslintCompatPlugin } from "@oxlint/plugins";

import { noPrivateWireTypesRule } from "./rules/no-private-wire-types.ts";

/** Project rules that keep Kafka wire I/O behind src/protocol. */
const protocolBoundaryPlugin = eslintCompatPlugin({
  meta: { name: "protocol-boundary" },
  rules: {
    "no-private-wire-types": noPrivateWireTypesRule,
  },
});

export default protocolBoundaryPlugin;
