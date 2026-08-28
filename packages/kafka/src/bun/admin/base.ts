import type { ClusterMetadata } from "../../types.ts";
import { Cluster } from "../cluster.ts";
import type { KafkaOptions } from "../shared.ts";

export class AdminBase {
  protected cluster: Cluster;
  protected closed = false;
  protected ownsCluster: boolean;
  protected onClose: () => void;

  constructor(options: KafkaOptions | Cluster, onClose = () => {}) {
    this.ownsCluster = !(options instanceof Cluster);
    this.cluster = options instanceof Cluster ? options : new Cluster(options);
    this.onClose = onClose;
  }

  metadata(topics: string[] | null = null): Promise<ClusterMetadata> {
    if (this.closed) {
      throw new Error("Admin is closed");
    }
    return this.cluster.metadata(topics);
  }

  async close(): Promise<void> {
    if (this.closed) {
      return;
    }
    this.closed = true;
    if (this.ownsCluster) {
      this.cluster.close();
    }
    this.onClose();
  }

  disconnect(): Promise<void> {
    return this.close();
  }
  protected open(): void {
    if (this.closed) {
      throw new Error("Admin is closed");
    }
  }
}
