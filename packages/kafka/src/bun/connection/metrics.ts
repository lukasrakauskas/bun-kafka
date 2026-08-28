export class ConnectionMetrics {
  #requests = 0;
  #bytesSent = 0;
  #bytesReceived = 0;

  recordRequest(bytes: number): void {
    this.#requests++;
    this.#bytesSent += bytes;
  }

  recordResponse(bytes: number): void {
    this.#bytesReceived += bytes;
  }

  get stats() {
    return {
      requests: this.#requests,
      bytesSent: this.#bytesSent,
      bytesReceived: this.#bytesReceived,
    };
  }
}
