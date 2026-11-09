"use strict";Object.defineProperty(exports, "__esModule", { value: true });exports.MultiNodeDevServer = void 0;


var _server_main = require("../server_main.js"); /**
 * @fileoverview v2.5 Multi-Node Role-Aware Cluster Server (`brain` | `body` | `gateway`) for `deploy/dev-cluster`.
 */class MultiNodeDevServer extends _server_main.AnstsLocalAndComposeServer {
  constructor(options) {
    super(options);
  }
}exports.MultiNodeDevServer = MultiNodeDevServer;