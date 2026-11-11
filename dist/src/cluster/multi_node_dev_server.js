"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.MultiNodeDevServer = void 0;
/**
 * @fileoverview v2.5 Multi-Node Role-Aware Cluster Server (`brain` | `body` | `gateway`) for `deploy/dev-cluster`.
 */
const server_main_js_1 = require("../server_main.js");
class MultiNodeDevServer extends server_main_js_1.AnstsLocalAndComposeServer {
    constructor(options) {
        super(options);
    }
}
exports.MultiNodeDevServer = MultiNodeDevServer;
