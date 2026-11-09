"use strict";Object.defineProperty(exports, "__esModule", { value: true });exports.CoWSandboxBranchForker = void 0;


var _layerfs_workspace_driver = require("./layerfs_workspace_driver.js"); /**
 * @fileoverview M22 Copy-on-Write Sandbox Branch Forker for Parallel Speculative Execution.
 */class CoWSandboxBranchForker {
  driver;

  constructor(driver = new _layerfs_workspace_driver.LayerFsWorkspaceDriver()) {
    this.driver = driver;
  }

  forkParallelBranches(sourceBranch, branchNames)



  {
    return branchNames.map((name) => this.driver.forkBranch(sourceBranch, name));
  }
}exports.CoWSandboxBranchForker = CoWSandboxBranchForker;