"use strict";Object.defineProperty(exports, "__esModule", { value: true });exports.UffdLazyPageFaultRestorer = void 0; /**
 * @fileoverview Linux `userfaultfd` On-Demand Page-Fault Memory & SQLite Page Restorer.
 */
class UffdLazyPageFaultRestorer {
  faultedPages = new Set();

  handlePageFault(pageIndex) {
    const alreadyPresent = this.faultedPages.has(pageIndex);
    this.faultedPages.add(pageIndex);
    return { pageIndex, fetchedFromRemote: !alreadyPresent };
  }
}exports.UffdLazyPageFaultRestorer = UffdLazyPageFaultRestorer;