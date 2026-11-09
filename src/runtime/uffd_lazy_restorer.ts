/**
 * @fileoverview Linux `userfaultfd` On-Demand Page-Fault Memory & SQLite Page Restorer.
 */
export class UffdLazyPageFaultRestorer {
  private readonly faultedPages: Set<number> = new Set();

  public handlePageFault(pageIndex: number): { pageIndex: number; fetchedFromRemote: boolean } {
    const alreadyPresent = this.faultedPages.has(pageIndex);
    this.faultedPages.add(pageIndex);
    return { pageIndex, fetchedFromRemote: !alreadyPresent };
  }
}
