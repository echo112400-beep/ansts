/** Encode text cells for spreadsheet consumers without evaluating formulas. */
export function safeCostCsvCell(value: string): string {
  // Excel and similar tools may ignore leading whitespace and control characters.
  const dangerous = /^[\s\u0000-\u001f\u007f\ufeff]*[=+\-@]/u.test(value) ||
    /^[\t\r\n]/u.test(value);
  return '"' + (dangerous ? "'" + value : value).replace(/"/g,'""') + '"';
}

export function encodeCostCsv(rows: Iterable<readonly string[]>): string {
  return Array.from(rows,row=>row.map(safeCostCsvCell).join(',')+'\r\n').join('');
}
