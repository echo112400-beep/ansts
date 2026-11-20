/** Regional placement is simulation-only until a real adapter is accepted.
 * Reject even an empty explicit constraint instead of dropping it on local submission.
 */
export function rejectUnavailableRegionPlacement(input: object): Response | null {
  return Object.hasOwn(input,'region_placement')
    ? Response.json({code:'REGION_PLACEMENT_NOT_ENABLED'},{status:409})
    : null;
}
