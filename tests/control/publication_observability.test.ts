import {test} from 'node:test';
import assert from 'node:assert/strict';
import {assertPublicationObservability} from '../../src/control/publication_observability.js';
test('publication requires every uniquely identified fresh monitoring target',()=>{
 const now=100000,targets=[{id:'gateway',health:'up',scrapedAt:now-1000}];
 assertPublicationObservability({collectedAt:now,targets},['gateway'],now);
 for(const evidence of [{collectedAt:now,targets:[]},{collectedAt:now,targets:[...targets,...targets]},{collectedAt:now,targets:[{...targets[0],health:'down'}]},{collectedAt:now,targets:[{...targets[0],scrapedAt:now-20000}]},{collectedAt:now-20000,targets},{collectedAt:now+1,targets}])assert.throws(()=>assertPublicationObservability(evidence,['gateway'],now),{code:'PUBLICATION_MONITORING_REQUIRED'});
 assert.throws(()=>assertPublicationObservability({collectedAt:now,targets},[],now),{code:'PUBLICATION_MONITORING_REQUIRED'});
});
